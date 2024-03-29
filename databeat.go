package databeat

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goware/calc"
	"github.com/goware/logger"
	"github.com/horizon-games/go-databeat/proto"
)

type Databeat struct {
	options Options
	log     logger.Logger

	Client  proto.Databeat
	Enabled bool

	authKey string
	authCtx context.Context

	assertTypes map[string]struct{}
	queue       []*proto.Event
	queueRaw    []*proto.RawEvent
	flushSem    chan struct{}

	stats Stats

	ctx     context.Context
	ctxStop context.CancelFunc
	running int32
	mu      sync.Mutex
}

type Options struct {
	Privacy             PrivacyOptions
	AssertEventTypes    []string
	FlushBatchSize      int
	FlushInterval       time.Duration
	FlushTimeout        time.Duration
	FlushConcurrency    int
	MaxQueueSize        int
	SetServerClientProp bool
	HTTPClient          *http.Client
}

var DefaultOptions = Options{
	Privacy:             DefaultPrivacyOptions,
	AssertEventTypes:    []string{},
	FlushBatchSize:      100,
	FlushInterval:       2000 * time.Millisecond,
	FlushTimeout:        30 * time.Second,
	FlushConcurrency:    10,
	MaxQueueSize:        10_000,
	SetServerClientProp: false,
	HTTPClient:          &http.Client{},
}

type Stats struct {
	NumEvents uint64
	NumFails  uint64
}

type (
	Event    = proto.Event
	Device   = proto.Device
	RawEvent = proto.RawEvent
)

func NewDatabeatClient(host, authKey string, logger logger.Logger, opts ...Options) (*Databeat, error) {
	options := DefaultOptions
	if len(opts) > 0 {
		options = opts[0]
	}

	if options.FlushBatchSize < 1 {
		return nil, fmt.Errorf("databeat: invalid FlushBatchSize")
	}
	if options.FlushInterval < 1*time.Second {
		return nil, fmt.Errorf("databeat: invalid FlushInterval")
	}
	if options.FlushTimeout < 2*time.Second {
		return nil, fmt.Errorf("databeat: invalid FlushTimeout")
	}
	if options.MaxQueueSize <= 10 {
		return nil, fmt.Errorf("databeat: invalid MaxQueueSize")
	}
	if options.FlushConcurrency <= 0 {
		options.FlushConcurrency = 1
	}

	assertTypes := map[string]struct{}{}
	for _, et := range options.AssertEventTypes {
		assertTypes[et] = struct{}{}
	}

	client := proto.NewDatabeatClient(host, options.HTTPClient)

	headers := http.Header{}
	headers.Set("Authorization", fmt.Sprintf("BEARER %s", authKey))
	authCtx, err := proto.WithHTTPRequestHeaders(context.Background(), headers)
	if err != nil {
		return nil, err
	}

	// TODO: in future to just stdlib slog

	dbeat := &Databeat{
		options:     options,
		log:         logger.With("ps", "databeat"),
		Client:      client,
		Enabled:     true,
		authKey:     authKey,
		authCtx:     authCtx,
		assertTypes: assertTypes,
		queue:       make([]*proto.Event, 0, options.MaxQueueSize),
		queueRaw:    make([]*proto.RawEvent, 0, options.MaxQueueSize),
		flushSem:    make(chan struct{}, options.FlushConcurrency),
	}

	return dbeat, nil
}

func (t *Databeat) Run(ctx context.Context) error {
	if t.IsRunning() {
		return fmt.Errorf("databeat: already running")
	}

	t.ctx, t.ctxStop = context.WithCancel(ctx)

	atomic.StoreInt32(&t.running, 1)
	defer atomic.StoreInt32(&t.running, 0)

	return t.run()
}

func (t *Databeat) Stop() {
	t.log.Info("databeat: stop")
	if t.ctxStop != nil {
		t.ctxStop()
		t.ctxStop = nil
	}
}

func (t *Databeat) IsRunning() bool {
	return atomic.LoadInt32(&t.running) == 1
}

func (t *Databeat) Stats() Stats {
	return t.stats
}

func (t *Databeat) Options() Options {
	return t.options
}

func (t *Databeat) TrackEvent(from From, trackEvents ...Event) {
	if !t.Enabled {
		return
	}

	// Copy events
	events := make([]*Event, len(trackEvents))
	for i, ev := range trackEvents {
		v := ev // copy
		events[i] = &v
	}

	// Set event based on http request and user info
	var uid string
	var ident Ident
	if from.UserHTTPRequest != nil || from.UserID != "" {
		uid, ident = GenUserIDFromRequest(from.UserHTTPRequest, from.UserID, t.options.Privacy)
	}

	// Set ident to service if no user details are passed, and project id is passed
	if uid == "" && from.ProjectID > 0 {
		ident = IDENT_SERVICE
	}

	for _, ev := range events {
		// User & ident
		if ev.UserID == nil || *ev.UserID == "" {
			ev.UserID = &uid
			ev.Ident = uint8(ident)
		}

		// Decorate event if project id is passed
		if from.ProjectID > 0 {
			ev.ProjectID = from.ProjectID
		}

		// Decorate event if user request is passed
		if from.UserHTTPRequest != nil {
			// Source
			if ev.Source == "" {
				ev.Source = from.UserHTTPRequest.URL.Path
			}

			// Device from User-Agent
			userAgent := from.UserHTTPRequest.Header.Get("User-Agent")
			if userAgent != "" {
				ev.Device = DeviceFromUserAgent(userAgent)
			}

			// Country
			countryCode := CountryCodeFromRequest(from.UserHTTPRequest)
			if countryCode != "" {
				ev.CountryCode = &countryCode
			}
		}
	}

	// Track!
	t.Track(events...)
}

// TrackUserEvent will track the event associated to a particular user. We use the http request
// `r` for User-Agent and IP information. Note that `r` is optional, and you can pass `nil`
// as the argument, but it will be unable to offer device and country information.
func (t *Databeat) TrackUserEvent(r *http.Request, userID string, userEvents ...Event) {
	t.TrackEvent(From{
		UserID:          userID,
		UserHTTPRequest: r,
	}, userEvents...)
}

// Track is a low-level track function where you control the full payload.
// The method TrackUserEvent calls Track as well.
func (t *Databeat) Track(events ...*Event) {
	if !t.Enabled {
		return
	}

	if !t.IsRunning() {
		t.log.Warn("databeat worker is not running, skipping event.")
		return
	}

	// Validate event types at runtime if EventTypes has been provided in options
	if len(t.assertTypes) > 0 {
		var valid bool
		var invalidNames []string
		valid, invalidNames, events = validateEventTypes(t.assertTypes, events)
		if !valid {
			t.log.With("invalidEvents", invalidNames).Warnf("databeat: %d invalid event types", len(invalidNames))
			// TODO: add alerter here
		}
	}

	// Annotate events
	for _, ev := range events {
		if ev.Props == nil {
			ev.Props = map[string]string{}
		}
		ev.Props["_tracker"] = "go-databeat"
	}

	// Add events to the queue
	t.mu.Lock()
	t.queue = append(t.queue, events...)
	n := len(t.queue)
	t.mu.Unlock()

	if n > t.options.FlushBatchSize {
		t.Flush(t.ctx)
	}
}

func (t *Databeat) TrackRaw(events ...*RawEvent) {
	if !t.Enabled {
		return
	}

	if !t.IsRunning() {
		t.log.Warn("databeat worker is not running, skipping event.")
		return
	}

	t.mu.Lock()
	t.queueRaw = append(t.queueRaw, events...)
	n := len(t.queueRaw)
	t.mu.Unlock()

	if n > t.options.FlushBatchSize {
		t.Flush(t.ctx)
	}
}

func (t *Databeat) Flush(ctx context.Context) error {
	if !t.IsRunning() {
		return nil
	}

	// copy queue
	t.mu.Lock()

	var trackBatch []*proto.Event
	var flushedBatch uint32
	if len(t.queue) > 0 {
		trackBatch = make([]*proto.Event, len(t.queue))
		copy(trackBatch, t.queue)
		t.queue = t.queue[:0]
	}

	// copy queueRaw
	var rawBatch []*proto.RawEvent
	var flushedRaw uint32
	if len(t.queueRaw) > 0 {
		rawBatch = make([]*proto.RawEvent, len(t.queueRaw))
		copy(rawBatch, t.queueRaw)
		t.queueRaw = t.queueRaw[:0]
	}

	t.mu.Unlock()

	// short-circuit if no events
	if len(trackBatch) == 0 && len(rawBatch) == 0 {
		return nil
	}

	// call http request to do the flushing for Tick() or RawEvents()
	// with concurrency

	// Send events to databeat server Tick endpoint
	if len(trackBatch) > 0 {
		var wg sync.WaitGroup

		for i := 0; i < len(trackBatch); i += t.options.FlushBatchSize {
			wg.Add(1)

			events := trackBatch[i:calc.Min(i+t.options.FlushBatchSize, len(trackBatch))]
			if t.options.SetServerClientProp {
				updateEventClientProp(events)
			}
			updateEventDeviceType(events, ServerDevice())

			t.flushSem <- struct{}{}
			go func(events []*proto.Event) {
				defer func() { <-t.flushSem }()
				defer wg.Done()

				ctx, clear := context.WithTimeout(t.authCtx, t.options.FlushTimeout)
				defer clear()

				ok, err := t.Client.Tick(ctx, events)
				if err != nil {
					// TODO: add retry logic as right now the events will just get dropped
					t.log.With("err", err).Errorf("databeat failed to flush %d Tick events -- error", len(events))
				}
				if err == nil && !ok {
					t.log.Warnf("databeat failed to flush %d Tick events -- not ok", len(events))
				}
				if ok {
					atomic.AddUint32(&flushedBatch, uint32(len(events)))
				}
			}(events)
		}

		wg.Wait()
	}

	// Send events to databeat server RawEvents endpoint
	if len(rawBatch) > 0 {
		var wg sync.WaitGroup

		for i := 0; i < len(rawBatch); i += t.options.FlushBatchSize {
			wg.Add(1)
			events := rawBatch[i:calc.Min(i+t.options.FlushBatchSize, len(rawBatch))]
			updateRawEventDeviceType(events, ServerDevice())

			t.flushSem <- struct{}{}
			go func(events []*proto.RawEvent) {
				defer func() { <-t.flushSem }()
				defer wg.Done()

				ctx, clear := context.WithTimeout(t.authCtx, t.options.FlushTimeout)
				defer clear()

				ok, err := t.Client.RawEvents(ctx, events)
				if err != nil {
					// TODO: add retry logic as right now the events will just get dropped
					t.log.With("err", err).Errorf("databeat failed to flush %d RawEvents events -- error", len(events))
				}
				if err == nil && !ok {
					t.log.Warnf("databeat failed to flush %d RawEvents events -- not ok", len(events))
				}
				if ok {
					atomic.AddUint32(&flushedRaw, uint32(len(events)))
				}
			}(events)
		}

		wg.Wait()
	}

	t.log.Debugf("databeat flushed %d events", flushedBatch+flushedRaw)

	return nil
}

func (t *Databeat) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.queue = t.queue[:0]
	t.queueRaw = t.queueRaw[:0]
}

func (t *Databeat) run() error {
	for {
		select {
		case <-t.ctx.Done():
			return nil
		case <-time.After(t.options.FlushInterval):
			err := t.Flush(t.ctx)
			if err != nil {
				t.log.With("err", err).Error("databeat: failed to flush")
			}
		}
	}
}
