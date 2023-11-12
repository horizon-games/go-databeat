package databeat

import (
	"net/http"
	"strings"
	"time"

	"github.com/horizon-games/go-databeat/proto"
	"github.com/mileusna/useragent"
)

type From struct {
	UserID          string
	UserHTTPRequest *http.Request
	ProjectID       uint64
}

func TimeNow() *time.Time {
	t := time.Now().UTC()
	return &t
}

func String(s string) *string {
	return &s
}

func DeviceFromUserAgent(userAgent string) *proto.Device {
	if userAgent == "" {
		return nil
	}
	ua := useragent.Parse(userAgent)

	device := &proto.Device{}
	device.OS = strings.ToLower(ua.OS)
	device.OSVersion = strings.ToLower(ua.OSVersion)
	device.Browser = strings.ToLower(ua.Name)
	device.BrowserVersion = strings.ToLower(ua.Version)

	if ua.Desktop {
		device.Type = "desktop"
	} else if ua.Mobile {
		device.Type = "mobile"
	} else if ua.Tablet {
		device.Type = "tablet"
	} else if ua.Bot {
		device.Type = "bot"
	}

	return device
}

func CountryCodeFromRequest(r *http.Request) string {
	h := r.Header.Get("CF-IPCountry")
	if h == "" {
		return ""
	}
	return strings.ToUpper(h)
}

func ServerDevice() *Device {
	return &Device{Type: "server"}
}

func updateEventDeviceType(events []*Event, device *Device) {
	for _, ev := range events {
		if ev.Device == nil {
			ev.Device = device
		}
	}
}

func updateRawEventDeviceType(events []*RawEvent, device *Device) {
	for _, ev := range events {
		if ev.DeviceType == nil {
			ev.DeviceType = &device.Type
		}
	}
}

func updateEventClientProp(events []*Event) {
	for _, ev := range events {
		if ev.Props == nil {
			ev.Props = map[string]string{"_dbeatClient_": "server"}
		} else {
			ev.Props["_dbeatClient_"] = "server"
		}
	}
}

func EventTypes(eventTypesMap ...map[string]uint32) []string {
	var keys []string
	if len(eventTypesMap) == 0 {
		// if none passed, just return empty list which will skip validation
		return keys
	}

	// append our default event types and any custom event types
	eventTypesMap = append(eventTypesMap, proto.EventType_value)
	for _, m := range eventTypesMap {
		for k := range m {
			keys = append(keys, k)
		}
	}
	return keys
}

func validateEventTypes(eventTypes map[string]struct{}, events []*Event) (bool, []string, []*Event) {
	if len(eventTypes) == 0 {
		// skip if event types assert map is empty
		return true, nil, events
	}

	// quick validation check
	var valid bool
	for _, ev := range events {
		if _, ok := eventTypes[ev.Event]; !ok {
			valid = false
			break
		}
	}
	if valid {
		return true, nil, events
	}

	// return list of only valid events and list of invalid event names
	validEvents := []*Event{}
	invalidNames := []string{}
	for _, ev := range events {
		if _, ok := eventTypes[ev.Event]; !ok {
			invalidNames = append(invalidNames, ev.Event)
		} else {
			validEvents = append(validEvents, ev)
		}
	}

	return false, invalidNames, validEvents
}
