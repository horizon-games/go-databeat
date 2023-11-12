package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/goware/logadapter-zerolog"
	databeat "github.com/horizon-games/go-databeat"
	"github.com/rs/zerolog"
)

func main() {
	fmt.Println("go-databeat example")
	fmt.Println("===================")

	databeatHost := "http://localhost:9999"
	logger := zerolog.New(os.Stdout)
	wlogger := logadapter.LogAdapter(logger)
	authToken := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhcHAiOiJkZW1vIn0.rkbj-101BpUkQPMtmKdp2uANFBsiPmd8JMV3jPwj7X0"

	dbeat, err := databeat.NewDatabeatClient(databeatHost, authToken, wlogger)

	if err != nil {
		log.Fatal(err)
	}

	go func() {
		err := dbeat.Run(context.Background())
		if err != nil {
			log.Fatal(err)
		}
	}()

	// Wait to start
	time.Sleep(500 * time.Millisecond)

	dbeat.Track(
		&databeat.Event{
			Event:  "example.ping",
			Source: "api-server/some-endpoint",
		},
		&databeat.Event{
			Event:  "example.test",
			Source: "api-server/some-endpoint",
		},
	)

	dbeat.TrackUserEvent(nil, "user1", databeat.Event{
		Event:  "example.test",
		Source: "api-server/some-endpoint",
	})

	// TrackEvent is a helper for tracking events with a project ID, which you
	// can also use to track user events like TrackUserEvent. Just alternative sugar.
	dbeat.TrackEvent(databeat.From{ProjectID: 42}, databeat.Event{
		Event:  "server.sync",
		Source: "api-server/sync",
	})

	dbeat.Track(&databeat.Event{
		Event:  "example.ping",
		Source: "api-server/some-endpoint",
		Props:  map[string]string{"key1": "yes"},
	})

	// can be manually called, or will automatically happen on interval, etc.
	dbeat.Flush(context.Background())

	dbeat.Track(&databeat.Event{
		Event:  "example.ping",
		Source: "api-server/some-endpoint",
		Props:  map[string]string{"key1": "yes"},
	})

	// NOTE: you can also call TrackRaw but you must have a JWT token with authorization
	// dbeat.TrackRaw(&databeat.RawEvent{
	// 	Event:  "example.ping",
	// 	Source: "api-server/some-endpoint",
	// 	Props:  map[string]string{"raw": "test"},
	// 	TS:     databeat.TimeNow(),
	// 	UserID: databeat.String("user1"),
	// })

	// Wait to finish
	time.Sleep(5000 * time.Millisecond)
}
