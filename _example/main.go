package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	databeat "github.com/horizon-games/go-databeat"
	"github.com/rs/zerolog"
)

func main() {
	fmt.Println("go-databeat example")
	fmt.Println("===================")

	databeatHost := "http://localhost:9999"
	logger := zerolog.New(os.Stdout)
	authToken := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhcHAiOiJkZW1vIn0.rkbj-101BpUkQPMtmKdp2uANFBsiPmd8JMV3jPwj7X0"

	dbeat, err := databeat.NewDatabeatClient(databeatHost, authToken, logger)

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

	dbeat.TrackUserEvent("user1", &databeat.Event{
		Event:  "example.test",
		Source: "api-server/some-endpoint",
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
