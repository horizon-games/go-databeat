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
	authToken := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhcHAiOiI0MDE5NjM5NzUifQ.pEOH2aaIv3ZaEv0TAoHGEilMPI3qpPM3j0X4v0mcLWs"

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

	dbeat.Track(&databeat.Event{
		Event:  "example.ping",
		Source: "api-server/some-endpoint",
	},
		&databeat.Event{
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
	// dbeat.TrackRaw(&databeat.Event{
	// 	Event:  "example.ping",
	// 	Source: "api-server/some-endpoint",
	// 	Props:  map[string]string{"raw": "test"},
	// 	TS:     databeat.TimeNow(),
	// 	User:   databeat.String("user1"),
	// })

	// Wait to finish
	time.Sleep(5000 * time.Millisecond)
}
