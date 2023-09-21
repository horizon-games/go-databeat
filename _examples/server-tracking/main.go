package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	databeat "github.com/horizon-games/go-databeat"
	"github.com/rs/zerolog"
)

func main() {
	fmt.Println("go-databeat server-tracking example")
	fmt.Println("===================================")

	databeatHost := "http://localhost:9999"
	logger := zerolog.New(os.Stdout)
	authToken := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhcHAiOiJkZW1vIn0.rkbj-101BpUkQPMtmKdp2uANFBsiPmd8JMV3jPwj7X0"

	dbeatOptions := databeat.DefaultOptions
	// dbeatOptions.Privacy.UserIDHash = true
	// dbeatOptions.Privacy.UserAgentSalt = false

	dbeat, err := databeat.NewDatabeatClient(databeatHost, authToken, logger, dbeatOptions)

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

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("hello world"))
	})

	r.Get("/login", func(w http.ResponseWriter, r *http.Request) {
		dbeat.TrackUserEvent(r, "user1", &databeat.Event{
			Event:  "LOGIN",
			Source: "examples/server-tracking",
		})

		w.Write([]byte("ok"))
	})

	http.ListenAndServe(":3333", r)
}
