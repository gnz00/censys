package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/censys/scan-takehome/pkg/processing"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	projectId := flag.String("project", "test-project", "GCP Project ID")
	subscriptionId := flag.String("subscription", "scan-sub", "GCP PubSub subscription ID")
	concurrency := flag.Int("concurrency", 10, "Number of workers")

	client, err := pubsub.NewClient(ctx, *projectId)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	sub := client.Subscription(*subscriptionId)

	// this is our message sink for pubsub messages to be consumed by the worker pool
	messages := make(chan *pubsub.Message)
	defer close(messages)

	// create a new duckdb memory database
	store, err := processing.NewDuckDbStore()
	if err != nil {
		panic(err)
	}

	workerPool := processing.NewWorkerPool(ctx, *concurrency, messages, store)
	go workerPool.Start()

	// print the table every minute
	go (func() {
		for range time.Tick(time.Minute) {
			select {
			case <-ctx.Done():
				return
			default:
				store.PrintTable(ctx)
			}
		}
	})()

	shutdown := make(chan os.Signal, 3)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	// pipe the subscription into the messages channel
	go func(ctx context.Context, messages chan *pubsub.Message) {
		err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			if m != nil {
				messages <- m
			}
		})
		if err != nil {
			panic(err)
		}
	}(ctx, messages)

	// wait for a shutdown signal
	<-shutdown
	cancel()
	workerPool.Stop()
}
