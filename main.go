package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Backblaze/blazer/b2"
	"github.com/go-co-op/gocron/v2"
)

func main() {
	id := os.Getenv("B2_APPLICATION_KEY_ID")
	key := os.Getenv("B2_APPLICATION_KEY")

	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "config.yaml"
	}

	ctx := context.Background()

	b2, err := b2.NewClient(ctx, id, key)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	buckets, err := b2.ListBuckets(ctx)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	config, err := ParseConfig(configPath)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	s, err := gocron.NewScheduler()
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	ScheduleTasks(s, buckets, &config)
	s.Start()

	sigc := make(chan os.Signal)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
	sig := <-sigc

	slog.Info(fmt.Sprintf("received %s, shutting down", sig.String()))
	s.Shutdown()
}
