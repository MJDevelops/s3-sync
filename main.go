package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-co-op/gocron/v2"
)

type Application struct {
	s3Client  *s3.Client
	config    *Config
	scheduler gocron.Scheduler
}

func Initialize(scheduler gocron.Scheduler) (*Application, error) {
	var err error
	app := &Application{}

	app.scheduler = scheduler

	id := os.Getenv("B2_APPLICATION_KEY_ID")
	key := os.Getenv("B2_APPLICATION_KEY")
	s3Endpoint := os.Getenv("S3_ENDPOINT")
	s3Region := os.Getenv("S3_REGION")

	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "config.yaml"
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(id, key, "")),
	)
	if err != nil {
		return nil, err
	}

	app.s3Client = s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(s3Endpoint)
		o.Region = s3Region
	})

	app.config, err = ParseConfig(configPath)
	if err != nil {
		return nil, err
	}

	return app, nil
}

func main() {
	s, err := gocron.NewScheduler()
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	app, err := Initialize(s)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	ctx := context.Background()

	app.ScheduleTasks(ctx)
	s.Start()

	sigc := make(chan os.Signal)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
	sig := <-sigc

	slog.Info(fmt.Sprintf("received %s, shutting down", sig.String()))
	s.Shutdown()
}
