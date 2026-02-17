package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-co-op/gocron/v2"
)

type Application struct {
	s3Client  *s3.Client
	config    *Config
	scheduler gocron.Scheduler
	manager   *transfermanager.Client
	uploadCh  chan upload
	wg        sync.WaitGroup
}

func Initialize(scheduler gocron.Scheduler) (*Application, error) {
	var err error
	app := &Application{}

	app.scheduler = scheduler

	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "config.yaml"
	}

	app.config, err = ParseConfig(configPath)
	if err != nil {
		return nil, err
	}

	conf := app.config

	if conf.Concurrency <= 0 {
		slog.Warn("invalid concurrency, defaulting to 5")
		conf.Concurrency = 5
	}

	app.uploadCh = make(chan upload)

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(conf.Credentials.ApplicationKeyId,
				conf.Credentials.ApplicationKey,
				"",
			),
		),
	)
	if err != nil {
		return nil, err
	}

	app.s3Client = s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(conf.Remote.Endpoint)
		o.Region = conf.Remote.Region
	})

	app.manager = transfermanager.New(app.s3Client)

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

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan struct{}, 1)
	go func() {
		sig := <-sigc
		slog.Info(fmt.Sprintf("received %s, shutting down", sig))
		done <- struct{}{}
	}()

	<-done

	close(app.uploadCh)
	app.wg.Wait()

	s.Shutdown()
}
