package main

import (
	"errors"
	"io/fs"
	"log/slog"
	"path/filepath"
	"sync"

	"github.com/Backblaze/blazer/b2"
	"github.com/go-co-op/gocron/v2"
)

func scheduleBucketTasks(s gocron.Scheduler, b2bucket *b2.Bucket, bucket Bucket) {
	for _, task := range bucket.Tasks {
		_, err := s.NewJob(
			gocron.CronJob(
				task.Schedule,
				false,
			),
			gocron.NewTask(func() {
				filepath.WalkDir(task.LocalPath, func(path string, d fs.DirEntry, err error) error {
					return nil
				})
			}),
		)

		if err != nil {
			if errors.Is(err, gocron.ErrCronJobInvalid) {
				slog.Warn(
					"invalid cron job definition",
					"bucket", bucket.Name,
					"local", task.LocalPath,
					"remote", task.RemotePath,
				)
			} else {
				slog.Warn(err.Error())
			}
		}

		slog.Info(
			"task scheduled",
			"bucket", bucket.Name,
			"local", task.LocalPath,
			"remote", task.RemotePath,
		)
	}
}

func ScheduleTasks(s gocron.Scheduler, buckets []*b2.Bucket, config *Config) {
	var wg sync.WaitGroup

	for _, bucket := range config.Buckets {
		wg.Go(func() {
			found := false
			for _, b2bucket := range buckets {
				if bucket.Name == b2bucket.Name() {
					found = true
					scheduleBucketTasks(s, b2bucket, bucket)
					break
				}
			}

			if !found {
				slog.Warn("bucket does not exist in remote", "bucket", bucket.Name)
			}
		})
	}

	wg.Wait()
}
