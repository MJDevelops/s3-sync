package main

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/go-co-op/gocron/v2"
)

func (app *Application) scheduleBucketTasks(ctx context.Context, bucket Bucket) {
	manager := transfermanager.New(app.s3Client)

	for _, task := range bucket.Tasks {
		_, err := app.scheduler.NewJob(
			gocron.CronJob(
				task.Schedule,
				false,
			),
			gocron.NewTask(func() {
				d := os.DirFS(task.LocalPath)
				fs.WalkDir(d, ".", func(path string, d fs.DirEntry, err error) error {
					if err != nil {
						slog.Error(err.Error())
						return nil
					}

					if !d.IsDir() {
						_, err = app.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
							Bucket: aws.String(bucket.Name),
							Key:    aws.String(path),
						})
						if err != nil {
							if apiError, ok := errors.AsType[smithy.APIError](err); ok {
								switch apiError.(type) {
								case *types.NotFound:
									f, err := os.Open(path)
									if err != nil {
										slog.Warn("error opening file", "file", path, "error", err.Error())
										return nil
									}

									defer f.Close()
									slog.Info("uploading object", "object", path)

									_, err = manager.UploadObject(ctx, &transfermanager.UploadObjectInput{
										Bucket: aws.String(bucket.Name),
										Key:    aws.String(path),
										Body:   f,
									})

									if err != nil {
										slog.Error("error uploading file", "file", path)
									} else {
										slog.Info("uploaded file", "file", path, "bucket", bucket.Name)
									}
								default:
									slog.Warn("error occured with object", "object", path, "error", err.Error())
								}
							}
						}
					}

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
		} else {
			slog.Info(
				"task scheduled",
				"bucket", bucket.Name,
				"local", task.LocalPath,
				"remote", task.RemotePath,
			)
		}
	}
}

func (app *Application) ScheduleTasks(ctx context.Context) {
	for _, bucket := range app.config.Buckets {
		_, err := app.s3Client.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: aws.String(bucket.Name),
		})

		if err != nil {
			if apiError, ok := errors.AsType[smithy.APIError](err); ok {
				switch apiError.(type) {
				case *types.NotFound:
					slog.Warn("bucket doesn't exist in remote", "bucket", bucket.Name)
				default:
					slog.Warn("error occured with bucket", "bucket", bucket.Name)
				}
			}

			continue
		}

		app.scheduleBucketTasks(ctx, bucket)
	}
}
