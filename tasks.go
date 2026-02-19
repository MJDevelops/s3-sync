package main

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/charlievieth/fastwalk"
	"github.com/go-co-op/gocron/v2"
)

type upload struct {
	key     string
	absPath string
	bucket  string
}

func (app *Application) uploadFile(ctx context.Context) {
	for {
		upload, ok := <-app.uploadCh
		if !ok {
			break
		}

		f, err := os.Open(upload.absPath)
		if err != nil {
			slog.Error("error opening file", "file", upload.absPath, "error", err.Error())
			continue
		}

		defer f.Close()
		slog.Info("uploading object", "object", upload.absPath)

		_, err = app.manager.UploadObject(ctx, &transfermanager.UploadObjectInput{
			Bucket: aws.String(upload.bucket),
			Key:    aws.String(upload.key),
			Body:   f,
		})

		if err != nil {
			slog.Error("error uploading file", "file", upload.absPath)
		} else {
			slog.Info("uploaded file", "file", upload.absPath, "bucket", upload.bucket)
		}
	}
}

func (app *Application) scheduleBucketTasks(ctx context.Context, bucket Bucket) {
	for _, task := range bucket.Tasks {
		_, err := app.scheduler.NewJob(
			gocron.CronJob(
				task.Schedule,
				false,
			),
			gocron.NewTask(func() {
				fastwalk.Walk(nil, task.LocalPath, func(path string, d fs.DirEntry, err error) error {
					if err != nil {
						slog.Error(err.Error())
						return nil
					}

					if !d.IsDir() {
						objectKey := filepath.ToSlash(filepath.Join(task.RemotePath, path))

						_, err = app.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
							Bucket: aws.String(bucket.Name),
							Key:    aws.String(objectKey),
						})
						if err != nil {
							if apiError, ok := errors.AsType[smithy.APIError](err); ok {
								switch apiError.(type) {
								case *types.NotFound:
									app.uploadCh <- upload{
										absPath: path,
										key:     objectKey,
										bucket:  bucket.Name,
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
	slog.Info("starting upload handlers")

	for range app.config.Concurrency {
		app.wg.Go(func() {
			app.uploadFile(ctx)
		})
	}

	slog.Info("started upload handlers")
	slog.Info("scheduling bucket tasks")

	for _, bucket := range app.config.Buckets {
		slog.Info("scheduling tasks for bucket", "bucket", bucket.Name)
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
		slog.Info("scheduled tasks for bucket", "bucket", bucket.Name)
	}
}
