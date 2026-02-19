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

type GetBucketKeysOpts struct {
	BucketName string
	Prefix     string
}

func (app *Application) getBucketKeys(ctx context.Context, opts *GetBucketKeysOpts) (map[string]struct{}, error) {
	keys := make(map[string]struct{})

	objectsPaginator := s3.NewListObjectsV2Paginator(app.s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(opts.BucketName),
		Prefix: aws.String(opts.Prefix),
	})

	for objectsPaginator.HasMorePages() {
		objectsOutput, err := objectsPaginator.NextPage(ctx)
		if err != nil {
			slog.Error(err.Error())
			return nil, err
		}

		for _, object := range objectsOutput.Contents {
			keys[aws.ToString(object.Key)] = struct{}{}
		}
	}

	return keys, nil
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
				slog.Info("acquiring object keys", "bucket", bucket.Name, "remote", task.RemotePath)
				keys, err := app.getBucketKeys(ctx, &GetBucketKeysOpts{
					BucketName: bucket.Name,
					Prefix:     task.RemotePath,
				})
				if err != nil {
					slog.Error(err.Error())
					return
				}
				slog.Info("acquired object keys", "bucket", bucket.Name, "remote", task.RemotePath)

				fastwalk.Walk(nil, task.LocalPath, func(path string, d fs.DirEntry, err error) error {
					if err != nil {
						slog.Error(err.Error())
						return nil
					}

					if !d.IsDir() {
						relPath, err := filepath.Rel(task.LocalPath, path)
						if err != nil {
							slog.Error(err.Error())
							return nil
						}

						objectKey := filepath.ToSlash(filepath.Join(task.RemotePath, relPath))
						if _, ok := keys[objectKey]; !ok {
							go func() {
								app.uploadCh <- upload{
									absPath: path,
									key:     objectKey,
									bucket:  bucket.Name,
								}
							}()
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
