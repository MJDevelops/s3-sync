FROM golang:1.26.0-alpine AS build
WORKDIR /app
COPY . .
RUN go mod download
RUN go build -o /app/s3-sync

FROM alpine:latest
RUN mkdir -p /app/config
WORKDIR /app
COPY --from=build /app/s3-sync .
CMD [ "./s3-sync" ]