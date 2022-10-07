## syntax=docker/dockerfile:1
#
###
### Build
###
#FROM golang:1.18-alpine AS build
#
#WORKDIR /app
#
#COPY go.mod ./
#RUN go mod download
#
#COPY . ./
#
#RUN CGO_ENABLED=0 GOOS=linux go build -o /master-docker-go-server/cmd/main,go
#
###
### Deploy
###
#FROM scratch
#
#WORKDIR /
#
#COPY --from=build /master-docker-go-server/cmd /master-docker-go-server/cmd
#
#EXPOSE 9099
#
#ENTRYPOINT ["/master-docker-go-server/"]

FROM golang:1.18-alpine AS build
WORKDIR /app

RUN docker run -itd --name exporter quay.io/prometheus/node-exporter:latest --path.procfs=/proc1 --collector.filesystem.ignored-mount-points='^/(rootfs/)?(dev|etc|host|proc|run|sys|volume1)($$|/)'

COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download

ADD . /app

EXPOSE 9099
EXPOSE 2345
RUN CGO_ENABLED=0 GOOS=linux go build ./cmd/main.go


ENTRYPOINT ["./main"]