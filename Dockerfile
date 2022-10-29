FROM golang:1.18-alpine AS build
WORKDIR /app

COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download

ADD . /app

EXPOSE 9099
EXPOSE 2345
RUN CGO_ENABLED=0 GOOS=linux go build ./cmd/main.go


ENTRYPOINT ["./main"]