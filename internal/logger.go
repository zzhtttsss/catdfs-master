package internal

import (
	"context"
	"encoding/json"
	"github.com/sirupsen/logrus"
	"io"
	"os"
)

type Logger struct {
	*logrus.Logger
}

var Log = &Logger{}

func init() {
	Log.SetFormatter(&logrus.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
	})
	writer1 := os.Stdout
	writer2, err := os.OpenFile(".\\edits.txt", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		Log.Fatalf("Create file log.txt failed: %v\n", err)
	}
	Log.SetOutput(io.MultiWriter(writer1, writer2))
}

func (l *Logger) Info(a any) {
	data, err := json.Marshal(a)
	if err != nil {
		Log.Warnf("Marshal failed: %v\n", err)
	}
	l.WithContext(context.Background()).Info(data)
}
