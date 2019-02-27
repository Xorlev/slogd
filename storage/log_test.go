package storage

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/xorlev/slogd/internal"
	pb "github.com/xorlev/slogd/proto"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

func TestLog_End2End(t *testing.T) {
	tmpDir := tempdir()
	defer os.Remove(tmpDir)

	zap := logger()
	log := setupLog(zap, tmpDir)

	go func(ch LogEntryChannel) {
		for {
			_, ok := <-ch
			if !ok {
				break
			}
		}
	}(log.LogChannel())

	// Produce messages
	for i := uint64(0); i < 1000; i++ {
		ctx := context.WithValue(context.Background(), "requestStart", time.Now())
		offset, err := log.Append(ctx, []*pb.LogEntry{
			{
				Entry: &pb.LogEntry_RawBytes{
					RawBytes: []byte("helloworld"),
				},
			},
		})

		if err != nil {
			t.Fatal(err)
		}

		if len(offset) > 1 {
			t.Fatal("More than 1 offset returned, expected 1")
		}

		if offset[0] != i {
			t.Fatalf("Expected offset %d, Append returned: %d", i, offset[0])
		}
	}

	// Consume them again one at a time
	for i := uint64(0); i < 1000; i++ {
		q := &LogQuery{
			StartOffset: i,
			MaxMessages: 1,
		}

		retrieval, err := log.Retrieve(context.Background(), q)
		if err != nil {
			t.Fatal(err)
		}

		if len(retrieval.Logs) != 1 {
			t.Fatalf("Expected 1 log, found: %d", len(retrieval.Logs))
		}

		if retrieval.Logs[0].Offset != i {
			t.Fatalf("Expected log at offset %d, actual: %+v", i, retrieval.Logs[0])
		}
	}

	// Consume them all at once
	{
		q := &LogQuery{
			StartOffset: 0,
			MaxMessages: 1000,
		}

		retrieval, err := log.Retrieve(context.Background(), q)
		if err != nil {
			t.Fatal(err)
		}

		if len(retrieval.Logs) != 1000 {
			t.Fatalf("Expected 1000 log, found: %d", len(retrieval.Logs))
		}

		for i, log := range retrieval.Logs {
			if log.Offset != uint64(i) {
				t.Fatalf("Expected log at offset %d, actual: %+v", i, log)
			}
		}
	}

	// Consume them 50 at a time via cursor
	{
		logsPerBatch := uint32(50)

		q := &LogQuery{
			StartOffset: 0,
			MaxMessages: logsPerBatch,
		}

		ch := make(LogEntryChannel, 1)

		// log.Retrieve(context.Background(), q, continuation)
		cursor := NewCursor(context.Background(), q, ch, logsPerBatch)

		zap.Sync()

		consumed := make(LogEntryChannel, 1000)
		err := cursor.Consume(log, func(resp interface{}) error {
			response := resp.(*pb.GetLogsResponse)

			zap.Sync()

			if len(response.Logs) != 1 {
				t.Fatalf("Expected 1 log, had: %d", len(response.Logs))
			}

			for _, l := range response.Logs {
				consumed <- &internal.TopicAndLog{
					LogEntry: l,
					Topic:    "end2end",
				}
			}
			return nil
		})
		if err != nil {
			panic(err)
		}

		i := uint64(0)
		for entry := range consumed {
			if entry.LogEntry.Offset != i {
				t.Fatalf("Expected offset %d, found %+v", i, entry)
			}
			i += 1

			if i == 1000 {
				break
			}
		}

		close(consumed)
		close(ch)
	}

	// Consume the latest 50 at a time via cursor
	{
		logsPerBatch := uint32(50)

		q := &LogQuery{
			MaxMessages: logsPerBatch,
			Position: LATEST,
		}

		ch := make(LogEntryChannel, 1)

		// log.Retrieve(context.Background(), q, continuation)
		cursor := NewCursor(context.Background(), q, ch, logsPerBatch)

		zap.Sync()

		consumed := make(LogEntryChannel, 1000)
		err := cursor.Consume(log, func(resp interface{}) error {
			response := resp.(*pb.GetLogsResponse)

			zap.Sync()

			if len(response.Logs) != 1 {
				t.Fatalf("Expected 1 log, had: %d", len(response.Logs))
			}

			for _, l := range response.Logs {
				consumed <- &internal.TopicAndLog{
					LogEntry: l,
					Topic:    "end2end",
				}
			}
			return nil
		})
		if err != nil {
			panic(err)
		}

		i := uint64(950)
		for entry := range consumed {
			if entry.LogEntry.Offset != i {
				t.Fatalf("Expected offset %d, found %+v", i, entry)
			}
			i += 1

			if i == 1000 {
				break
			}
		}

		close(consumed)
		close(ch)
	}

	log.Close()
}

func BenchmarkLogWrite(b *testing.B) {
	tmpDir := tempdir()
	defer os.Remove(tmpDir)

	log := setupLog(zap.NewNop().Sugar(), tmpDir)

	go func(ch LogEntryChannel) {
		for {
			_, ok := <-ch
			if !ok {
				break
			}
		}
	}(log.LogChannel())

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		ctx := context.WithValue(context.Background(), "requestStart", time.Now())
		_, err := log.Append(ctx, []*pb.LogEntry{
			{
				Entry: &pb.LogEntry_RawBytes{
					RawBytes: []byte("helloklsfasd;lfjsdlk;fjls;dkaworld"),
				},
			},
		})
		if err != nil {
			panic(err)
		}
	}
}

func setupLog(logger *zap.SugaredLogger, tmpDir string) *FileLog {
	log, err := NewFileLog(logger, tmpDir, "end2end")
	log.config = &pb.TopicConfig{
		MessageSizeLimit:          4 * 1024 * 1024,
		SegmentSizeLimit:          4 * 1024, // force new segments every few messages
		IndexAfterBytes:           256,
		RotateSegmentAfterSeconds: 24 * 3600,
		StaleSegmentSeconds:       720 * 3600,
		LogMaintenancePeriod:      300,
	}
	if err != nil {
		panic(err)
	}
	return log
}

func logger() *zap.SugaredLogger {
	cfg := zap.NewDevelopmentConfig()
	logger, _ := cfg.Build()
	defer logger.Sync() // flushes buffer, if any

	return logger.Sugar()
}

func tempdir() string {
	f, err := ioutil.TempDir("", "slogd-")
	if err != nil {
		panic(err)
	}
	// if err := os.Mkdir(f, 0666); err != nil {
	// 	panic(err)
	// }
	// if err := os.Remove(f); err != nil {
	// 	panic(err)
	// }
	return f
}
