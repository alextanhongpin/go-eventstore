package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/EventStore/EventStore-Client-Go/v3/esdb"
	"github.com/google/uuid"
)

var streamName string = "some-stream"

type TestEvent struct {
	ID      string `json:"id"`
	Message string `json:"message"`
}

func main() {
	settings, err := esdb.ParseConnectionString("esdb://localhost:2113?tls=false")
	if err != nil {
		log.Fatal("failed to parse connection string:", err)
	}

	db, err := esdb.NewClient(settings)
	if err != nil {
		log.Fatal("failed to create new client:", err)
	}
	defer db.Close()

	testEvent := TestEvent{
		ID:      uuid.New().String(),
		Message: "My first event",
	}
	data, err := json.Marshal(testEvent)
	if err != nil {
		log.Fatal("failed to marshal event", err)
	}
	eventData := esdb.EventData{
		ContentType: esdb.ContentTypeJson,
		EventType:   "TestEvent",
		Data:        data,
	}
	fmt.Println(eventData)

	revision, err := lastStreamRevision(context.Background(), db, streamName)
	if err != nil {
		panic(err)
	}
	fmt.Println("last revision is", revision)

	// For the first event, use `NO_STREAM` as the expected revision.
	result, err := db.AppendToStream(context.Background(), streamName, esdb.AppendToStreamOptions{
		//ExpectedRevision: esdb.NoStream{},
		ExpectedRevision: revision,
	}, eventData)
	if err != nil {
		if isCode(err, esdb.ErrorCodeWrongExpectedVersion) {
			log.Println("wrong revision", err)
		}
		log.Fatal("failed to append to stream:", err)
	}
	fmt.Printf("appended: %+v\n", result)

	stream, err := db.ReadStream(context.Background(), streamName, esdb.ReadStreamOptions{
		From:      esdb.Start{},
		Direction: esdb.Forwards,
	}, 10)
	if err != nil {
		// NOTE: We can't check for stream not found here.
		log.Fatal("failed to read stream:", err)
	}
	defer stream.Close()

	for {
		event, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if isCode(err, esdb.ErrorCodeResourceNotFound) {
			fmt.Println("stream not found")
			break
		}

		if err != nil {
			log.Fatal("failed to read event:", err)
		}

		fmt.Printf("event: %+v %+v\n", event.Event, event.OriginalEvent())
		fmt.Println("last revision:", event.OriginalEvent().EventNumber)

		switch event.Event.EventType {
		case "TestEvent":
			var e TestEvent
			if err := json.Unmarshal(event.Event.Data, &e); err != nil {
				log.Fatal("failed to unmarshal", err)
			}
			fmt.Println("unmarshaled", e)
		default:
			panic("unhandled event")
		}
	}
}

func lastStreamRevision(ctx context.Context, db *esdb.Client, streamName string) (esdb.ExpectedRevision, error) {
	stream, err := db.ReadStream(ctx, streamName, esdb.ReadStreamOptions{
		From:      esdb.End{},
		Direction: esdb.Backwards,
	}, 1)
	if err != nil {
		return nil, err
	}
	defer stream.Close()

	event, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		fmt.Println("EOF", err)
		return esdb.NoStream{}, nil
	}

	if err != nil {
		var esdbErr *esdb.Error
		if errors.As(err, &esdbErr) {
			if esdbErr.Code() == esdb.ErrorCodeResourceNotFound {
				return esdb.NoStream{}, nil
			}
		}
		return nil, err
	}

	return esdb.Revision(event.OriginalEvent().EventNumber), nil
}

func isCode(err error, code esdb.ErrorCode) bool {
	var esdbErr *esdb.Error
	if ok := errors.As(err, &esdbErr); ok {
		return esdbErr.Code() == code
	}

	return false
}
