package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/EventStore/EventStore-Client-Go/esdb"
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
		ContentType: esdb.JsonContentType,
		EventType:   "TestEvent",
		Data:        data,
	}
	fmt.Println(eventData)
	result, err := db.AppendToStream(context.Background(), streamName, esdb.AppendToStreamOptions{
		//ExpectedRevision: esdb.NoStream{},
		ExpectedRevision: esdb.Revision(2),
	}, eventData)
	if err != nil {
		if errors.Is(err, esdb.ErrWrongExpectedStreamRevision) {
			fmt.Println("is wrong revision")
		}
		log.Fatal("failed to append to stream:", err)
	}
	fmt.Printf("appended: %+v\n", result)

	stream, err := db.ReadStream(context.Background(), streamName, esdb.ReadStreamOptions{
		Direction: esdb.Forwards,
	}, 10)
	if err != nil {
		log.Fatal("failed to read stream:", err)
	}
	defer stream.Close()

	for {
		event, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			log.Fatal("failed to read event:", err)
		}

		fmt.Printf("event: %+v %+v\n", event.Event, event.OriginalEvent())
		fmt.Println("last revision:", event.OriginalEvent().EventNumber)
	}
}
