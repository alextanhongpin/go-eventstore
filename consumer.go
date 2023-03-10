package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/EventStore/EventStore-Client-Go/v3/esdb"
)

var (
	// Specific stream.
	streamName = "some-stream"
	groupName  = "some-stream-group"

	// All stream.
	streamPrefix = "some-"
	groupNameAll = "some-all-group"
)

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

	if err := upsertPersistentSubscription(db, streamName, groupName); err != nil {
		log.Fatal("failed to create persistent subscription:", err)
	}

	if false {
		ctx := context.Background()
		sub, err := db.SubscribeToPersistentSubscription(ctx, streamName, groupName, esdb.SubscribeToPersistentSubscriptionOptions{
			RequiresLeader: true,
		})
		if err != nil {
			log.Fatal("failed to connect to persistent subscription:", err)
		}
		defer func() {
			if err := sub.Close(); err != nil {
				log.Fatal("failed to close persistent subscription:", err)
			}
		}()

		for {
			event := sub.Recv()

			if event.EventAppeared != nil {
				fmt.Println("event:", event.EventAppeared.Event)
				// Do some processing.
				e := event.EventAppeared.Event.Event
				switch e.EventType {
				case "TestEvent":
					var te TestEvent
					if err := json.Unmarshal(e.Data, &te); err != nil {
						log.Fatal("failed to unmarshal", err)
					}
					fmt.Println("unmarshaled", te)
				default:
					panic("unhandled event")
				}

				// Acknowledge the event to increment the offset.
				if err := sub.Ack(event.EventAppeared.Event); err != nil {
					log.Fatal("failed to ack event:", err)
				}
			}

			if event.SubscriptionDropped != nil {
				fmt.Println(event.SubscriptionDropped.Error)
				break
			}
		}
	}

	// Subscribe to $all, but with the prefix.

	{
		options := esdb.PersistentAllSubscriptionOptions{
			Filter: &esdb.SubscriptionFilter{
				Type:     esdb.StreamFilterType,
				Prefixes: []string{streamPrefix},
			},
		}

		fmt.Println("subscribed to $all")
		err := db.CreatePersistentSubscriptionToAll(context.Background(), groupNameAll, options)
		if err != nil {
			if isCode(err, esdb.ErrorCodeResourceAlreadyExists) {
				fmt.Println("duplicate subscription")
			} else {
				log.Fatal("failed to create persistent subscription to $all:", err)
			}
		}
		sub, err := db.SubscribeToPersistentSubscriptionToAll(context.Background(), groupNameAll, esdb.SubscribeToPersistentSubscriptionOptions{})

		if err != nil {
			panic(err)
		}

		for {
			event := sub.Recv()
			fmt.Println("event", event)

			if event.EventAppeared != nil {
				fmt.Println("got event all", event.EventAppeared.Event.Event)
				if err := sub.Ack(event.EventAppeared.Event); err != nil {
					log.Fatal("failed to ack:", err)
				}
			}

			if event.SubscriptionDropped != nil {
				break
			}
		}
	}
}

func upsertPersistentSubscription(client *esdb.Client, streamName, groupName string) error {
	ctx := context.Background()
	options := esdb.PersistentStreamSubscriptionOptions{
		StartFrom: esdb.Start{},
	}

	err := client.CreatePersistentSubscription(ctx, streamName, groupName, options)
	if isCode(err, esdb.ErrorCodeResourceAlreadyExists) {
		fmt.Println("persistent subscription already exists", streamName, groupName)
		return nil
	}

	if err != nil {
		return err
	}

	return nil
}

func isCode(err error, code esdb.ErrorCode) bool {
	var esdbErr *esdb.Error
	if ok := errors.As(err, &esdbErr); ok {
		return esdbErr.Code() == code
	}

	return false
}
