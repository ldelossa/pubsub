// +build integration

package pubsub

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
)

var projectID = flag.String("projectID", "expanded-system-198721", "Specifies GCP project ID to use for integration test")
var topicName = flag.String("topic", "pubsub-test", "Specifies PubSub topic to use for integration test")
var subName = flag.String("subscription", "pubsub-test", "Specifies PubSub subscription to use for integration test")

// Setup creates the necessary fixtures and returns a teardown function
// to be called at the end of a test
func Setup(t *testing.T) (*pubsub.Topic, *pubsub.Subscription, func()) {
	v := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if v == "" {
		t.Fatal("Test not ran with GOOGLE_APPLICATION_CREDENTIALS environment variable")
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Create a client
	client, err := pubsub.NewClient(ctx, *projectID)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	// Create test topic
	topic, err := client.CreateTopic(ctx, *topicName)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Create test subscription
	sub, err := client.CreateSubscription(ctx, *subName,
		pubsub.SubscriptionConfig{Topic: topic})
	if err != nil {
		t.Fatalf("could not create subscriber: %s", err)
	}

	// teardown function gets topic and sub via closure
	teardown := func() {
		ctx := context.Background()
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		// Cleanup
		err = sub.Delete(ctx)
		if err != nil {
			log.Printf("could not delete subscription. Must delete manually: %s", err)
		}
		err = topic.Delete(ctx)
		if err != nil {
			log.Printf("could not delete topic. Must delete manually: %s", err)
		}

		// Stop topic
		topic.Stop()

		// Close client
		client.Close()

		// confirm deletions
		for {
			subex, suberr := sub.Exists(context.Background())

			topicex, toperr := topic.Exists(context.Background())

			if (suberr != nil) && (toperr != nil) {
				log.Printf("teardown: confirmed subscription and topic deletion")
				break
			}

			if subex && topicex {
				log.Printf("teardown: confirmed subscription and topic deletion")
				break
			}
		}

		log.Printf("pubsub env teardown finished")
	}

	return topic, sub, teardown

}

func TestEnqueuerEnqueue(t *testing.T) {
	// Setup pubsub test env and tear down
	topic, sub, teardown := Setup(t)
	defer teardown()

	// Create Enqueuer
	e, err := NewEnqueuer(topic)
	if err != nil {
		t.Fatalf("could not create enqueuer: %s", err)
	}

	// Create message
	m := struct{ Msg string }{"test"}

	// Create channel to receive message on subscription for
	sChan := make(chan *pubsub.Message)
	go func(sChan chan *pubsub.Message) {
		err := sub.Receive(context.Background(), func(ctx context.Context, m *pubsub.Message) {
			m.Ack()
			sChan <- m
			return
		})
		if err != nil {
			t.Fatalf("failed to receive message on subscription")
		}
	}(sChan)

	// Start Enqueuer
	go e.Start()
	defer e.Stop()

	// Attempt enqueue of stream
	j, _ := json.Marshal(&m)
	err = e.Enqueue(j)
	if err != nil {
		t.Fatalf("failed to enqueue pubsub message: %s", err)
	}

	// Wait on subscriber channel for message
	d := &pubsub.Message{}

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	select {
	case dd := <-sChan:
		d = dd
	case <-ticker.C:
		t.Fatalf("did not receieve message from subscription within 1 minutes")
	}

	// Deserialize
	var mm struct{ Msg string }
	err = json.Unmarshal(d.Data, &mm)
	if err != nil {
		t.Fatalf("could not deserialize message into stream: %s", err)
	}

	// Confirm stream is intact
	if mm.Msg != m.Msg {
		t.Fatalf("expected key %s but received %s", mm.Msg, m.Msg)
	}
}

func TestDequeuerDequeue(t *testing.T) {
	topic, sub, teardown := Setup(t)
	defer teardown()

	// Create new dequeuer
	d, err := NewDequeuer(sub)
	if err != nil {
		t.Fatalf("received error when creating dequeuer: %s", err)
	}

	// Test that calling Stop before Start returns an error
	err = d.Stop()
	if err == nil {
		t.Fatalf("called Stop before Start and did not receive error")
	}

	// Launch as a go routine
	go d.Start()
	defer d.Stop()

	// Publish message to subscription
	data, _ := json.Marshal(struct{ Msg string }{"test"})
	result := topic.Publish(context.Background(), &pubsub.Message{
		Data: data,
	})

	// create context with deadline
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// wait until deadline for publish
	_, err = result.Get(ctx)
	if err != nil {
		t.Fatalf("could not publish message: %s", err)
	}

	// Block with ticker timeout to test dequeue
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	select {
	case <-ticker.C:
		t.Fatalf("could not dequeue a message within 1 minute")
	case m := <-d.Dequeue():
		// attempt to deserialize m
		var mm struct{ Msg string }
		err := json.Unmarshal(m, &mm)
		if err != nil {
			t.Fatalf("could not unmarshal received message to expected struct: %s", err)
		}

		// confirm Msg field is what we expect
		if mm.Msg != "test" {
			t.Fatalf("received unexpected message. wanted %s but got %s", "test", mm.Msg)
		}
	}
}

func TestMultiDequeueEnqueue(t *testing.T) {
	topic, sub, teardown := Setup(t)
	defer teardown()

	// Number of messages to test enqueue and dequeue
	msgN := 15

	// Create new dequeuer
	d, err := NewDequeuer(sub)
	if err != nil {
		t.Fatalf("received error when creating dequeuer: %s", err)
	}

	// Create Enqueuer
	e, err := NewEnqueuer(topic)
	if err != nil {
		t.Fatalf("could not create enqueuer: %s", err)
	}

	// Start Enqueuer and Dequeuer
	go d.Start()
	defer d.Stop()
	go e.Start()
	defer e.Stop()

	// Enqueue multiple messages
	for i := 0; i < msgN; i++ {
		e.Enqueue([]byte(`test`))
	}

	// Make channel of bytes to hold Dequeue results
	resChan := make(chan []byte, msgN)

	// Dequeue messages
	ticker := time.NewTicker(15 * time.Second)
	for {
		select {
		case b := <-d.Dequeue():
			resChan <- b
			if len(resChan) == msgN {
				return
			}
		case <-ticker.C:
			t.Fatalf("failed to dequeue a message within 15 seconds")
		}
	}

}
