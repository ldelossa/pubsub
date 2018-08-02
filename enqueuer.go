package pubsub

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/pubsub"
)

// Enqueuer interface expects the Enqueue method to be implemented
type Enqueuer interface {
	// Enqueue enqueues a stream object onto pubsub
	Enqueue(b []byte) error
}

// E implements the Enqueuer interface and is expected to be called as a long running go routine.
type E struct {
	t     *pubsub.Topic
	sChan chan []byte
	qChan chan struct{}
}

// NewEnqueuer returns a E struct with a .Start() method ready to be called.
// External caller should run Start() as a go routine.
// TODO: Consider making this take in a pubsub.Topic instead of creating it internally
func NewEnqueuer(topic *pubsub.Topic) (*E, error) {
	// Create channels
	sChan := make(chan []byte, 65535)
	qChan := make(chan struct{}, 1)

	// Create enqueuer
	e := &E{
		t:     topic,
		sChan: sChan,
		qChan: qChan,
	}

	return e, nil
}

// Start begins routing passed messages to the .enqueue function.
// Listens on sChan for passed in messages, qChan for a quit signal. TODO: May want to look more into context for this
func (e *E) Start() {
	for {
		select {
		case s := <-e.sChan:
			go e.enqueue(s)
		case <-e.qChan:
			log.Printf("recieved message on quit channel. enqueuer will drain sChan and quit")
			// set e.sChan and e.qChan to nil so Enqueue and Stop methods will begin to fail
			e.sChan = nil
			e.qChan = nil
			// drain sChan
			for st := range e.sChan {
				if st == nil {
					break
				}

				go e.enqueue(st)

			}
			return
		}
	}
}

// Stop sends an empty struct to E's quit channel.
// This will set e.qChan to nil and this method will beging to send errors upstream
func (e *E) Stop() error {
	select {
	case e.qChan <- struct{}{}:
		return nil
	default:
		return fmt.Errorf("could not send quit message to quit channel")
	}
}

// Enqueue implements the Enquerer interface and is the external API endpoint for enqueing a message
// to pubsub
func (e *E) Enqueue(b []byte) error {
	select {
	case e.sChan <- b:
		return nil
	default:
		return fmt.Errorf("could not enqueue message. buffer full or stream channel has been set to nil. buffer len %d", len(e.sChan))
	}
}

// enqueue is launched as a go routine to concurrenctly process pubsub publishes
func (e *E) enqueue(b []byte) {
	// publish message
	// this uses the publish settings defined by the instantiator of E
	result := e.t.Publish(context.Background(), &pubsub.Message{
		Data: b,
	})

	// block until result is returned, match publish timeout
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, e.t.PublishSettings.Timeout)
	defer cancel()

	id, err := result.Get(ctx)
	if err != nil {
		log.Printf("error returned publishing stream: %s", err)
		return
	}
	log.Printf("successfully published message %s", id)

}
