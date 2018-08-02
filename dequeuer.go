package pubsub

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/pubsub"
)

// Dequeuer interface expects the Dequeue method to be implemented
type Dequeuer interface {
	// Dequeue returns a receive only channel returning a byte array
	// Callers are expected to decode the byte array into the expected format
	Dequeue() <-chan []byte
}

// D implements Dequeuer.
// D listens on a Subscription, unpacks inbound messages, and
type D struct {
	sub *pubsub.Subscription
	// internal channel
	psChan chan []byte
	cancel context.CancelFunc
}

// NewDequeuer returns a D struct with a .Start() method ready to be called.
// External caller should run Start() as a go routine.
func NewDequeuer(sub *pubsub.Subscription) (*D, error) {
	// Create internal channel
	psChan := make(chan []byte, 65535)

	// Create D
	d := &D{
		sub:    sub,
		psChan: psChan,
	}
	return d, nil
}

// Start begins receiving off the pubsub Subscription and sends the unpacked
// pubsub messages to it's internal channel. Expected to be called as a goroutine
func (d *D) Start() {
	// inChan we be accessed via closure
	var inChan chan<- []byte = d.psChan

	// create a context to cancel our Receive call
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	// keep handle to cancel func for Stop() method
	d.cancel = cancel

	// TODO: Should this be put into a retry loop?
	err := d.sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		// Send data onto inChan
		// IN THE ACT OF SLOW CONSUMERS MESSAGES WILL BE REQUEUED TO PUBSUB IF DOWNSTREAM BUFFER IS FULL
		select {
		case inChan <- m.Data:
			// Successfully pushed message downstream, ACK message
			m.Ack()
			log.Printf("enqueued message onto internal channel. current channel length: %d", len(d.psChan))
		default:
			// Could not push message downstream, requeue to pubsub server
			m.Nack()
			log.Printf("could not enqueue received message. current channel buffer: %d. indicates slow consumers. Message has been requeued to pubsub", len(d.psChan))
		}

		return
	})
	if err != nil {
		log.Printf("received error on pubsub Receive, Dequeuer is not receiving messages: %s", err)
	}

}

// Stop calls the cancel function created when Start() is called. Expected
// to be called synchronously
func (d *D) Stop() error {
	if d.cancel == nil {
		return fmt.Errorf("cannot call Stop() before Start()")
	}

	// Call cancel received from Start()
	d.cancel()

	// Close channels, clients are expected to drain and check for zero value
	close(d.psChan)

	return nil
}

// Dequeue implements the Dequeuer interface. Returns a receieve only channel of byte arrays
// Clients are free to use this channel as the see fit.
// Clients are expected to handle this channel being closed gracefully.
func (d *D) Dequeue() <-chan []byte {
	return d.psChan
}
