package mock

type MockDequeuer struct {
	inChan chan []byte
}

func (m *MockDequeuer) Dequeue() <-chan []byte {
	return m.inChan
}

func NewMockDequeur() (*MockDequeuer, chan<- []byte) {
	// create channel
	c := make(chan []byte, 1024)

	// Create MockDequeuer
	m := &MockDequeuer{
		inChan: c,
	}

	return m, c
}
