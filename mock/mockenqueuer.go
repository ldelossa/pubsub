package mock

type GoodEnqueuer struct{}

func (e *GoodEnqueuer) Enqueue(b []byte) error {
	return nil
}
