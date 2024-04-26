package processing

import (
	"context"
	"sync"

	"cloud.google.com/go/pubsub"
)

type WorkerPool struct {
	ctx      context.Context
	cancelFn context.CancelFunc
	workers  []*Worker
	wg       *sync.WaitGroup
	messages chan *pubsub.Message
	db       ScanStore
}

func (wp *WorkerPool) Start() error {
	for i := 0; i < len(wp.workers); i++ {
		worker, err := NewWorker(wp.ctx, wp.messages, wp.wg, wp.db)
		if err != nil {
			return err
		}

		wp.workers[i] = worker
		go worker.Start()
	}

	<-wp.ctx.Done()
	wp.Stop()

	return nil
}

func (wp *WorkerPool) Stop() {
	for i := 0; i < len(wp.workers); i++ {
		go wp.workers[i].Stop()
	}

	wp.cancelFn()
	wp.wg.Wait()
}

func NewWorkerPool(ctx context.Context, numWorkers int, messages chan *pubsub.Message, db ScanStore) *WorkerPool {
	c, cancelFn := context.WithCancel(ctx)
	return &WorkerPool{
		wg:       &sync.WaitGroup{},
		ctx:      c,
		cancelFn: cancelFn,
		workers:  make([]*Worker, numWorkers),
		messages: messages,
		db:       db,
	}
}
