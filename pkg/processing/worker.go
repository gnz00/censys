package processing

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
)

type Worker struct {
	id       string
	ctx      context.Context
	cancelFn context.CancelFunc
	messages chan *pubsub.Message
	wg       sync.WaitGroup
	db       ScanStore
}

func (w *Worker) Start() {
	println("starting worker: " + w.id)
	w.wg.Add(1)
	defer w.wg.Done()

	for {
		select {
		case msg := <-w.messages:
			println(fmt.Sprintf("worker %s, msg %s", w.id, msg.ID))
			normalized := Message{}
			err := json.Unmarshal(msg.Data, &normalized)
			if err != nil {
				msg.Nack()
				continue
			}

			err = w.db.SaveScan(w.ctx, normalized)
			if err != nil {
				msg.Nack()
				continue
			}

			msg.Ack()
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *Worker) Stop() {
	w.cancelFn()
	<-w.ctx.Done()
	println("stopped worker: " + w.id)
}

func NewWorker(ctx context.Context, messages chan *pubsub.Message, wg *sync.WaitGroup, db ScanStore) (*Worker, error) {
	workerCtx, cancelFn := context.WithCancel(ctx)

	return &Worker{
		id:       uuid.NewString(),
		ctx:      workerCtx,
		cancelFn: cancelFn,
		messages: messages,
		wg:       sync.WaitGroup{},
		db:       db,
	}, nil
}
