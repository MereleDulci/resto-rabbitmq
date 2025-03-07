package background

import (
	"context"
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"strconv"
	"time"
)

type RPCClient struct {
	serviceName string
}

func MakeRPCClient(serviceName string) *RPCClient {

	return &RPCClient{
		serviceName: serviceName,
	}
}

func (rpc *RPCClient) Request(ctx context.Context, payload []byte) ([]byte, error) {
	ch, err := ChannelFromPool(fmt.Sprintf("rpc-%s-client", rpc.serviceName), nil)
	if err != nil {
		panic(err)
	}
	defer ch.Close()

	msgs, err := ch.Consume(fmt.Sprintf("rpc.%s.replies", rpc.serviceName), "", true, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	correlationId := primitive.NewObjectID().Hex()

	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, fmt.Errorf("no deadline in context")
	}

	if err := ch.PublishWithContext(ctx, "rpc", fmt.Sprintf("rpc.%s.requests", rpc.serviceName), false, false, amqp091.Publishing{
		ContentType:   "application/json",
		CorrelationId: correlationId,
		ReplyTo:       fmt.Sprintf("rpc.%s.replies", rpc.serviceName),
		Body:          payload,
		Expiration:    strconv.FormatInt(time.Until(deadline).Milliseconds(), 10),
	}); err != nil {
		return nil, err
	}

	resultChan := make(chan []byte)
	defer close(resultChan)

	go func() {
		for d := range msgs {
			if d.CorrelationId == correlationId {
				resultChan <- d.Body
				return
			}
		}
	}()

	select {
	case out := <-resultChan:
		return out, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
