package background

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/MereleDulci/resto/pkg/typecast"
	"github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"slices"
	"time"
)

const MethodSchedule = "SCHEDULE"

type Processor func(ctx context.Context, body *ResourceMessageBody) error
type WHProcessor func(ctx context.Context, body *WebhookMessageBody) error

type Handler interface {
	Queue() string
	StartProcessor() chan error
}

type ServeConfig string

type Webhook struct {
	queue     string
	processor WHProcessor
	timeout   time.Duration
	errchan   chan error
}

type WebhookMessageBody struct {
	Signature string `json:"signature"`
	Payload   []byte `json:"payload"`
}

type Handle struct {
	methods   []string
	processor Processor
	timeout   time.Duration
	kind      string
	queue     string
	errchan   chan error
}

type ResourceMessageBody struct {
	ResourceType string      `json:"resourceType"`
	ResourceId   string      `json:"resourceId"`
	Method       string      `json:"method"`
	When         string      `json:"when"`
	Payload      interface{} `json:"payload"`
}

func (b *ResourceMessageBody) UnmarshalJSON(data []byte) error {
	type Alias ResourceMessageBody
	v := &Alias{}
	if err := json.Unmarshal(data, v); err != nil {
		return err
	}

	b.ResourceType = v.ResourceType
	b.ResourceId = v.ResourceId
	b.Method = v.Method
	b.When = v.When

	asList, ok := v.Payload.([]interface{})
	var e error
	if ok {
		patches := make([]typecast.PatchOperation, len(asList))
		for i, op := range asList {
			asMap, ok := op.(map[string]interface{})
			if !ok || asMap["op"] == nil || asMap["path"] == nil {
				e = errors.New("invalid patch payload")
				break
			}
			patches[i] = typecast.PatchOperation{
				Op:    asMap["op"].(string),
				Path:  asMap["path"].(string),
				Value: asMap["value"],
			}
		}
		b.Payload = patches
	} else {
		b.Payload = v.Payload
	}

	if e != nil {
		b.Payload = v.Payload
	}

	return nil
}

func NewHandle(queue string, methods []string) *Handle {
	return &Handle{
		queue:   queue,
		methods: methods,
		timeout: time.Minute * 5,
		kind:    "clear",
	}
}

func (h *Handle) Queue() string {
	return h.queue
}

func (h *Handle) WithTimeout(timeout time.Duration) *Handle {
	h.timeout = timeout
	return h
}

func (h *Handle) Fanout(resourceNames ...ServeConfig) *Handle {
	if h.kind != "clear" && h.kind != "fanout" {
		panic(fmt.Errorf("cannot change queue type from %s to fanout", h.kind))
	}
	h.kind = "fanout"

	c, err := MakeNewConnection()
	if err != nil {
		panic(err)
	}
	defer c.Close()

	ch, err := ChannelFromPool(fmt.Sprintf("%s-producer", "worker"), h.errchan)
	if err != nil {
		panic(err)
	}
	defer ch.Close()

	if err := ch.ExchangeDeclare("resource", "topic", true, false, false, false, nil); err != nil {
		panic(err)
	}

	if _, err := ch.QueueDeclare(
		h.queue,
		true,
		false,
		false,
		false,
		nil); err != nil {
		panic(err)
	}

	for _, resourceName := range resourceNames {
		registry.Register(string(resourceName))

		if err := ch.QueueBind(h.queue, string(resourceName), "resource", false, nil); err != nil {
			panic(err)
		}
	}

	return h
}

func (h *Handle) WithRPC(rpcQueue string) *Handle {
	ch, err := ChannelFromPool(fmt.Sprintf("%s-producer", "worker"), h.errchan)
	if err != nil {
		panic(err)
	}

	if err := ch.ExchangeDeclare("rpc", "direct", false, false, false, false, nil); err != nil {
		panic(err)
	}

	if _, err := ch.QueueDeclare(fmt.Sprintf("rpc.%s.requests", rpcQueue), false, false, false, false, nil); err != nil {
		panic(err)
	}
	if err := ch.QueueBind(fmt.Sprintf("rpc.%s.requests", rpcQueue), fmt.Sprintf("rpc.%s.requests", rpcQueue), "rpc", false, nil); err != nil {
		panic(err)
	}

	if _, err := ch.QueueDeclare(fmt.Sprintf("rpc.%s.replies", rpcQueue), false, false, false, false, nil); err != nil {
		panic(err)
	}
	if err := ch.QueueBind(fmt.Sprintf("rpc.%s.replies", rpcQueue), fmt.Sprintf("rpc.%s.replies", rpcQueue), "rpc", false, nil); err != nil {
		panic(err)
	}

	return h
}

func (h *Handle) Declare() *Handle {

	ch, err := ChannelFromPool(fmt.Sprintf("%s-producer", "worker"), h.errchan)
	if err != nil {
		panic(err)
	}

	if err := ch.ExchangeDeclare("schedule", "topic", true, false, false, false, nil); err != nil {
		panic(err)
	}

	if _, err := ch.QueueDeclare(
		h.queue,
		true,
		false,
		false,
		false,
		nil); err != nil {
		panic(err)
	}

	if err := ch.QueueBind(h.queue, h.queue, "schedule", false, nil); err != nil {
		panic(err)
	}

	if err := ch.Close(); err != nil {
		panic(err)
	}

	registry.Register(h.queue)
	return h
}

func (h *Handle) Every(interval time.Duration) *Handle {
	if h.kind != "clear" {
		panic(fmt.Errorf("cannot change queue type from %s to every", h.kind))
	}
	h.kind = "every"

	ch, err := ChannelFromPool(fmt.Sprintf("%s-producer", "worker"), h.errchan)
	if err != nil {
		panic(err)
	}

	if err := ch.ExchangeDeclare("schedule", "topic", true, false, false, false, nil); err != nil {
		panic(err)
	}

	if _, err := ch.QueueDeclare(
		h.queue,
		true,
		false,
		false,
		false,
		nil); err != nil {
		panic(err)
	}

	if err := ch.QueueBind(h.queue, h.queue, "schedule", false, nil); err != nil {
		panic(err)
	}

	go func() {
		defer ch.Close()
		ctx := context.Background()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			payload := ResourceMessageBody{
				ResourceType: h.queue,
				ResourceId:   fmt.Sprintf("%s-%d", h.queue, time.Now().Unix()),
				Method:       MethodSchedule,
				When:         "scheduled",
				Payload:      nil,
			}
			body, err := json.Marshal(payload)
			if err != nil {
				h.errchan <- err
				return
			}

			registry.Enqueue(h.queue)
			if err := ch.PublishWithContext(ctx, "schedule", h.queue, true, false, amqp091.Publishing{
				MessageId:    primitive.NewObjectID().Hex(),
				Timestamp:    time.Now(),
				DeliveryMode: amqp091.Persistent,
				ContentType:  "application/json",
				Body:         body,
			}); err != nil {
				h.errchan <- err
				return
			}
		}
	}()

	return h
}

func (h *Handle) StartProcessor() chan error {
	h.errchan = make(chan error, 3)
	if h.processor == nil {
		panic(errors.New("processor not set"))
	}

	ch, err := ChannelFromPool(fmt.Sprintf("%s-consumer", "worker"), h.errchan)
	if err != nil {
		h.errchan <- err
		return h.errchan
	}

	msgs, err := ch.Consume(h.queue, "", false, true, false, false, nil)
	if err != nil {
		h.errchan <- err
		return h.errchan
	}

	go func() {
		defer close(h.errchan)
		defer ch.Close()
		for d := range msgs {
			body := &ResourceMessageBody{}
			if unmarshalErr := json.Unmarshal(d.Body, body); unmarshalErr != nil {
				h.errchan <- unmarshalErr
				return
			}
			if !slices.Contains(h.methods, body.Method) {
				err := d.Ack(false)
				registry.Done(body.ResourceType)
				if err != nil {
					h.errchan <- err
					return
				}
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
			processingErr := h.processor(ctx, body)
			cancel()

			if processingErr != nil {
				if d.Redelivered {
					registry.Done(body.ResourceType)
				}
				if nackerr := d.Nack(false, !d.Redelivered); nackerr != nil {
					h.errchan <- nackerr
				}
				if closeErr := ch.Close(); closeErr != nil {
					h.errchan <- closeErr
				}
				h.errchan <- processingErr
				return
			}

			registry.Done(body.ResourceType)
			if err := d.Ack(false); err != nil {
				h.errchan <- err
				return
			}

		}
	}()

	return h.errchan
}

func (h *Handle) WithProcessor(fn Processor) *Handle {
	h.processor = fn
	return h
}

func NewWebhook(queue string) *Webhook {
	return &Webhook{
		queue:   queue,
		timeout: time.Minute * 5,
	}
}

func (w *Webhook) Queue() string {
	return w.queue
}

func (w *Webhook) WithProcessor(fn WHProcessor) *Webhook {
	w.processor = fn
	return w
}

func (w *Webhook) Declare() *Webhook {
	c, err := MakeNewConnection()
	if err != nil {
		panic(err)
	}
	defer c.Close()

	ch, err := ChannelFromPool("webhook-consumer", w.errchan)
	if err != nil {
		panic(err)
	}
	defer ch.Close()

	if _, err := ch.QueueDeclare(w.queue, true, false, false, false, nil); err != nil {
		panic(err)
	}

	registry.Register(w.queue)
	return w
}

func (w *Webhook) StartProcessor() chan error {

	w.errchan = make(chan error, 2)
	if w.processor == nil {
		panic(errors.New("processor not set"))
	}

	ch, err := ChannelFromPool(fmt.Sprintf("webhoook-consumer"), w.errchan)
	if err != nil {
		w.errchan <- err
		return w.errchan
	}

	msgs, err := ch.Consume(w.queue, "", false, true, false, false, nil)
	if err != nil {
		w.errchan <- err
		return w.errchan
	}

	go func() {
		defer close(w.errchan)
		defer ch.Close()

		for d := range msgs {
			body := &WebhookMessageBody{}
			if err := json.Unmarshal(d.Body, body); err != nil {
				w.errchan <- err
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
			defer cancel()

			if err := w.processor(ctx, body); err != nil {
				w.errchan <- err
				if d.Redelivered {
					registry.Done(w.queue)
				}

				if err := d.Nack(false, !d.Redelivered); err != nil {
					w.errchan <- err
				}

				return
			} else {
				if err := d.Ack(false); err != nil {
					w.errchan <- err
					return
				}

				registry.Done(w.queue)
			}
		}
	}()

	return w.errchan
}
