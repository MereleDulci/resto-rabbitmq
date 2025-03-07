package background

import (
	"context"
	"encoding/json"
	"github.com/MereleDulci/resto/pkg/hook"
	"github.com/MereleDulci/resto/pkg/resource"
	"github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

type Enqueuer func(context.Context, resource.Req, resource.Resourcer) error

func MakeDirectEnqueuePOST(resourceType string, db *mongo.Database) Enqueuer {
	channel, err := ChannelFromPool("publisher-enqueue-on-create", nil)
	if err != nil {
		panic(err)
	}

	return func(ctx context.Context, r resource.Req, record resource.Resourcer) error {
		message := ResourceMessageBody{
			ResourceType: resourceType,
			ResourceId:   record.GetID(),
			Method:       "POST",
			When:         "afterCreate",
			Payload:      record,
		}

		body, err := json.Marshal(message)
		if err != nil {
			return err
		}

		registry.Enqueue(resourceType)
		if err := channel.PublishWithContext(ctx, "resource", resourceType, true, false, amqp091.Publishing{
			MessageId:    primitive.NewObjectID().Hex(),
			Timestamp:    time.Now(),
			DeliveryMode: amqp091.Persistent,
			ContentType:  "application/json",
			Body:         body,
		}); err != nil {
			return err
		}
		go func() {
			_ = logResourceJob(db, &message)
		}()

		return nil
	}
}

func MakeDirectEnqueuePATCH(resourceType string, db *mongo.Database) Enqueuer {
	channel, err := ChannelFromPool("publisher-enqueue-on-update", nil)
	if err != nil {
		panic(err)
	}

	return func(ctx context.Context, r resource.Req, record resource.Resourcer) error {
		message := ResourceMessageBody{
			ResourceType: resourceType,
			ResourceId:   record.GetID(),
			Method:       "PATCH",
			When:         "afterUpdate",
			Payload:      r.Payload(),
		}

		body, err := json.Marshal(message)
		if err != nil {
			return err
		}

		registry.Enqueue(resourceType)
		if err := channel.PublishWithContext(ctx, "resource", resourceType, true, false, amqp091.Publishing{
			MessageId:    primitive.NewObjectID().Hex(),
			Timestamp:    time.Now(),
			DeliveryMode: amqp091.Persistent,
			ContentType:  "application/json",
			Body:         body,
		}); err != nil {
			return err
		}
		go func() {
			_ = logResourceJob(db, &message)
		}()

		return nil
	}
}

func logResourceJob(db *mongo.Database, message *ResourceMessageBody) error {
	collection := db.Collection("resource-jobs-log")
	_, err := collection.InsertOne(context.Background(), message)
	return err
}

func MakeResourceEnqueueHook(resourceName string, db *mongo.Database) hook.After {
	onPost := MakeDirectEnqueuePOST(resourceName, db)
	onPatch := MakeDirectEnqueuePATCH(resourceName, db)

	return func(ctx context.Context, r resource.Req, record resource.Resourcer) (resource.Resourcer, error) {
		switch r.Method() {
		case "POST":
			if err := onPost(ctx, r, record); err != nil {
				return nil, err
			}
		case "PATCH":
			if err := onPatch(ctx, r, record); err != nil {
				return nil, err
			}
		}

		return record, nil
	}
}
