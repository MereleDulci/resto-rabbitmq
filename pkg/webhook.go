package background

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"github.com/MereleDulci/jsonapi"
	"github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"io"
	"net/http"
	"strconv"
	"time"
)

func MakeSignedWebhook(queue string, publicKeyPem []byte) http.HandlerFunc {

	block, _ := pem.Decode(publicKeyPem)
	if block == nil {
		panic("failed to parse PEM block containing the public key")
	}

	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		panic("failed to parse DER encoded public key: " + err.Error())
	}

	key, ok := pub.(ed25519.PublicKey)
	if !ok {
		panic("invalid public key")
	}

	channel, err := ChannelFromPool("publisher-webhook", nil)
	if err != nil {
		panic(err)
	}

	if err := channel.ExchangeDeclare("webhook", "direct", true, false, false, false, nil); err != nil {
		panic(err)
	}

	if _, err := channel.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		panic(err)
	}

	if err := channel.QueueBind(queue, queue, "webhook", false, nil); err != nil {
		panic(err)
	}

	return func(w http.ResponseWriter, r *http.Request) {

		bodyBuf, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 128*1024))
		if err != nil {
			var maxBytesErr *http.MaxBytesError
			if errors.As(err, &maxBytesErr) {
				sendError(w, http.StatusRequestEntityTooLarge, err)
				return
			}
			sendError(w, http.StatusBadRequest, fmt.Errorf("reading request body: %w", err))
			return
		}

		signature := r.Header.Get("X-Signature")
		if signature == "" {
			sendError(w, http.StatusBadRequest, errors.New("missing signature"))
			return
		}

		bsig, err := hex.DecodeString(signature)
		if err != nil {
			sendError(w, http.StatusBadRequest, fmt.Errorf("decoding signature: %w", err))
			return
		}

		if !ed25519.Verify(key, bodyBuf, bsig) {
			sendError(w, http.StatusUnauthorized, errors.New("invalid signature"))
		}

		message := WebhookMessageBody{
			Signature: signature,
			Payload:   bodyBuf,
		}

		body, err := json.Marshal(message)
		if err != nil {
			sendError(w, http.StatusInternalServerError, err)
			return
		}

		registry.Enqueue(queue)
		if err := channel.PublishWithContext(r.Context(), "webhook", queue, true, false, amqp091.Publishing{
			MessageId:    primitive.NewObjectID().Hex(),
			Timestamp:    time.Now(),
			DeliveryMode: amqp091.Persistent,
			ContentType:  "application/json",
			Body:         body,
		}); err != nil {
			sendError(w, http.StatusInternalServerError, err)
			return
		}

		_, _ = w.Write([]byte("OK"))
	}
}

func sendError(w http.ResponseWriter, status int, err error) {
	raw, marshalErr := jsonapi.MarshalErrors([]*jsonapi.JSONAPIError{
		{
			ID:     primitive.NewObjectID().Hex(),
			Status: strconv.Itoa(status),
			Title:  err.Error(),
			Detail: err.Error(),
		},
	})

	if marshalErr != nil {
		return
	}

	w.WriteHeader(status)
	_, sendErr := w.Write(raw)

	if sendErr != nil {
		return
	}
}
