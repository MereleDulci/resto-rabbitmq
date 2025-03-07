package background

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	"github.com/spf13/viper"
	"os"
	"strings"
	"sync"
	"time"
)

type rabbitPool struct {
	m   map[string]*amqp091.Connection
	mut sync.Mutex
}

var pool = rabbitPool{
	m:   map[string]*amqp091.Connection{},
	mut: sync.Mutex{},
}

func ChannelFromPool(connectionName string, closeNotifyChan chan error) (*amqp091.Channel, error) {
	pool.mut.Lock()
	defer pool.mut.Unlock()

	if connection, ok := pool.m[connectionName]; ok && !connection.IsClosed() {
		ch, err := connection.Channel()
		if err != nil {
			return nil, err
		}
		return ch, nil
	}

	connection, err := MakeNewConnection()
	if err != nil {
		return nil, err
	}

	go func() { //Subscribes to close notifications and passes error up if consumer provided a channel
		err := <-connection.NotifyClose(make(chan *amqp091.Error))
		pool.mut.Lock()
		defer pool.mut.Unlock()
		delete(pool.m, connectionName)

		if closeNotifyChan != nil {
			if err != nil {
				closeNotifyChan <- err
			} else {
				close(closeNotifyChan)
			}
		}
	}()

	channel, err := connection.Channel()
	if err != nil {
		return nil, err
	}

	pool.m[connectionName] = connection

	return channel, nil

}

func MakeNewConnection() (*amqp091.Connection, error) {
	ctx, cancel := context.WithTimeout(context.Background(), viper.GetDuration("RABBITMQ_CONNECTION_TIMEOUT")*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			if strings.Contains(viper.GetString("RABBITMQ_CONNECTION_STRING"), "amqps://") {
				cfg := new(tls.Config)
				cfg.RootCAs = x509.NewCertPool()

				if _, err := os.ReadFile(viper.GetString("RABBITMQ_CA_CERT_PATH")); err != nil {
					fmt.Println("unable to read ca cert file")
					panic(err)
				}
				if _, err := os.ReadFile(viper.GetString("RABBITMQ_CERT_PATH")); err != nil {
					fmt.Println("unable to read certPath")
					panic(err)
				}
				if _, err := os.ReadFile(viper.GetString("RABBITMQ_KEY_PATH")); err != nil {
					fmt.Println("unable to read keyPath")
					panic(err)
				}

				if ca, err := os.ReadFile(viper.GetString("RABBITMQ_CA_CERT_PATH")); err != nil {
					panic(err)
				} else {
					cfg.RootCAs.AppendCertsFromPEM(ca)
				}

				cert, err := tls.LoadX509KeyPair(viper.GetString("RABBITMQ_CERT_PATH"), viper.GetString("RABBITMQ_KEY_PATH"))
				if err != nil {
					panic(err)
				} else {
					cfg.Certificates = append(cfg.Certificates, cert)
				}

				connection, err := amqp091.DialTLS(viper.GetString("RABBITMQ_CONNECTION_STRING"), cfg)
				if err != nil {
					fmt.Println(err)
					<-time.After(time.Second)
					continue
				}
				return connection, nil
			} else {
				connection, err := amqp091.Dial(viper.GetString("RABBITMQ_CONNECTION_STRING"))
				if err != nil {
					fmt.Println(err)
					<-time.After(time.Second)
					continue
				}
				return connection, nil
			}

		}
	}
}
