package kafka

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"time"
)

type Client struct {
	priceReader      *kafka.Reader
	priceBatchReader *kafka.Conn
	priceWriter      *kafka.Conn
}

// Close kafka connection
func (k *Client) Close() error {
	err := k.priceWriter.Close()
	return err
}

// New kafka client
func New(topicPrice, batchPriceGroup, url, network string) (*Client, error) {

	//priceReader, err := kafka.DialLeader(context.Background(),
	//	network, url, topicPrice, 0)
	//if err != nil {
	//	return nil, err
	//}

	priceReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{url},
		Topic:       topicPrice,
		Partition:   0,
		Logger:      log.New(),
		ErrorLogger: log.New(),
	})

	batchPriceDialer := kafka.Dialer{
		ClientID: batchPriceGroup,
	}

	batchReadPrice, err := batchPriceDialer.DialLeader(context.Background(), network, url, topicPrice, 0)
	if err != nil {
		return nil, err
	}

	priceWriter, err := kafka.DialLeader(context.Background(), network, url, topicPrice, 0)

	return &Client{
		priceBatchReader: batchReadPrice,
		priceReader:      priceReader,
		priceWriter:      priceWriter,
	}, nil
}

// write message to kafka
func (k *Client) WriteMessage(msg []byte) error {
	_, err := k.priceWriter.WriteMessages(kafka.Message{
		Value: msg,
	})
	return err
}

//Batch read message from kafka
func (k *Client) ReadBatchMessage(batchMaxSize, maxBytePerMessage, timeout int) ([]byte, int64, error) {
	//log
	t := time.Now().Add(time.Second * time.Duration(timeout))
	err := k.priceBatchReader.SetReadDeadline(t)
	if err != nil {
		return nil, 0, err
	}
	batch := k.priceBatchReader.ReadBatch(1, batchMaxSize)

	buff := make([]byte, 0, batchMaxSize)
	buff = append(buff, byte('{'))
	buff = append(buff, byte('"'))
	buff = append(buff, []byte("prices")...)
	buff = append(buff, byte('"'))
	buff = append(buff, byte(':'))
	buff = append(buff, byte('['))

	for {
		b := make([]byte, maxBytePerMessage)
		n, err := batch.Read(b)
		if err != nil {
			break
		}
		trimB := b[:n]
		buff = append(buff, trimB...)
		buff = append(buff, byte(','))
	}
	offset := batch.Offset()
	err = batch.Close()

	buff[len(buff)-1] = byte(']')
	buff = append(buff, byte('}'))
	return buff, offset, err
}

func (k *Client) ReadMessage(ctx context.Context) {

	select {
	case <-ctx.Done():
		{
			return
		}
	default:
		{
			for {
				m, err := k.priceReader.ReadMessage(context.Background())
				if err != nil {
					return
				}
				fmt.Printf("message at topic %v: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
			}
		}


	}
}
