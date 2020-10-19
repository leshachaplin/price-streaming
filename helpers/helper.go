package helpers

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/leshachaplin/price-streaming/protocol"
	"math/rand"
	"strconv"
	"sync"

	//kafka2 "github.com/leshachaplin/price-streaming/kafka"
	"github.com/go-redis/redis/v7"
	"time"
)

type Price struct {
	Id       string
	Bid      float64
	Ask      float64
	Date     time.Time
	Symbol   string
	Currency string
}

func (p *Price) GetPrice(short bool) float64 {
	if short {
		return p.Ask
	}
	return p.Bid
}

func (p *Price) UnmarshalBinary(data []byte) (*Price, error) {
	price := &protocol.Price{}
	err := proto.Unmarshal(data, price)
	if err != nil {
		return nil, err
	}

	return &Price{
		Id:       price.PriceId,
		Bid:      price.Bid,
		Ask:      price.Ack,
		Date:     time.Unix(price.Date, 0),
		Symbol:   price.Symbol,
		Currency: price.Currency,
	}, nil
}
func (p *Price) MarshalBinary() ([]byte, error) {

	message := &protocol.Price{
		PriceId:  p.Id,
		Bid:      p.Bid,
		Ack:      p.Ask,
		Date:     p.Date.Unix(),
		Symbol:   p.Symbol,
		Currency: p.Currency,
	}
	return proto.Marshal(message)
}

type RedisSender struct {
	c      redis.UniversalClient
	lastID string
	mu     sync.RWMutex
}

func NewRedisSender(ctx context.Context, client redis.UniversalClient,
	askIncrement float64, bidIncrement float64,
	prices *Price, seconds int) (*RedisSender, error) {
	snd := &RedisSender{
		c:      client,
		lastID: strconv.Itoa(int(time.Now().UTC().Unix())),
	}
	var err error
	go func(ctx context.Context, e error, sec int, pr *Price, askIncrement, bidIncrement float64) {
		err = snd.SendMsgToRedis(ctx, askIncrement, bidIncrement, prices, seconds)
	}(ctx, err, seconds, prices, askIncrement, bidIncrement)

	return snd, err
}

func (r *RedisSender) SendMsgToRedis(ctx context.Context, askIncrement float64,
	bidIncrement float64, prices *Price, seconds int) error {
	t := time.NewTicker(time.Second * time.Duration(seconds))
	for {
		select {
		case <-ctx.Done():
			{
				return nil
			}
		case <-t.C:
			{
				prices.Ask += askIncrement + float64(rand.Intn(100))/1000
				prices.Bid += bidIncrement + float64(rand.Intn(100))/1000
				prices.Id = fmt.Sprintf("%d", time.Now().UTC().UnixNano())

				msg1, err := json.Marshal(prices)
				if err != nil {
					return err
				}

				fmt.Println(string(msg1))
				_, err = r.c.XAdd(&redis.XAddArgs{
					Stream: prices.Symbol,
					ID:     fmt.Sprintf("%d", time.Now().UTC().UnixNano()),
					Values: map[string]interface{}{
						prices.Id: prices,
					},
				}).Result()
				if err != nil {
					return err
				}
			}
		}
	}
}
