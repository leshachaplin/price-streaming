package helpers

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"sync"

	//kafka2 "github.com/leshachaplin/price-streaming/kafka"
	"github.com/go-redis/redis/v7"
	"time"
)

type Price struct {
	ID       string
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
	price := &Price{}
	err := json.Unmarshal(data, price)
	if err != nil {
		return nil, err
	}

	return price, nil
}
func (p *Price) MarshalBinary() ([]byte, error) {
	return json.Marshal(p)
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
				prices.ID = fmt.Sprintf("%d", time.Now().UTC().UnixNano())

				msg1, err := json.Marshal(prices)
				if err != nil {
					return err
				}

				msg, err := prices.MarshalBinary()
				if err != nil {
					return err
				}

				fmt.Println(string(msg1))
				_, err = r.c.XAdd(&redis.XAddArgs{
					Stream:       prices.Symbol,
					MaxLenApprox: 5000,
					ID:           prices.ID,
					Values: map[string]interface{}{
						prices.ID: msg,
					},
				}).Result()
				if err != nil {
					return err
				}
			}
		}
	}
}
