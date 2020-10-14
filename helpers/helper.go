package helpers

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/leshachaplin/price-streaming/protocol"
	"strconv"
	"sync"

	//kafka2 "github.com/leshachaplin/price-streaming/kafka"
	"github.com/go-redis/redis/v7"
	"time"
)

type Price struct {
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
		Bid:      price.Bid,
		Ask:      price.Ack,
		Date:     time.Unix(price.Date, 0),
		Symbol:   price.Symbol,
		Currency: price.Currency,
	}, nil
}
func (p *Price) MarshalBinary() ([]byte, error) {

	message := &protocol.Price{
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
	prices []*Price, seconds int) (*RedisSender, error) {
	snd := &RedisSender{
		c:      client,
		lastID: strconv.Itoa(int(time.Now().UTC().Unix())),
	}

	err := snd.SendMsgToRedis(ctx, askIncrement, bidIncrement, prices, seconds)

	return snd, err
}

func (r *RedisSender) SendMsgToRedis(ctx context.Context, askIncrement float64,
	bidIncrement float64, prices []*Price, seconds int) error {
	t := time.NewTicker(time.Second * time.Duration(seconds))
	for {
		select {
		case <-ctx.Done():
			{
				return nil
			}
		case <-t.C:
			{
				for i := 0; i < len(prices); i++ {
					prices[i].Ask += askIncrement + float64(i)/1000
					prices[i].Bid += bidIncrement + float64(i)/1000
					msg, err := prices[i].MarshalBinary()
					if err != nil {
						return err
					}

					fmt.Println(string(msg))

					_, err = r.c.XAdd(&redis.XAddArgs{
						Stream: "prices",
						ID:     strconv.Itoa(int(time.Now().UTC().Unix())),
						Values: map[string]interface{}{
							prices[i].Symbol: prices[i],
						},
					}).Result()
					if err != nil {
						return err
					}
				}
			}
		}
	}
}
