package helpers

import (
	"context"
	"encoding/json"
	kafka2 "github.com/leshachaplin/price-streaming/kafka"
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

func SendMsgToKafka(ctx context.Context, askIncrement float64,
	bidIncrement float64, conn *kafka2.Client, price *Price, seconds int) error {
	t := time.NewTicker(time.Second * time.Duration(seconds))
	for {
		select {
		case <-ctx.Done():
			{
				return nil
			}
		case <-t.C:
			{
				price.Ask += askIncrement
				price.Bid += bidIncrement
				msg, err := json.Marshal(price)
				if err != nil {
					return err
				}

				err = conn.WriteMessage(msg)
				if err != nil {
					return err
				}
				return nil
			}
		}
	}
}
