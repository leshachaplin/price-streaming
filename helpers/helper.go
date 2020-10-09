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

func SendMsgToKafka(ctx context.Context, conn *kafka2.Client, price *Price) error {

	//symbols := make([]Price, 0, 100000)
	//
	//for i := 0; i < 100000; i++ {
	//	symbol := Price{
	//		Bid:      0,
	//		Ask:      0,
	//		Date:     time.Now(),
	//		Symbol:   "a" + strconv.Itoa(i),
	//		Currency: "b" + strconv.Itoa(i),
	//	}
	//	symbols = append(symbols, symbol)
	//}
	//
	//s := time.Now()
	//for _, v := range symbols {
	//
	//	go func(v Price) {
	//		for i := 1; i <= 1000; i++ {
	//			v.Ask++
	//			v.Bid++
	//			msg, err := json.Marshal(v)
	//			if err != nil {
	//				log.Error(err)
	//			}
	//
	//			err = conn.WriteMessage(msg)
	//			if err != nil {
	//				log.Error(err)
	//			}
	//		}
	//	}(v)
	//}
	//e := time.Since(s)
	//log.Info(e.Milliseconds())
	//log.Info("---------------------------------------------------------")
	//time.Sleep(time.Second * 2)
	for {
		select {
		case <-ctx.Done():
			{
				return nil
			}
		default:
			{
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
