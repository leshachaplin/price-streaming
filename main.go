package main

import (
	"context"
	"github.com/leshachaplin/price-streaming/config"
	"github.com/leshachaplin/price-streaming/helpers"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"time"
)

func main() {
	s := make(chan os.Signal)
	done, cnsl := context.WithCancel(context.Background())

	cfg, err := config.NewConfig()
	if err != nil {
		log.Error(err)
	}

	var prices = []*helpers.Price{
		{
			Bid:      1.0,
			Ask:      1.0,
			Date:     time.Now().UTC(),
			Symbol:   "EURUSD",
			Currency: "USD",
		},

		{
			Bid:      1.0,
			Ask:      1.0,
			Date:     time.Now().UTC(),
			Symbol:   "EURCZK",
			Currency: "EUR",
		},

		{
			Bid:      1.0,
			Ask:      1.0,
			Date:     time.Now().UTC(),
			Symbol:   "BELUSD",
			Currency: "BEL",
		},

		{
			Bid:      1.0,
			Ask:      1.0,
			Date:     time.Now().UTC(),
			Symbol:   "USDUAH",
			Currency: "UAH",
		},
	}

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt)

	_, err = helpers.NewRedisSender(done, cfg.RedisClient,0.001, 0.003, prices, 15)
	if err != nil {
		log.Fatal(err)
	}

	<-s
	close(s)
	cnsl()

	for {
		select {
		case <-c:
			cnsl()
			if err := cfg.RedisClient.Close(); err != nil {
				log.Errorf("database not closed %s", err)
			}
			log.Info("Cancel is successful")
			close(c)
			return
		}
	}
}