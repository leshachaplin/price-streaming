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

	EURUSD := &helpers.Price{

		Bid:      1.0,
		Ask:      1.0,
		Date:     time.Now().UTC(),
		Symbol:   "EURUSD",
		Currency: "USD",
	}

	EURCZK := &helpers.Price{
		Bid:      1.0,
		Ask:      1.0,
		Date:     time.Now().UTC(),
		Symbol:   "EURCZK",
		Currency: "EUR",
	}

	BELUSD := &helpers.Price{
		Bid:      1.0,
		Ask:      1.0,
		Date:     time.Now().UTC(),
		Symbol:   "BELUSD",
		Currency: "BEL",
	}

	USDUAH := &helpers.Price{
		Bid:      1.0,
		Ask:      1.0,
		Date:     time.Now().UTC(),
		Symbol:   "USDUAH",
		Currency: "UAH",
	}

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt)

	_, err = helpers.NewRedisSender(done, cfg.RedisClient, 0.1, 0.3, EURUSD, 10)
	if err != nil {
		log.Fatal(err)
	}

	_, err = helpers.NewRedisSender(done, cfg.RedisClient, 0.2, 0.5, EURCZK, 10)
	if err != nil {
		log.Fatal(err)
	}

	_, err = helpers.NewRedisSender(done, cfg.RedisClient, 0.6, 0.7, BELUSD, 10)
	if err != nil {
		log.Fatal(err)
	}

	_, err = helpers.NewRedisSender(done, cfg.RedisClient, 0.2, 0.3, USDUAH, 10)
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
