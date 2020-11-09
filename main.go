package main

import (
	"context"
	"github.com/leshachaplin/price-streaming/config"
	"github.com/leshachaplin/price-streaming/helpers"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"strconv"
	"time"
)

func main() {

	s := make(chan os.Signal)
	done, cnsl := context.WithCancel(context.Background())

	cfg, err := config.NewConfig()
	if err != nil {
		log.Error(err)
	}

	for i := 0; i < 5000; i++ {
		EURUSD := &helpers.Price{

			Bid:      1.0,
			Ask:      1.0,
			Date:     time.Now().UTC(),
			Symbol:   "EURUSD" + strconv.Itoa(i),
			Currency: "USD",
		}

		EURCZK := &helpers.Price{
			Bid:      1.0,
			Ask:      1.0,
			Date:     time.Now().UTC(),
			Symbol:   "EURCZK" + strconv.Itoa(i),
			Currency: "EUR",
		}

		BELUSD := &helpers.Price{
			Bid:      1.0,
			Ask:      1.0,
			Date:     time.Now().UTC(),
			Symbol:   "BELUSD" + strconv.Itoa(i),
			Currency: "BEL",
		}

		USDUAH := &helpers.Price{
			Bid:      1.0,
			Ask:      1.0,
			Date:     time.Now().UTC(),
			Symbol:   "USDUAH" + strconv.Itoa(i),
			Currency: "UAH",
		}

		_, err = helpers.NewRedisSender(done, cfg.RedisClient, 4, 3.5, EURUSD, 10)
		if err != nil {
			log.Fatal(err)
		}

		_, err = helpers.NewRedisSender(done, cfg.RedisClient, 2, 2.5, EURCZK, 10)
		if err != nil {
			log.Fatal(err)
		}

		_, err = helpers.NewRedisSender(done, cfg.RedisClient, 2.6, 2.7, BELUSD, 10)
		if err != nil {
			log.Fatal(err)
		}

		_, err = helpers.NewRedisSender(done, cfg.RedisClient, 2.2, 2.3, USDUAH, 10)
		if err != nil {
			log.Fatal(err)
		}
	}

	log.Info("cycle for END")

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt)

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
