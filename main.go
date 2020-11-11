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

	//for i := 0; i < 5; i++ {
	//	EURUSD := &helpers.Price{
	//
	//		Bid:      1.0,
	//		Ask:      1.0,
	//		Date:     time.Now().UTC(),
	//		Symbol:   "EURUSD" + strconv.Itoa(i),
	//		Currency: "USD",
	//	}
	//
	//	_, err = helpers.NewRedisSender(done, cfg.RedisClient, 4, 3.5, EURUSD, 10)
	//	if err != nil {
	//		log.Fatal(err)
	//	}
	//}

	EURUSD := &helpers.Price{

		Bid:      1.0,
		Ask:      1.0,
		Date:     time.Now().UTC(),
		Symbol:   "EURUSD",
		Currency: "USD",
	}

	_, err = helpers.NewRedisSender(done, cfg.RedisClient, 2, 2.5, EURUSD, 10)
	if err != nil {
		log.Fatal(err)
	}

	EURUSD0 := &helpers.Price{

		Bid:      1.0,
		Ask:      1.0,
		Date:     time.Now().UTC(),
		Symbol:   "EURUSD0",
		Currency: "USD",
	}

	_, err = helpers.NewRedisSender(done, cfg.RedisClient, 19, 17.5, EURUSD0, 10)
	if err != nil {
		log.Fatal(err)
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
