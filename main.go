package price_streaming

import (
	"context"
	"fmt"
	"github.com/leshachaplin/price-streaming/config"
	"github.com/leshachaplin/price-streaming/helpers"
	"github.com/leshachaplin/price-streaming/kafka"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
)

func PriceStreamer(price *helpers.Price) {
	s := make(chan os.Signal)
	done, cnsl := context.WithCancel(context.Background())

	cfg, err := config.NewConfig()
	if err != nil {
		log.Error(err)
	}

	kafkaUrl := fmt.Sprint(cfg.KafkaConfig.Host + ":" + strconv.Itoa(cfg.KafkaConfig.Port))
	kafkaClient, err := kafka.New(cfg.KafkaConfig.Topic, cfg.KafkaConfig.Group, kafkaUrl, "tcp")
	if err != nil {
		log.Fatal(err)
	}

	defer kafkaClient.Close()


	err = helpers.SendMsgToKafka(done, kafkaClient, price)
	if err != nil {
		log.Fatal(err)
	}

	<-s
	close(s)
	cnsl()
}