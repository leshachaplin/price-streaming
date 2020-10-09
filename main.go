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

func PriceStreamer(ctx context.Context, cnsl context.CancelFunc,
	s chan os.Signal, price *helpers.Price, second int, askIncrement, bidIncrement float64) {
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


	err = helpers.SendMsgToKafka(ctx, askIncrement, bidIncrement, kafkaClient, price, second)
	if err != nil {
		log.Fatal(err)
	}

	<-s
	close(s)
	cnsl()
}