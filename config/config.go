package config

// if using go modules

import (
	"fmt"
	"github.com/caarlos0/env/v6"
	"github.com/leshachaplin/config"
	log "github.com/sirupsen/logrus"
)

type Config struct {
	Port        int    `env:"Port" envDefault:"1323"`
	GrpcPort    string `env:"GrpcPort" envDefault:"0.0.0.0:50051"`
	KafkaSecret string `env:"KafkaConnectionSecret"`
	KafkaConfig *config.Kafka
}

func NewConfig() (*Config, error) {
	cfg := Config{}
	if err := env.Parse(&cfg); err != nil {
		fmt.Printf("%+v\n", err)
	}

	awsConf, err := config.NewForAws("us-west-2")
	if err != nil {
		log.Errorf("Can't connect to aws", err)
		return nil, err
	}

	cfg.KafkaConfig, err = awsConf.GetKafka(cfg.KafkaSecret)
	if err != nil {
		log.Errorf("Can't get kafka config from aws", err)
		return nil, err
	}

	return &cfg, nil
}
