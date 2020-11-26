package config

// if using go modules

import (
	"fmt"
	"github.com/caarlos0/env/v6"
	"github.com/go-redis/redis/v7"
	"github.com/leshachaplin/config"
	log "github.com/sirupsen/logrus"
	"strconv"
)

type Config struct {
	Port        int    `env:"Port" envDefault:"1323"`
	GrpcPort    string `env:"GrpcPort" envDefault:"0.0.0.0:50051"`
	RedisSecret string `env:"RedisConnectionSecret"`
	RedisClient redis.UniversalClient
}

func NewConfig() (*Config, error) {
	cfg := Config{}
	if err := env.Parse(&cfg); err != nil {
		fmt.Printf("%+v\n", err)
	}

	redisConfig, err := config.GetRedis(cfg.RedisSecret)
	if err != nil {
		log.Errorf("Can't get kafka config from aws", err)
		return nil, err
	}

	cfg.RedisClient = redis.NewClient(&redis.Options{
		Addr: redisConfig.Host + ":" + strconv.Itoa(redisConfig.Port),
		DB:   0,
	})

	return &cfg, nil
}
