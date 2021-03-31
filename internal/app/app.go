package app

import (
	"context"
	"route/internal/broker"
	"route/internal/cluster"
	"route/internal/common"
	"route/internal/config"
	"route/internal/log"
	"route/internal/service"
	"sync"
)

type App interface {
	common.Runnable
}

type app struct {
	broker  broker.Broker
	swarm   cluster.Swarm
	service service.Service
}

func NewApp(logFactoryer log.LogFactoryer, broker broker.Broker, swarm cluster.Swarm, service service.Service, config *config.Config) App {
	log := logFactoryer.CreateLogger("app")
	log.Printf("运行配置: %+v", config)

	return &app{
		broker:  broker,
		swarm:   swarm,
		service: service,
	}
}

func (a *app) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	wg.Add(1)
	go a.broker.Run(ctx, wg)

	wg.Add(1)
	go a.swarm.Run(ctx, wg)

	wg.Add(1)
	go a.service.Run(ctx, wg)
}
