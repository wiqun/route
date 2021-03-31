package app

import (
	"context"
	"github.com/wiqun/route/internal/broker"
	"github.com/wiqun/route/internal/cluster"
	"github.com/wiqun/route/internal/common"
	"github.com/wiqun/route/internal/config"
	"github.com/wiqun/route/internal/log"
	"github.com/wiqun/route/internal/service"
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
