package main

import (
	"context"
	"fmt"
	cli "github.com/jawher/mow.cli"
	"github.com/wiqun/route/internal/app"
	"github.com/wiqun/route/internal/broker"
	"github.com/wiqun/route/internal/cluster"
	"github.com/wiqun/route/internal/config"
	"github.com/wiqun/route/internal/generate"
	"github.com/wiqun/route/internal/log"
	"github.com/wiqun/route/internal/message"
	"github.com/wiqun/route/internal/service"
	"go.uber.org/dig"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func provideAll(container *dig.Container, configPath string, keyAndValue []string) {

	var errs = []error{

		container.Provide(app.NewApp),
		container.Provide(broker.NewBroker),
		container.Provide(cluster.NewSwarm),
		container.Provide(func() (*config.Config, *config.ClusterConfig, *config.BrokerConfig, *config.ChanConfig, *config.ServiceConfig) {
			return config.NewConfig(configPath, keyAndValue)
		}),
		container.Provide(generate.NewLocalIdGenerator),

		container.Provide(log.NewLogFactoryer),
		container.Provide(message.NewLocalMsgNotifierChanReceiver),
		container.Provide(message.NewRemoteMsgNotifierChanReceiver),

		container.Provide(service.NewService),
	}

	for _, err := range errs {
		if err != nil {
			panic(err)
		}
	}

}

func main() {

	app := cli.App("route", "Runs the Route broker.")
	confPath := app.StringOpt("c config", "route.yaml", "指定配置文件的路径(yaml格式)")
	keyAndValue := app.StringsOpt("e environment", nil,
		"指定某个配置(忽视大小写) 格式: -e route_broker_listenAddr=:4002 ")
	app.Action = func() { run(*confPath, *keyAndValue) }

	err := app.Run(os.Args)
	if err != nil {
		panic(err)
	}

}

func run(configPath string, keyAndValue []string) {
	container := dig.New()

	provideAll(container, configPath, keyAndValue)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := &sync.WaitGroup{}

	err := container.Invoke(func(app app.App) {
		wg.Add(1)
		go app.Run(ctx, wg)
	})

	if err != nil {
		panic(err)
	}

	fmt.Println("主程序运行中....")

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	var sign os.Signal
	select {
	case sign = <-signalChan: //wait exit signal from Docker
		fmt.Printf("收到退出信号: '%s'\n", sign)
		cancel() //通知所有下游ctx绑定的协程退出
		break
	}

	fmt.Println("Main程序等待退出...")
	wg.Wait()
	fmt.Println("Main程序退出完成")
}
