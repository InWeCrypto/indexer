package main

import (
	"flag"

	"github.com/dynamicgo/aliyunlog"
	"github.com/dynamicgo/config"
	"github.com/dynamicgo/slf4go"
	"github.com/inwecrypto/neo-indexer/neo"
)

var logger = slf4go.Get("neo-indexer")
var configpath = flag.String("conf", "./neo.json", "neo indexer config file path")

func main() {

	flag.Parse()

	neocnf, err := config.NewFromFile(*configpath)

	if err != nil {
		logger.ErrorF("load neo config err , %s", err)
		return
	}

	factory, err := aliyunlog.NewAliyunBackend(neocnf)

	neo.OpenLogger()

	if err != nil {
		logger.ErrorF("create aliyun log backend err , %s", err)
		return
	}

	slf4go.Backend(factory)

	etl, err := neo.NewETL(neocnf)

	if err != nil {
		logger.ErrorF("create neo etl err , %s", err)
		return
	}

	monitor, err := neo.NewMonitor(neocnf, etl)

	if err != nil {
		logger.ErrorF("create neo monitor err , %s", err)
		return
	}

	monitor.Run()

}
