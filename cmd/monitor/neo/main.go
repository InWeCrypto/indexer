package main

import (
	"flag"

	"github.com/dynamicgo/config"
	"github.com/goany/slf4go"
	"github.com/inwecrypto/indexer/mq"
	"github.com/inwecrypto/indexer/neo"
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

	producer, err := mq.NewAliyunProducer(neocnf)

	if err != nil {
		logger.ErrorF("create mq producer err , %s", err)
		return
	}

	indexer, err := neo.NewMonitor(neocnf, producer)

	if err != nil {
		logger.ErrorF("create neo indexer err , %s", err)
		return
	}

	indexer.Run()

}
