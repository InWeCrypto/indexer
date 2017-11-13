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

	consumer, err := mq.NewAliyunConsumer(neocnf)

	if err != nil {
		logger.ErrorF("create mq consumer err , %s", err)
		return
	}

	indexer, err := neo.NewETL(neocnf, consumer)

	if err != nil {
		logger.ErrorF("create neo dbwriter err , %s", err)
		return
	}

	indexer.Run()

}
