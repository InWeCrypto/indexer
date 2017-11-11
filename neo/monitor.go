package neo

import (
	"encoding/binary"
	"sync"
	"time"

	"github.com/dynamicgo/config"
	"github.com/inwecrypto/indexer/mq"
	"github.com/syndtr/goleveldb/leveldb"
)

var localdbkey = []byte("cursor")

// Monitor neo blockchain Monitor implement
type Monitor struct {
	rpc          *Client
	producer     mq.Producer
	localdb      *leveldb.DB
	pollinterval time.Duration
	topic        string
	stop         bool
	mutex        sync.Mutex
	cond         *sync.Cond
	ticker       *time.Ticker
}

// NewMonitor create new neo blockchain Monitor
func NewMonitor(cnf *config.Config, producer mq.Producer) (*Monitor, error) {

	db, err := leveldb.OpenFile(cnf.GetString("monitor.localdb", "./"), nil)

	if err != nil {
		return nil, err
	}

	ok, err := db.Has(localdbkey, nil)

	if err != nil {
		return nil, err
	}

	Monitor := &Monitor{
		rpc:          NewClient(cnf.GetString("monitor.rpc", "http://localhost:10332")),
		producer:     producer,
		localdb:      db,
		pollinterval: time.Second * time.Duration(cnf.GetInt64("Monitor.poll.interval", 5)),
		topic:        cnf.GetString("monitor.topic", "xxxx"),
	}

	Monitor.cond = sync.NewCond(&Monitor.mutex)

	if !ok {
		err = Monitor.setCursor(0)

		if err != nil {
			db.Close()
			return nil, err
		}
	}

	Monitor.ticker = time.NewTicker(Monitor.pollinterval)

	go func() {
		for _ = range Monitor.ticker.C {
			Monitor.cond.Signal()
		}
	}()

	return Monitor, nil
}

// Run run current neo blockchain tx Monitor instance
func (Monitor *Monitor) Run() {
	defer Monitor.mutex.Unlock()

	Monitor.mutex.Lock()

	Monitor.stop = false

	for !Monitor.stop {

		Monitor.mutex.Unlock()

		// first update best block number
		blocks, err := Monitor.rpc.GetBlockCount()

		if err != nil {
			logger.ErrorF("get block count err, %s", err)
			Monitor.mutex.Lock()
			Monitor.cond.Wait()
			continue
		}

		Monitor.mutex.Lock()

		for Monitor.getCursor() < uint64(blocks) && !Monitor.stop {

			Monitor.mutex.Unlock()

			if err := Monitor.syncBlockOnce(); err != nil {
				Monitor.mutex.Lock()
				Monitor.cond.Wait()
				continue
			}

			Monitor.mutex.Lock()
		}

	}
}

// Stop .
func (Monitor *Monitor) Stop() {
	go func() {
		Monitor.mutex.Lock()
		defer Monitor.mutex.Unlock()

		Monitor.stop = true

		Monitor.cond.Signal()

	}()
}

func (Monitor *Monitor) syncBlockOnce() error {
	cursor := Monitor.getCursor()

	logger.DebugF("sync block(%d) ...", cursor)

	block, err := Monitor.rpc.GetBlockByIndex(int64(cursor))

	if err != nil {
		logger.ErrorF("sync block(%d) failed !!!!, %s", cursor, err)
		return err
	}

	logger.InfoF("sync block(%d) success !!!! ", cursor)

	logger.DebugF("queue block(%s) ...", block.Hash)

	err = Monitor.producer.Produce(Monitor.topic, []byte(block.Hash), block)

	if err != nil {
		logger.ErrorF("queue block(%d,%s) failed !!!!, %s", cursor, block.Hash, err)
		return err
	}

	logger.InfoF("sync block(%d,%s) success !!!! ", cursor, block.Hash)

	if err := Monitor.setCursor(cursor + 1); err != nil {
		logger.ErrorF("save cursor err,%s", err)

		return err
	}

	return nil
}

func (Monitor *Monitor) getCursor() uint64 {
	buff, err := Monitor.localdb.Get(localdbkey, nil)

	if err != nil {
		logger.ErrorF("get Monitor local cursor error :%s", err)
		return 0
	}

	if buff == nil {
		logger.ErrorF("get Monitor local cursor error : cursor not exists")
		return 0
	}

	return binary.BigEndian.Uint64(buff)
}

func (Monitor *Monitor) setCursor(cursor uint64) error {
	buff := make([]byte, 8)
	binary.BigEndian.PutUint64(buff, cursor)

	return Monitor.localdb.Put(localdbkey, buff, nil)
}

// Close .
func (Monitor *Monitor) Close() {
	Monitor.localdb.Close()
	Monitor.ticker.Stop()
}
