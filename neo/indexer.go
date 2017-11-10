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

// Indexer neo blockchain indexer implement
type Indexer struct {
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

// NewIndexer create new neo blockchain indexer
func NewIndexer(cnf *config.Config, producer mq.Producer) (*Indexer, error) {

	db, err := leveldb.OpenFile(cnf.GetString("indexer.localdb", "./"), nil)

	if err != nil {
		return nil, err
	}

	ok, err := db.Has(localdbkey, nil)

	if err != nil {
		return nil, err
	}

	indexer := &Indexer{
		rpc:          NewClient(cnf.GetString("indexer.rpc", "http://localhost:10332")),
		producer:     producer,
		localdb:      db,
		pollinterval: time.Second * time.Duration(cnf.GetInt64("indexer.poll.interval", 5)),
		topic:        cnf.GetString("indexer.topic", "xxxx"),
	}

	indexer.cond = sync.NewCond(&indexer.mutex)

	if !ok {
		err = indexer.setCursor(0)

		if err != nil {
			db.Close()
			return nil, err
		}
	}

	indexer.ticker = time.NewTicker(indexer.pollinterval)

	go func() {
		for _ = range indexer.ticker.C {
			indexer.cond.Signal()
		}
	}()

	return indexer, nil
}

// Run run current neo blockchain tx indexer instance
func (indexer *Indexer) Run() {
	defer indexer.mutex.Unlock()

	indexer.mutex.Lock()

	indexer.stop = false

	for !indexer.stop {

		indexer.mutex.Unlock()

		// first update best block number
		blocks, err := indexer.rpc.GetBlockCount()

		if err != nil {
			logger.ErrorF("get block count err, %s", err)
			indexer.mutex.Lock()
			indexer.cond.Wait()
			continue
		}

		indexer.mutex.Lock()

		for indexer.getCursor() < uint64(blocks) && !indexer.stop {

			indexer.mutex.Unlock()

			if err := indexer.syncBlockOnce(); err != nil {
				indexer.mutex.Lock()
				indexer.cond.Wait()
				continue
			}

			indexer.mutex.Lock()
		}

	}
}

// Stop .
func (indexer *Indexer) Stop() {
	go func() {
		indexer.mutex.Lock()
		defer indexer.mutex.Unlock()

		indexer.stop = true

		indexer.cond.Signal()

	}()
}

func (indexer *Indexer) syncBlockOnce() error {
	cursor := indexer.getCursor()

	logger.DebugF("sync block(%d) ...", cursor)

	block, err := indexer.rpc.GetBlockByIndex(int64(cursor))

	if err != nil {
		logger.ErrorF("sync block(%d) failed !!!!, %s", cursor, err)
		return err
	}

	logger.InfoF("sync block(%d) success !!!! ", cursor)

	logger.DebugF("queue block(%s) ...", block.Hash)

	err = indexer.producer.Produce(indexer.topic, []byte(block.Hash), block)

	if err != nil {
		logger.ErrorF("queue block(%d,%s) failed !!!!, %s", cursor, block.Hash, err)
		return err
	}

	logger.InfoF("sync block(%d,%s) success !!!! ", cursor, block.Hash)

	if err := indexer.setCursor(cursor + 1); err != nil {
		logger.ErrorF("save cursor err,%s", err)

		return err
	}

	return nil
}

func (indexer *Indexer) getCursor() uint64 {
	buff, err := indexer.localdb.Get(localdbkey, nil)

	if err != nil {
		logger.ErrorF("get indexer local cursor error :%s", err)
		return 0
	}

	if buff == nil {
		logger.ErrorF("get indexer local cursor error : cursor not exists")
		return 0
	}

	return binary.BigEndian.Uint64(buff)
}

func (indexer *Indexer) setCursor(cursor uint64) error {
	buff := make([]byte, 8)
	binary.BigEndian.PutUint64(buff, cursor)

	return indexer.localdb.Put(localdbkey, buff, nil)
}

// Close .
func (indexer *Indexer) Close() {
	indexer.localdb.Close()
	indexer.ticker.Stop()
}
