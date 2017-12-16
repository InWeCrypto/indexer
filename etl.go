package indexer

import (
	"fmt"
	"strconv"
	"time"

	"github.com/dynamicgo/config"
	"github.com/dynamicgo/slf4go"
	"github.com/go-xorm/xorm"
	"github.com/inwecrypto/gomq"
	gomqkafka "github.com/inwecrypto/gomq-kafka"
	"github.com/inwecrypto/neodb"
	"github.com/inwecrypto/neogo"
)

// ETL .
type ETL struct {
	slf4go.Logger
	conf   *config.Config
	engine *xorm.Engine
	mq     gomq.Producer // mq producer
	topic  string
	client *neogo.Client
}

func newETL(conf *config.Config) (*ETL, error) {
	username := conf.GetString("indexer.ethdb.username", "xxx")
	password := conf.GetString("indexer.ethdb.password", "xxx")
	port := conf.GetString("indexer.ethdb.port", "6543")
	host := conf.GetString("indexer.ethdb.host", "localhost")
	scheme := conf.GetString("indexer.ethdb.schema", "postgres")

	engine, err := xorm.NewEngine(
		"postgres",
		fmt.Sprintf(
			"user=%v password=%v host=%v dbname=%v port=%v sslmode=disable",
			username, password, host, scheme, port,
		),
	)

	if err != nil {
		return nil, err
	}

	mq, err := gomqkafka.NewAliyunProducer(conf)

	if err != nil {
		return nil, err
	}

	return &ETL{
		Logger: slf4go.Get("eth-indexer-etl"),
		conf:   conf,
		engine: engine,
		mq:     mq,
		topic:  conf.GetString("aliyun.kafka.topic", "xxxxx"),
		client: neogo.NewClient(conf.GetString("indexer.neo", "http://localhost:8545")),
	}, nil
}

// Handle handle eth block
func (etl *ETL) Handle(block *neogo.Block) error {

	etl.DebugF("block %d tx %d", block.Index, len(block.Transactions))

	for _, tx := range block.Transactions {
		etl.DebugF("tx %s vin %d vout %d claims %d", tx.ID, len(tx.Vin), len(tx.Vout), len(tx.Claims))
	}

	if err := etl.insertUTXOs(block); err != nil {
		return err
	}

	if err := etl.spentUTXOs(block); err != nil {
		return err
	}

	if err := etl.claimUTXOs(block); err != nil {
		return err
	}

	if err := etl.insertTx(block); err != nil {
		return err
	}

	if err := etl.insertBlock(block); err != nil {
		return err
	}

	for _, tx := range block.Transactions {
		if err := etl.mq.Produce(etl.topic, []byte(tx.ID), tx.ID); err != nil {
			etl.ErrorF("mq insert tx %s err :%s", tx.ID, err)
			return err
		}

		etl.DebugF("tx %s event send", tx.ID)
	}

	return nil
}

func (etl *ETL) insertBlock(block *neogo.Block) (err error) {
	sysfee := float64(0)
	netfee := float64(0)

	for _, tx := range block.Transactions {
		fee, err := strconv.ParseFloat(tx.SysFee, 8)

		if err != nil {
			etl.ErrorF("parse tx(%s) sysfee(%s) err, %s", tx.ID, tx.SysFee, err)
			continue
		}

		sysfee += fee

		fee, err = strconv.ParseFloat(tx.NetFee, 8)

		if err != nil {
			etl.ErrorF("parse tx(%s) netfee(%s) err, %s", tx.ID, tx.NetFee, err)
			continue
		}

		netfee += fee
	}

	_, err = etl.engine.Insert(&neodb.Block{
		Block:      block.Index,
		SysFee:     sysfee,
		NetFee:     netfee,
		CreateTime: time.Unix(block.Time, 0),
	})

	return err
}

func sysFeeToString(f float64) string {
	data := fmt.Sprintf("%.1f", f)

	data = data[0 : len(data)-2]

	return data
}

func netFeeToString(f float64) string {
	data := fmt.Sprintf("%.9f", f)

	data = data[0 : len(data)-1]

	return data
}

func (etl *ETL) insertTx(block *neogo.Block) (err error) {
	utxos := make([]*neodb.Tx, 0)

	for _, tx := range block.Transactions {

		from := ""

		if len(tx.Vin) > 0 {
			rawtx, err := etl.client.GetRawTransaction(tx.Vin[0].TransactionID)

			if err != nil {
				return err
			}

			from = rawtx.Vout[tx.Vin[0].Vout].Address
		}

		for _, vout := range tx.Vout {

			if len(tx.Claims) > 0 {
				from = vout.Address
			}

			utxos = append(utxos, &neodb.Tx{
				TX:         tx.ID,
				Block:      uint64(block.Index),
				From:       from,
				To:         vout.Address,
				Asset:      vout.Asset,
				Value:      vout.Value,
				CreateTime: time.Unix(block.Time, 0),
			})

			if len(utxos) >= 100 {
				if err := etl.batchInsertTx(utxos); err != nil {
					return err
				}

				for _, utxo := range utxos {
					etl.DebugF("create tx %s from %s to %s", utxo.TX, utxo.From, utxo.To)
				}

				utxos = make([]*neodb.Tx, 0)
			}
		}
	}

	if len(utxos) > 0 {
		if err := etl.batchInsertTx(utxos); err != nil {
			return err
		}

		for _, utxo := range utxos {
			etl.DebugF("create tx %s from %s to %s", utxo.TX, utxo.From, utxo.To)
		}
	}

	return nil
}

func (etl *ETL) batchInsertTx(rows []*neodb.Tx) (err error) {
	session := etl.engine.NewSession()

	session.Begin()

	defer func() {
		if err != nil {
			session.Rollback()
		} else {
			session.Commit()
		}
	}()

	_, err = etl.engine.Insert(&rows)

	return
}

func (etl *ETL) spentUTXOs(block *neogo.Block) (err error) {
	utxos := make([]*neodb.UTXO, 0)

	for _, tx := range block.Transactions {

		spentTime := time.Unix(block.Time, 0)

		for _, vin := range tx.Vin {
			utxos = append(utxos, &neodb.UTXO{
				TX:         vin.TransactionID,
				N:          vin.Vout,
				SpentTime:  &spentTime,
				SpentBlock: block.Index,
			})

			if len(utxos) >= 100 {
				if err := etl.updateUTXOs(utxos, "spent_block", "spent_time"); err != nil {
					return err
				}

				for _, utxo := range utxos {
					etl.DebugF("spent utxo %s %d", utxo.TX, utxo.N)
				}

				utxos = make([]*neodb.UTXO, 0)
			}
		}
	}

	if len(utxos) > 0 {
		if err := etl.updateUTXOs(utxos, "spent_block", "spent_time"); err != nil {
			return err
		}

		for _, utxo := range utxos {
			etl.DebugF("spent utxo %s %d", utxo.TX, utxo.N)
		}
	}

	return
}

func (etl *ETL) claimUTXOs(block *neogo.Block) (err error) {
	utxos := make([]*neodb.UTXO, 0)

	for _, tx := range block.Transactions {

		for _, claim := range tx.Claims {
			utxos = append(utxos, &neodb.UTXO{
				TX:      claim.TransactionID,
				N:       claim.Vout,
				Claimed: true,
			})

			if len(utxos) >= 100 {
				if err := etl.updateUTXOs(utxos, "claimed"); err != nil {
					return err
				}

				for _, utxo := range utxos {
					etl.DebugF("claim utxo %s %s", utxo.TX, utxo.N)
				}

				utxos = make([]*neodb.UTXO, 0)
			}
		}
	}

	if len(utxos) > 0 {
		if err := etl.updateUTXOs(utxos, "claimed"); err != nil {
			return err
		}

		for _, utxo := range utxos {
			etl.DebugF("claim utxo %s %s", utxo.TX, utxo.N)
		}
	}

	return
}

func (etl *ETL) updateUTXOs(utxos []*neodb.UTXO, cols ...string) (err error) {
	session := etl.engine.NewSession()

	session.Begin()

	defer func() {
		if err != nil {
			session.Rollback()
		} else {
			session.Commit()
		}
	}()

	for _, utxo := range utxos {
		_, err = session.Where("t_x = ? and n = ?", utxo.TX, utxo.N).Cols(cols...).Update(utxo)

		if err != nil {
			return
		}
	}

	return nil
}

func (etl *ETL) insertUTXOs(block *neogo.Block) error {

	etl.DebugF("start insert utxos")

	utxos := make([]*neodb.UTXO, 0)

	for _, tx := range block.Transactions {

		for _, vout := range tx.Vout {
			utxos = append(utxos, &neodb.UTXO{
				TX:          tx.ID,
				N:           vout.N,
				Address:     vout.Address,
				CreateBlock: block.Index,
				SpentBlock:  -1,
				Asset:       vout.Asset,
				Value:       vout.Value,
				CreateTime:  time.Unix(block.Time, 0),
				SpentTime:   nil,
				Claimed:     false,
			})

			if len(utxos) >= 100 {
				if err := etl.batchInsert(utxos); err != nil {
					return err
				}

				for _, utxo := range utxos {
					etl.DebugF("create utxo %s %d", utxo.TX, utxo.N)
				}

				utxos = make([]*neodb.UTXO, 0)
			}
		}
	}

	if len(utxos) > 0 {
		if err := etl.batchInsert(utxos); err != nil {
			return err
		}

		for _, utxo := range utxos {
			etl.DebugF("create utxo %s %d", utxo.TX, utxo.N)
		}
	}

	etl.DebugF("finish insert utxos")

	return nil
}

func (etl *ETL) batchInsert(rows []*neodb.UTXO) (err error) {
	session := etl.engine.NewSession()

	session.Begin()

	defer func() {
		if err != nil {
			session.Rollback()
		} else {
			session.Commit()
		}
	}()

	_, err = etl.engine.Insert(&rows)

	return
}
