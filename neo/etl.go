package neo

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/dynamicgo/config"
	"github.com/inwecrypto/neogo"
	"github.com/lib/pq"
)

// ETL neo indexer builtin etl service
type ETL struct {
	db      *sql.DB       // indexer database
	rpc     *neogo.Client // neo client
	tbutxo  string        // utxo table
	tbtx    string        //tx table
	tbblock string        // block table
}

// NewETL create new etl
func NewETL(cnf *config.Config) (*ETL, error) {
	db, err := openDB(cnf)

	if err != nil {
		return nil, err
	}

	etl := &ETL{
		db:      db,
		tbutxo:  cnf.GetString("dbwriter.tables.utxo", "NEO_UTXO"),
		tbtx:    cnf.GetString("dbwriter.tables.tx", "NEO_TX"),
		tbblock: cnf.GetString("dbwriter.tables.block", "NEO_BLOCK"),
	}

	return etl, nil
}

// Produce implement mq producer interface
func (etl *ETL) Produce(sysfee float64, block *neogo.Block) (float64, error) {
	return etl.processBlock(sysfee, block)
}

func (etl *ETL) processBlock(sysfee float64, block *neogo.Block) (inc float64, err error) {
	inc = sysfee
	dbTx, err := etl.db.Begin()

	if err != nil {
		return sysfee, err
	}

	defer func() {
		if err != nil {
			logger.Debug("rollback !!!!!!!!!!!!!!!!!!!!!!")
			err1 := dbTx.Rollback()
			logger.Debug("rollback success@@!!!!!!!!!!!!!!!!!!!!!!")
			if err1 != nil {
				logger.ErrorF("rollback error, %s", err1)
			}
		} else {
			err1 := dbTx.Commit()
			if err1 == nil {
				logger.DebugF("create indexer for block %d succeed", block.Index)
			}
		}
	}()

	if err = etl.bulkInsertUTXO(dbTx, block); err != nil {
		return
	}

	if err = etl.spendUTXO(dbTx, block); err != nil {
		return
	}

	if err = etl.bulkInsertTX(dbTx, block); err != nil {
		return
	}

	if inc, err = etl.bulkInsertBlocks(dbTx, block, sysfee); err != nil {
		return
	}

	return
}

func (etl *ETL) bulkInsertTX(dbTx *sql.Tx, block *neogo.Block) (err error) {
	var stmt *sql.Stmt
	stmt, err = dbTx.Prepare(pq.CopyIn(etl.tbtx, "blocks", "tx", "address", "type", "assert", "updateTime"))

	if err != nil {
		logger.ErrorF("tx bulk prepare error :%s", err)
		return err
	}

	defer func() {
		err1 := stmt.Close()

		if err1 != nil {
			logger.ErrorF("close stmt error, %s", err)
		}

	}()

	for _, tx := range block.Transactions {

		if len(tx.Vout) == 0 {
			logger.DebugF("tx %s %s without vout ", tx.ID, tx.Type)
			continue
		}

		for _, vout := range tx.Vout {

			logger.DebugF("tx %s vout %d ", tx.ID, vout.N)

			_, err = stmt.Exec(block.Index, tx.ID, vout.Address, tx.Type, vout.Asset, time.Unix(block.Time, 0).Format(time.RFC3339))

			if err != nil {
				logger.ErrorF("tx insert error :%s", err)
				return err
			}
		}
	}

	_, err = stmt.Exec()

	if err != nil {
		logger.ErrorF("tx bulk insert error :%s", err)
	}

	return
}

func (etl *ETL) bulkInsertBlocks(dbTx *sql.Tx, block *neogo.Block, sysfee float64) (inc float64, err error) {

	inc = sysfee

	var stmt *sql.Stmt

	stmt, err = dbTx.Prepare(fmt.Sprintf(`insert into %s("id", "sysfee", "netfee", "createTime") values($1,$2,$3,$4)`, etl.tbblock))

	if err != nil {
		logger.ErrorF("block bulk prepare error :%s", err)
		return 0, err
	}

	defer func() {
		err1 := stmt.Close()

		if err1 != nil {
			logger.ErrorF("close stmt error, %s", err)
		}

	}()

	netfee := float64(0)

	for _, tx := range block.Transactions {
		fee, err := strconv.ParseFloat(tx.SysFee, 8)

		if err != nil {
			logger.ErrorF("parse tx(%s) sysfee(%s) err, %s", tx.ID, tx.SysFee, err)
			continue
		}

		sysfee += fee

		fee, err = strconv.ParseFloat(tx.NetFee, 8)

		if err != nil {
			logger.ErrorF("parse tx(%s) netfee(%s) err, %s", tx.ID, tx.NetFee, err)
			continue
		}

		netfee += fee
	}

	_, err = stmt.Exec(block.Index, sysfee, netfee, time.Unix(block.Time, 0).Format(time.RFC3339))

	if err != nil {
		logger.ErrorF("block bulk insert error :%s", err)
	}

	return
}

func (etl *ETL) spendUTXO(dbTx *sql.Tx, block *neogo.Block) (err error) {
	var stmt *sql.Stmt

	sqlStr := fmt.Sprintf(`update %s set "spentTime"=$1 where "tx"=$2 and "n"=$3`, etl.tbutxo)

	stmt, err = dbTx.Prepare(sqlStr)

	if err != nil {
		logger.ErrorF("market utxo prepare error :%s", err)
		return err
	}

	defer func() {
		err1 := stmt.Close()

		if err1 != nil {
			logger.ErrorF("close stmt error, %s", err)
		}
	}()

	for _, tx := range block.Transactions {
		for _, vin := range tx.Vin {
			_, err := stmt.Exec(
				time.Unix(block.Time, 0).Format(time.RFC3339),
				vin.TransactionID,
				vin.Vout,
			)

			if err != nil {
				logger.ErrorF("market utxo exec error :%s\n\tvin :%v", err, vin)
				return err
			}
		}
	}

	return
}

func (etl *ETL) bulkInsertUTXO(dbTx *sql.Tx, block *neogo.Block) (err error) {
	var stmt *sql.Stmt
	stmt, err = dbTx.Prepare(pq.CopyIn(etl.tbutxo, "tx", "n", "address", "assert", "blocks", "value", "json", "createTime"))

	if err != nil {
		logger.ErrorF("utxo bulk prepare error :%s", err)
		return err
	}

	defer func() {
		err1 := stmt.Close()

		if err1 != nil {
			logger.ErrorF("close stmt error, %s", err)
		}
	}()

	for _, tx := range block.Transactions {
		for _, vout := range tx.Vout {

			utxo := &neogo.UTXO{
				TransactionID: tx.ID,
				Vout:          vout,
			}

			json, err := json.Marshal(utxo)

			if err != nil {
				logger.ErrorF("utxo marshal error :%s", err)
			}

			_, err = stmt.Exec(
				tx.ID,
				vout.N,
				vout.Address,
				vout.Asset,
				block.Index,
				vout.Value,
				string(json),
				time.Unix(block.Time, 0).Format(time.RFC3339),
			)

			if err != nil {
				logger.ErrorF("utxo bulk insert error :%s", err)
				return err
			}
		}
	}

	_, err = stmt.Exec()

	if err != nil {
		logger.ErrorF("utxo bulk insert error :%s", err)
	}

	return
}

func openDB(cnf *config.Config) (*sql.DB, error) {
	driver := cnf.GetString("dbwriter.database.driver", "xxxx")
	username := cnf.GetString("dbwriter.database.username", "xxx")
	password := cnf.GetString("dbwriter.database.password", "xxx")
	port := cnf.GetString("dbwriter.database.port", "6543")
	host := cnf.GetString("dbwriter.database.host", "localhost")
	schema := cnf.GetString("dbwriter.database.schema", "postgres")

	return sql.Open(driver, fmt.Sprintf("user=%v password=%v host=%v dbname=%v port=%v sslmode=disable", username, password, host, schema, port))
}
