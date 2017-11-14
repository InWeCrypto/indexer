package neo

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dynamicgo/config"
	"github.com/inwecrypto/neogo"
	"github.com/lib/pq"
)

// ETL neo indexer builtin etl service
type ETL struct {
	db     *sql.DB       // indexer database
	rpc    *neogo.Client // neo client
	tbutxo string        // utxo table
	tbtx   string        //tx table
}

// NewETL create new etl
func NewETL(cnf *config.Config) (*ETL, error) {
	db, err := openDB(cnf)

	if err != nil {
		return nil, err
	}

	etl := &ETL{
		db:     db,
		tbutxo: cnf.GetString("dbwriter.tables.utxo", "NEO_UTXO"),
		tbtx:   cnf.GetString("dbwriter.tables.tx", "NEO_TX"),
	}

	return etl, nil
}

// Produce implement mq producer interface
func (etl *ETL) Produce(topic string, key []byte, content interface{}) error {
	return etl.processBlock(content.(*neogo.Block))
}

func (etl *ETL) processBlock(block *neogo.Block) (err error) {

	dbTx, err := etl.db.Begin()

	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			err1 := dbTx.Rollback()
			if err1 != nil {
				logger.ErrorF("rollback error, %s", err1)
			}
		} else {
			err = dbTx.Commit()
			if err == nil {
				logger.DebugF("create indexer for block %d succeed", block.Index)
			}
		}
	}()

	if err = etl.bulkInsertUTXO(dbTx, block); err != nil {
		return
	}

	if err = etl.marktedVinUTXO(dbTx, block); err != nil {
		return
	}

	if err = etl.bulkInsertTX(dbTx, block); err != nil {
		return
	}

	return nil
}

func (etl *ETL) bulkInsertTX(dbTx *sql.Tx, block *neogo.Block) (err error) {
	var stmt *sql.Stmt
	stmt, err = dbTx.Prepare(pq.CopyIn(etl.tbtx, "tx", "address", "type", "assert"))

	if err != nil {
		logger.ErrorF("tx bulk prepare error :%s", err)
		return err
	}

	defer func() {
		err = stmt.Close()
	}()

	for _, tx := range block.Transactions {

		if len(tx.Vout) == 0 {
			logger.DebugF("tx %s %s without vout ", tx.ID, tx.Type)
			continue
		}

		for _, vout := range tx.Vout {

			logger.DebugF("tx %s vout %d ", tx.ID, vout.N)

			_, err = stmt.Exec(tx.ID, vout.Address, tx.Type, vout.Asset)

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

func (etl *ETL) marktedVinUTXO(dbTx *sql.Tx, block *neogo.Block) (err error) {
	var stmt *sql.Stmt

	sqlStr := fmt.Sprintf(`update %s set "used"=TRUE where "tx"=$1 and "n"=$2`, etl.tbutxo)

	stmt, err = dbTx.Prepare(sqlStr)

	if err != nil {
		logger.ErrorF("market utxo prepare error :%s", err)
		return err
	}

	defer func() {
		err = stmt.Close()
	}()

	for _, tx := range block.Transactions {
		for _, vin := range tx.Vin {
			_, err := stmt.Exec(vin.TransactionID, vin.Vout)

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
	stmt, err = dbTx.Prepare(pq.CopyIn(etl.tbutxo, "tx", "n", "address", "assert", "value", "json", "createTime"))

	if err != nil {
		logger.ErrorF("utxo bulk prepare error :%s", err)
		return err
	}

	defer func() {
		err = stmt.Close()
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
