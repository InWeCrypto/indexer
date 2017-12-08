DROP INDEX IF EXISTS NEO_BLOCK_ID;
DROP TABLE IF EXISTS NEO_BLOCK;


DROP INDEX IF EXISTS NEO_TX_BLOCKS;
DROP INDEX IF EXISTS NEO_TX_TYPE;
DROP INDEX IF EXISTS NEO_TX_UID;
DROP INDEX IF EXISTS NEO_TX_ASSET;
DROP TABLE IF EXISTS NEO_TX;


DROP INDEX IF EXISTS NEO_UTXO_UID;
DROP INDEX IF EXISTS NEO_UTXO_ASSET;
DROP INDEX IF EXISTS NEO_UTXO_BLOCKS;
DROP TABLE IF EXISTS NEO_UTXO;


CREATE TABLE NEO_UTXO (
  "id"         SERIAL PRIMARY KEY,
  "tx"         VARCHAR(128) NOT NULL, -- tx by which the utxo created
  "n"          INT          NOT NULL,
  "address"    VARCHAR(128) NOT NULL, -- value out address
  "blocks"     BIGINT       NOT NULL, -- tx packaged block number
  "spentBlock" BIGINT       NOT NULL DEFAULT -1,
  "assert"     VARCHAR(128) NOT NULL, -- asset type string
  "value"      NUMERIC      NOT NULL, -- utxo value
  "json"       JSONB        NOT NULL, -- raw data of utxo format as json
  "createTime" TIMESTAMP    NOT NULL, -- utxo create time
  "spentTime"  TIMESTAMP ,-- utxo spent time
  "claimed"   BOOLEAN NOT NULL DEFAULT FALSE -- claimed flag
);


CREATE INDEX NEO_UTXO_UID
  ON NEO_UTXO (tx, address, n);

CREATE INDEX NEO_UTXO_ASSET
  ON NEO_UTXO (assert);

CREATE INDEX NEO_UTXO_BLOCKS
  ON NEO_UTXO (blocks);

CREATE INDEX NEO_UTXO_TIMES
  ON NEO_UTXO ("createTime","spentTime");


CREATE TABLE NEO_TX (
  "id"         SERIAL PRIMARY KEY,
  "blocks"     BIGINT       NOT NULL, -- tx packaged block number
  "tx"         VARCHAR(128) NOT NULL, -- tx by which the utxo created
  "address"    VARCHAR(128) NOT NULL, -- value out address
  "type"       VARCHAR(64)  NOT NULL, -- tx type
  "value"      NUMERIC      NOT NULL, -- utxo value
  "assert"     VARCHAR(128) NOT NULL, -- asset type string
  "updateTime" TIMESTAMP    NOT NULL-- update time
);

CREATE INDEX NEO_TX_UID
  ON NEO_TX (tx, address);
CREATE INDEX NEO_TX_TYPE
  ON NEO_TX (type);

CREATE INDEX NEO_TX_ASSET
  ON NEO_TX (assert);

CREATE INDEX NEO_TX_BLOCKS
  ON NEO_TX (blocks);


CREATE TABLE NEO_BLOCK(
  "sid" SERIAL PRIMARY KEY,
  "id"     BIGINT, -- block id
  "sysfee" DECIMAL NOT NULL DEFAULT 0,
  "netfee" DECIMAL NOT NULL DEFAULT 0,
  "createTime" TIMESTAMP    NOT NULL
);

CREATE INDEX NEO_BLOCK_ID
  ON NEO_BLOCK (id);