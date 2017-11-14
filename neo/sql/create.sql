DROP INDEX IF EXISTS NEO_TX_TYPE;
DROP INDEX IF EXISTS NEO_TX_UID;
DROP INDEX IF EXISTS NEO_TX_ASSET;
DROP TABLE IF EXISTS NEO_TX;


DROP INDEX IF EXISTS NEO_UTXO_UID;
DROP TABLE IF EXISTS NEO_UTXO;


CREATE TABLE NEO_UTXO (
  "id"         SERIAL PRIMARY KEY,
  "tx"         VARCHAR(128) NOT NULL, -- tx by which the utxo created
  "n"          INT          NOT NULL,
  "address"    VARCHAR(128) NOT NULL, -- value out address
  "assert"     VARCHAR(128) NOT NULL, -- asset type string
  "used"       BOOLEAN      NOT NULL         DEFAULT FALSE, -- used flag
  "value"      NUMERIC      NOT NULL, -- utxo value
  "json"       JSONB        NOT NULL, -- raw data of utxo format as json
  "createTime" TIMESTAMP    NOT NULL, -- utxo create time
  "spentTime"  TIMESTAMP -- utxo spent time
);

CREATE INDEX NEO_UTXO_UID
  ON NEO_UTXO (tx, address, n);

CREATE INDEX NEO_UTXO_ASSET
  ON NEO_UTXO (assert);


CREATE TABLE NEO_TX (
  "id"         SERIAL PRIMARY KEY,
  "tx"         VARCHAR(128) NOT NULL, -- tx by which the utxo created
  "address"    VARCHAR(128) NOT NULL, -- value out address
  "type"       VARCHAR(64)  NOT NULL, -- tx type
  "assert"     VARCHAR(128) NOT NULL, -- asset type string
  "updateTime" TIMESTAMP    NOT NULL NOT NULL DEFAULT NOW()-- update time
);

CREATE INDEX NEO_TX_UID
  ON NEO_TX (tx, address);
CREATE INDEX NEO_TX_TYPE
  ON NEO_TX (type);

CREATE INDEX NEO_TX_ASSET
  ON NEO_TX (assert);