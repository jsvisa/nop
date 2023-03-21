# nop

NFT Orderbook Parser

## Getting started

## Local Development

Clone this project

```bash
git clone https://github.com/jsvisa/nop.git
cd nop
make setup
```

You need soft link this project into [blockchain-etl](https://github.com/jsvisa/blockchain-etl/), eg:

```bash
cd /the/path/to/blockchain-etl

ln -snf ../nop/nop nop
```

Create the PostgreSQL database and table:

```sql
CREATE TABLE IF NOT EXISTS ethereum.v2_nft_orderbooks (
    id                      BIGSERIAL,
    _st                     INTEGER,
    _st_day                 DATE,
    blknum                  BIGINT,
    txhash                  CHAR(66) NOT NULL,
    txpos                   BIGINT,
    xfer_logpos             BIGINT,
    order_logpos            BIGINT,
    token_type              TEXT,
    token_address           TEXT,
    token_id                NUMERIC,
    token_value             NUMERIC,
    from_address            TEXT,
    to_address              TEXT,
    maker                   TEXT,
    taker                   TEXT,
    price                   NUMERIC,
    currency                CHAR(42) ,
    currency_decimals       INTEGER,
    pack_index              INTEGER,
    pack_count              INTEGER,
    value                   NUMERIC,
    value_from              CHAR(42),
    value_to                CHAR(42),
    value_usd               NUMERIC,
    platform                CHAR(42),
    app                     TEXT,
    action                  TEXT,
    pattern                 TEXT,
    fee_from                CHAR(42),
    fee_to                  CHAR(42),
    fee_currency            CHAR(42),
    fee_currency_decimals   INTEGER,
    fee_value               NUMERIC,
    fee_value_usd           NUMERIC,
    created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at              TIMESTAMP DEFAULT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ethereum_v2_nft_orderbooks_id_idx ON ethereum.v2_nft_orderbooks(id);
CREATE INDEX IF NOT EXISTS ethereum_v2_nft_orderbooks_st_idx ON ethereum.v2_nft_orderbooks(_st);
CREATE INDEX IF NOT EXISTS ethereum_v2_nft_orderbooks_token_id_st_idx ON ethereum.v2_nft_orderbooks(token_address, token_id, _st);

```

Then test the specified NFT Platform as below(eg: Seaport):

```bash
./etl eth.export-nft-orderbook -c ethereum \
    -l .priv/nft-sea-orderbook.lsp \
    -p http://127.0.0.1:8545 \
    --price-url "http://127.0.0.1:9000" \
    --target-pg-url "postgresql://postgres:root@127.0.0.1:5432/postgres" \
    --start-block 14946474 \
    --nft-platforms seaport
```

## Run in production

Add this package into your Pipfile

```
nop = {git = "https://github.com/jsvisa/nop.git"}
```

And then run

```bash
pipenv install
```
