// Copyright (c) 2015-2017 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package blockchain

import (
	"bytes"
	reallog "log"

	"database/sql"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	_ "github.com/lib/pq"
)

type SqlBlockDB struct {
	db *sql.DB
}

// Adds a block header to the headers table in the blockchain database
func (db *SqlBlockDB) AddBlock(block *wire.MsgBlock) {
	db.AddBlockHeader(block.Header)
	blockHash := block.BlockHash()
	for idx, val := range block.Transactions {
		db.AddTX(blockHash[:], idx, val)
	}
}

// Adds a block header to the headers table in the blockchain database
func (db *SqlBlockDB) AddBlockHeader(h wire.BlockHeader) {
	blockHash := h.BlockHash()
	_, err := db.db.Exec(
		"INSERT INTO headers (blockHash, version, PervBlock, MerkleRoot, Timestamp, Bits, Nonce)"+
			"VALUES ($1, $2, $3, $4, $5, $6, $7) ", blockHash[:], h.Version, h.PrevBlock[:], h.MerkleRoot[:], h.Timestamp, h.Bits, h.Nonce)
	if err != nil {
		// Should be checked or something, left for debug
		reallog.Print(err)
	}
}

// Stores a tx in the database
func (db *SqlBlockDB) AddTX(blockHash []byte, idx int, tx *wire.MsgTx) {
	txHash := tx.TxHash()
	// Serialize tx to save
	var bb bytes.Buffer
	tx.Serialize(&bb)
	buf := bb.Bytes()

	_, err := db.db.Exec(
		"INSERT INTO txs (txhash, blockHash, BlockIndex, txData)"+
			"VALUES ($1, $2, $3, $4) ", txHash[:], blockHash, idx, buf)
	if err != nil {
		// Should be checked or something, left for debug
		reallog.Print(err)
	}
}

func (db *SqlBlockDB) RemoveUTXO(txHash chainhash.Hash) {
	_, err := db.db.Exec("DELETE FROM utxos WHERE txhash in ($1);", txHash[:])
	if err != nil {
		// Should be checked or something, left for debug
		reallog.Print("SQL Remove Err", err)
	} else {
		reallog.Print("Removing spent TX ", txHash)
	}
}

func (db *SqlBlockDB) StoreUTXO(txHash chainhash.Hash, serialized []byte) {
	_, err := db.db.Exec("INSERT INTO utxos (txhash, utxodata)"+
		"VALUES ($1, $2) ON CONFLICT (txhash) DO "+
		"UPDATE SET utxodata=$2;", txHash[:], serialized)
	reallog.Print("Inserting TX", txHash[:], "Serialized: ", serialized)
	if err != nil {
		// Should be checked or something, left for debug
		reallog.Print("SQL Insert Err:", err)
	} else {
		reallog.Print("Save tx ", txHash)
	}
}

// Create the tables in the SQL database
// This should only be done once when starting the database
func (db *SqlBlockDB) InitTables() error {
	// Create headers table
	_, err := db.db.Exec(
		"CREATE TABLE IF NOT EXISTS headers (blockHash BYTES PRIMARY KEY," +
			"Version INT, " +
			"PervBlock BYTES, " +
			"MerkleRoot BYTES, " +
			"Timestamp TIMESTAMP, " +
			"Bits INT, " +
			"Nonce INT)")
	if err != nil {
		reallog.Fatal(err)
	}

	// Create headers table
	_, err = db.db.Exec(
		"CREATE TABLE IF NOT EXISTS utxos (txHash BYTES PRIMARY KEY," +
			"utxoData BYTES)")
	if err != nil {
		reallog.Fatal(err)
	}

	// Create transactions table
	_, err = db.db.Exec(
		"CREATE TABLE IF NOT EXISTS txs (txHash BYTES PRIMARY KEY," +
			"blockHash BYTES, " +
			"BlockIndex INT, " +
			"txData BYTES)")
	if err != nil {
		reallog.Fatal(err)
	}

	return err
}

// Function to open the sqlDB to use with our blockchain
// DB needs to be created and running
// Start with cockroach start --insecure --host=localhost
func OpenDB() *SqlBlockDB {
	// Open the sql DB and join it
	db, err := sql.Open("postgres", "postgresql://amanusk@localhost:26257/blockchain?sslmode=disable")
	if err != nil {
		panic(err)
	}

	return &SqlBlockDB{
		db: db,
	}
}

func (db *SqlBlockDB) Close() {
	db.db.Close()
}
