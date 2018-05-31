// Copyright (c) 2015-2017 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package blockchain

import (
	"bytes"
	"errors"
	"fmt"
	reallog "log"

	"database/sql"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	_ "github.com/lib/pq" // This is needed for database/sql
)

// SQLBlockDB is a struct to hold an sql database
type SQLBlockDB struct {
	db *sql.DB
}

// AddBlock adds a block header to the headers table in the blockchain database
func (db *SQLBlockDB) AddBlock(block *wire.MsgBlock) {
	db.AddBlockHeader(block.Header)
	blockHash := block.BlockHash()
	for idx, val := range block.Transactions {
		db.AddTX(blockHash[:], int32(idx), val)
	}
}

// AddBlockHeader adds a block header to the headers table in the blockchain database
func (db *SQLBlockDB) AddBlockHeader(h wire.BlockHeader) {
	blockHash := h.BlockHash()
	_, err := db.db.Exec(
		"INSERT INTO headers (blockHash, version, PervBlock, MerkleRoot, Timestamp, Bits, Nonce)"+
			"VALUES ($1, $2, $3, $4, $5, $6, $7) ", blockHash[:], h.Version, h.PrevBlock[:], h.MerkleRoot[:], h.Timestamp, h.Bits, h.Nonce)
	if err != nil {
		// Should be checked or something, left for debug
		reallog.Print(err)
	}
}

// AddTX Stores a tx in the database
func (db *SQLBlockDB) AddTX(blockHash []byte, idx int32, tx *wire.MsgTx) {
	txHash := tx.TxHash()
	// Serialize tx to save
	var bb bytes.Buffer
	reallog.Println("Saving TX ", txHash, " with index ", idx)
	err := tx.Serialize(&bb)
	if err != nil {
		reallog.Fatal(err)
	}
	buf := bb.Bytes()
	// TODO: Instead, save a block with its txs for shorter search
	_, err = db.db.Exec(
		"INSERT INTO txs (txhash, blockHash, txindex, txData)"+
			"VALUES ($1, $2, $3, $4) ", txHash[:], blockHash, idx, buf)
	if err != nil {
		// Should be checked or something, left for debug
		reallog.Print(err)
	}
}

// RemoveUTXO deletes the utxo of the passed hash from the long-term UTXO set
func (db *SQLBlockDB) RemoveUTXO(key []byte) error {
	_, err := db.db.Exec("DELETE FROM utxos WHERE txhash in ($1);", key)
	if err != nil {
		// Should be checked or something, left for debug
		reallog.Print("SQL Remove Err", err)
		return err
	} else {
		reallog.Print("Removing spent TX ", key)
	}
	return nil
}

// StoreUTXO sotres a serialized UTXO as value. The key is the hash of the UTXO
// This is long term storage in the database
func (db *SQLBlockDB) StoreUTXO(key []byte, serialized []byte) {
	// TODO: consider: upsert instead of insert
	_, err := db.db.Exec("INSERT INTO utxos (txhash, utxodata)"+
		"VALUES ($1, $2) ON CONFLICT (txhash) DO "+
		"UPDATE SET utxodata=$2;", key, serialized)
	reallog.Print("Inserting TX", key, "Serialized: ", serialized)
	if err != nil {
		// Should be checked or something, left for debug
		reallog.Print("SQL Insert Err:", err)
	} else {
		reallog.Print("Save txout ", key)
	}
}

// FetchTXs fetches all transactions associated with the received block hash
func (db *SQLBlockDB) FetchTXs(hash chainhash.Hash) *wire.MsgBlockShard {
	reallog.Println("Fetching txs for block", hash)
	// Query all txs from databse
	rows, err := db.db.Query("SELECT * FROM txs WHERE blockHash=$1", hash[:])
	// Read the txs from the database after query
	if err != nil {
		reallog.Print("Err ", err)
		return nil
	}

	var blockShard wire.MsgBlockShard

	for rows.Next() {
		var txHash []byte
		var blockHash []byte
		var txIdx int32
		var txData []byte
		err = rows.Scan(&txHash, &blockHash, &txIdx, &txData)
		if err != nil {
			reallog.Println("Unable to scan transacions from query")
		}
		// Deserialize the transaction
		var tx wire.MsgTx
		rbuf := bytes.NewReader(txData)
		err := tx.Deserialize(rbuf)
		if err != nil {
			reallog.Printf("Deserialize error %v", err)
			continue
		}

		indexedTx := wire.NewTxIndexFromTx(&tx, txIdx)

		blockShard.AddTransaction(indexedTx)
	}
	//reallog.Println("Transactions in fetched block")
	//for _, val := range blockShard.Transactions {
	//	reallog.Printf("%s ", spew.Sdump(&val))
	//}
	return &blockShard
}

// SQLDbFetchUtxoEntry fetches the unspent transaction output information for
//the passed transaction hash.  Return nil when there is no entry.
func (db *SQLBlockDB) SQLDbFetchUtxoEntry(hash *chainhash.Hash) (*UtxoEntry, error) {
	var serializedUtxo []byte
	reallog.Println("Trying to fetch", hash[:])
	err := db.db.QueryRow(
		"SELECT utxodata FROM utxos WHERE txhash = $1", hash[:]).Scan(&serializedUtxo)
	if err != nil {
		reallog.Print("Err ", err)
		// If a TX is not found, it might be somewhere else
		return nil, nil
	}
	reallog.Print("Fetched Serialized UTXO", serializedUtxo)
	if serializedUtxo == nil {
		return nil, nil
	}

	// A non-nil zero-length entry means there is an entry in the database
	// for a fully spent transaction which should never be the case.
	//if len(serializedUtxo) == 0 {
	//	return nil, AssertError(fmt.Sprintf("database contains entry "+
	//		"for fully spent tx %v", hash))
	//}

	// Deserialize the utxo entry and return it.
	entry, err := deserializeUtxoEntry(serializedUtxo)
	if err != nil {
		// Ensure any deserialization errors are returned as database
		// corruption errors.
		if isDeserializeErr(err) {
			erro := fmt.Sprintf("corrupt utxo entry for %v: %v", hash, err)
			return nil, errors.New(erro)
		}

		return nil, err
	}

	return entry, nil
}

// FetchHeader fetches header of given hash from the database
func (db *SQLBlockDB) FetchHeader(hash chainhash.Hash) *wire.BlockHeader {
	reallog.Println("Fetching header of block", hash)
	row := db.db.QueryRow("SELECT * FROM headers WHERE blockHash=$1", hash[:])

	var blockHash []byte
	var h wire.BlockHeader

	err := row.Scan(&blockHash, &h.Version, &h.PrevBlock, &h.MerkleRoot, &h.Timestamp, &h.Bits, &h.Nonce)
	if err != nil {
		reallog.Println("Unable to scan transacions from query")
	}
	//reallog.Println("Block Header")
	//reallog.Printf("%s ", spew.Sdump(&h))
	return &h

}

// FetchCoinbase fetches the first (coinbase) tx of a block
// TODO: Move this to another table
func (db *SQLBlockDB) FetchCoinbase(hash *chainhash.Hash) *wire.MsgTxIndex {
	reallog.Println("Fetching header of block", hash)

	var txHash []byte
	var blockHash []byte
	var txIdx int32
	var txData []byte
	// TODO: Consider a different table for coinbase
	// TODO: Replace all SELECT * with only the columns you need
	// TODO: Use the txindex as the secondary Index (See cockroack docs)
	err := db.db.QueryRow("SELECT * FROM txs WHERE blockHash=$1 AND txindex=0", hash[:]).Scan(&txHash, &blockHash, &txIdx, &txData)

	if err != nil {
		reallog.Println("Unable to scan transacions from query ", err)
	}

	// Deserialize the transaction
	var tx wire.MsgTx
	rbuf := bytes.NewReader(txData)
	err = tx.Deserialize(rbuf)
	if err != nil {
		reallog.Printf("Deserialize error %v", err)
	}
	indexedTx := wire.NewTxIndexFromTx(&tx, txIdx)
	//reallog.Println("Transactions in fetched block")
	//for _, val := range blockShard.Transactions {
	//	reallog.Printf("%s ", spew.Sdump(&val))
	//}
	return indexedTx
}

// InitTables creates the tables in the SQL database
// This should only be done once when starting the database
func (db *SQLBlockDB) InitTables() error {
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

	// Create utxos table
	_, err = db.db.Exec(
		"CREATE TABLE IF NOT EXISTS utxos (txHash BYTES PRIMARY KEY," +
			"utxoData BYTES)")
	if err != nil {
		reallog.Fatal(err)
	}

	// Create headers table
	_, err = db.db.Exec(
		"CREATE TABLE IF NOT EXISTS txcache (txHash BYTES PRIMARY KEY," +
			"utxoData BYTES)")
	if err != nil {
		reallog.Fatal(err)
	}

	// Create transactions table
	_, err = db.db.Exec(
		"CREATE TABLE IF NOT EXISTS txs (txHash BYTES PRIMARY KEY," +
			"blockHash BYTES, " +
			"txindex INT, " +
			"txData BYTES)")
	if err != nil {
		reallog.Fatal(err)
	}

	return err
}

// OpenDB opens the sqlDB to use with our blockchain
// DB needs to be created and running
// Start with cockroach start --insecure --host=localhost
func OpenDB(postgres string) *SQLBlockDB {
	// Open the sql DB and join it
	reallog.Println("Connecting to ", postgres)
	db, err := sql.Open("postgres", postgres)
	//db, err := sql.Open("postgres", "postgresql://amanusk@localhost:26258/blockchain?sslmode=disable")
	if err != nil {
		panic(err)
	}

	return &SQLBlockDB{
		db: db,
	}
}

///
// ---------------- UTXO view methods --------------//
////

// NewUtoxView initialized an new view, with an SQL table as the database
func (db *SQLBlockDB) NewUtoxView() {
	// TODO: Consider drop tables if exist

	// Create utxos table
	_, err := db.db.Exec(
		"CREATE TABLE IF NOT EXISTS viewpoint (txhash BYTES PRIMARY KEY," +
			"modified BOOL, " +
			"version INT, " +
			"isCoinBase BOOL, " +
			"blockHeight INT)")
	if err != nil {
		reallog.Fatal(err)
	}
	// Create UtxoEntry table
	_, err = db.db.Exec(
		"CREATE TABLE IF NOT EXISTS utxooutputs (txhash BYTES," +
			"txOutIdx INT, " +
			"spent BOOL, " +
			"compressed BOOL, " +
			"amount INT, " +
			"pkscript BYTES, " +
			"PRIMARY KEY (txhash, txOutIdx), " +
			"CONSTRAINT fk_txhash FOREIGN KEY (txhash) REFERENCES viewpoint" +
			") INTERLEAVE IN PARENT viewpoint (txhash)")
	if err != nil {
		reallog.Fatal(err)
	}
	reallog.Printf("Created utxo view")

}

// StoreUtxoOutput adds an output entry to the TxOuts database.
func (db *SQLBlockDB) StoreUtxoOutput(txHash chainhash.Hash, txOutIdx int32, spent bool, amount int64, pkScript []byte) {
	_, err := db.db.Exec("INSERT INTO utxooutputs (txhash, txOutIdx, spent, compressed, amount, pkScript)"+
		"VALUES ($1, $2, $5, FALSE,$3, $4);", txHash[:], txOutIdx, amount, pkScript, spent)
	reallog.Print("Inserting output to utxo ", txHash[:])
	if err != nil {
		// Should be checked or something, left for debug
		reallog.Print("SQL Insert Err:", err)
	} else {
		reallog.Print("Save tx ", txHash[:])
	}
}

// StoreUtxoEntry stores a UTXO in a database
// This is different from StoreUTXO as here the data is not serialized
func (db *SQLBlockDB) StoreUtxoEntry(txHash chainhash.Hash, version int32, isCoinBase bool, blockHeight int32) {
	reallog.Print("txHash ", txHash)
	_, err := db.db.Exec("INSERT INTO viewpoint (txhash, version, iscoinbase, blockheight)"+
		"VALUES ($1, $2, $3, $4);", txHash[:], version, isCoinBase, blockHeight)
	reallog.Print("Inserting utxo ", txHash[:])
	if err != nil {
		reallog.Print("SQL Insert Err:", err)
	} else {
		reallog.Print("Save tx ", txHash)
	}
}

// Close the SQL database
func (db *SQLBlockDB) Close() {
	db.db.Close()
}
