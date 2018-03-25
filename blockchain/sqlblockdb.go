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
	_ "github.com/davecgh/go-spew/spew"
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
		db.AddTX(blockHash[:], int32(idx), val)
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
func (db *SqlBlockDB) AddTX(blockHash []byte, idx int32, tx *wire.MsgTx) {
	txHash := tx.TxHash()
	// Serialize tx to save
	var bb bytes.Buffer
	reallog.Println("Saving TX ", txHash, " with index ", idx)
	tx.Serialize(&bb)
	buf := bb.Bytes()

	_, err := db.db.Exec(
		"INSERT INTO txs (txhash, blockHash, txindex, txData)"+
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
	// TODO: consider: upsert instead of insert
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

// Fetch all transactions associated with the received block hash
func (db *SqlBlockDB) FetchTXs(hash chainhash.Hash) *wire.MsgBlockShard {
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

// Fetch the unspent transaction output information for the passed
// transaction hash.  Return nil when there is no entry.
func (db *SqlBlockDB) SqlDbFetchUtxoEntry(hash *chainhash.Hash) (*UtxoEntry, error) {
	var serializedUtxo []byte
	reallog.Println("Trying to fetch", hash[:])
	err := db.db.QueryRow(
		"SELECT utxodata FROM utxos WHERE txhash = $1", hash[:]).Scan(&serializedUtxo)
	if err != nil {
		reallog.Print("Err ", err)
	} else {
		reallog.Print("Fetched Serialized UTXO", serializedUtxo)
	}
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

// Fetch header of given hash
func (db *SqlBlockDB) FetchHeader(hash chainhash.Hash) *wire.BlockHeader {
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

// Fetch the first (coinbase) tx of a block
// TODO: Move this to another table
func (db *SqlBlockDB) FetchCoinbase(hash *chainhash.Hash) *wire.MsgTxIndex {
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

// Function to open the sqlDB to use with our blockchain
// DB needs to be created and running
// Start with cockroach start --insecure --host=localhost
func OpenDB(postgres string) *SqlBlockDB {
	// Open the sql DB and join it
	reallog.Println("Connecting to ", postgres)
	db, err := sql.Open("postgres", postgres)
	//db, err := sql.Open("postgres", "postgresql://amanusk@localhost:26258/blockchain?sslmode=disable")
	if err != nil {
		panic(err)
	}

	return &SqlBlockDB{
		db: db,
	}
}

// ---------------- UTXO view methods --------------//

func (db *SqlBlockDB) NewUtoxView() {
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

// Adds an entry to the TxOuts database
func (db *SqlBlockDB) StoreUtxoOutput(txHash chainhash.Hash, txOutIdx int32, spent bool, amount int64, pkScript []byte) {
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

func (db *SqlBlockDB) StoreUtxoEntry(txHash chainhash.Hash, version int32, isCoinBase bool, blockHeight int32) {
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

func (db *SqlBlockDB) Close() {
	db.db.Close()
}

// Fetch the unspent transaction output information for the passed
// transaction hash.
// This fetches only the necessary information from the viewpoint
func (db *SqlBlockDB) FetchUtxoEntry(hash *chainhash.Hash) (*UtxoEntry, error) {

	var txHash []byte
	var modified bool
	var version int32
	var isCoinBase bool
	var blockHeight int32

	reallog.Println("Trying to fetch", hash[:])
	err := db.db.QueryRow(
		"SELECT * FROM viewpoint WHERE txhash = $1", hash[:]).Scan(&txHash, &modified, &version, &isCoinBase, &blockHeight)
	if err != nil {
		reallog.Print("Err ", err)
		return nil, nil
	} else {
		reallog.Println("Fetched UTXO Entry ", hash)
	}

	entry := newUtxoEntry(version, isCoinBase, blockHeight)
	entry.modified = modified

	// Fetch all outputs of this UtxoEntry:
	rows, err := db.db.Query(
		"SELECT * FROM utxooutputs WHERE txhash = $1", hash[:])
	if err != nil {
		reallog.Println("Err ", err)
		return nil, err
	} else {
		reallog.Println("Fetched Ouptputs of UtxoEntry ", hash)
	}

	for rows.Next() {
		var txHash []byte
		var txOutIdx int
		var spent bool
		var compressed bool
		var amount int64
		var pkScript []byte
		err = rows.Scan(&txHash, &txOutIdx, &spent, &compressed, &amount, &pkScript)
		if err != nil {
			reallog.Println("Unable to scan outputs from query")
		}
		// Deserialize the transaction
		entry.sparseOutputs[uint32(txOutIdx)] = &utxoOutput{
			spent:      spent,
			compressed: compressed,
			amount:     amount,
			pkScript:   pkScript,
		}
	}
	return entry, nil
}

// Updates the blockHeight of a UtxoEntry
func (db *SqlBlockDB) UpdateUtxoEntryBlockHeight(txHash chainhash.Hash, blockHeight int32) {
	_, err := db.db.Exec("UPDATE viewpoint SET blockheight=$1 WHERE txhash=$2;", blockHeight, txHash[:])
	reallog.Print("Update height of ", txHash[:], " to ", blockHeight)
	if err != nil {
		reallog.Print("SQL Update Err:", err)
	} else {
		reallog.Print("Save tx ", txHash)
	}
}

func (db *SqlBlockDB) UpdateUtxoEntryModified(txHash chainhash.Hash) {
	_, err := db.db.Exec("UPDATE viewpoint SET modified=TRUE WHERE txhash=$1;", txHash[:])
	if err != nil {
		reallog.Print("SQL Update Err:", err)
	} else {
		reallog.Print("Update ", txHash[:], " to modified")
	}
}

func (db *SqlBlockDB) UpdateUtxoOuptput(txHash chainhash.Hash, txOutIdx int32, amount int64, pkScript []byte) bool {
	//_, err := db.db.Exec("UPDATE utxooutputs SET "+
	//	"spend=FALSE, compressed = FALSE, amount=$1, pkScript=$2 WHERE txhash=$3 AND txOutIdx=$4;", amount, pkScript, txHash[:], txOutIdx)
	_, err := db.db.Exec("UPSERT INTO utxooutputs "+
		"VALUES ($1, $2, FALSE, FALSE, $3, $4);", txHash[:], txOutIdx, amount, pkScript)
	if err != nil {
		// Should be checked or something, left for debug
		reallog.Print("Cannot update non existing tx", err)
		return false
	} else {
		reallog.Print("Update pkscript and amount of tx", txHash[:])
		return true
	}
}

// Marks output and index as spent.
// Does nothing if output does not exist
func (db *SqlBlockDB) UpdateOutputSpent(txHash chainhash.Hash, txOutIdx uint32) {
	_, err := db.db.Exec("UPDATE utxooutputs SET modified=TRUE, spent=TRUE WHERE txhash=$1 AND txOutIdx=$2;", txHash[:], txOutIdx)
	if err != nil {
		reallog.Println("SQL Update Err:", err)
	} else {
		reallog.Println("Update ", txHash[:], " output ", txOutIdx, " to spent")
	}
}

func (db *SqlBlockDB) FetchUtxoOutput(txHash chainhash.Hash, txOutIdx int32) *utxoOutput {

	var spent bool
	var compressed bool
	var amount int64
	var pkScript []byte

	reallog.Println("Trying to fetch", txHash[:], " Index ", txOutIdx)
	err := db.db.QueryRow(
		"SELECT spent,compressed,amount,pkScript FROM utxooutputs WHERE txhash = $1 AND txOutIdx=$2", txHash[:], txOutIdx).Scan(&spent, &compressed, &amount, &pkScript)
	if err != nil {
		reallog.Print("Err ", err)
		return nil
	} else {
		reallog.Print("Fetched TxOutput at idx", txOutIdx)
	}
	return &utxoOutput{
		spent:      spent,
		compressed: compressed,
		amount:     amount,
		pkScript:   pkScript,
	}

}

// Fetch all transactions associated with the received block hash
func (db *SqlBlockDB) FetchAllUtxoEntries() map[chainhash.Hash]*UtxoEntry {
	reallog.Println("Fetching all UtxoEntries")

	entries := make(map[chainhash.Hash]*UtxoEntry)

	rows, err := db.db.Query("SELECT txHash FROM viewpoint")
	// Read the txs from the database after query
	if err != nil {
		reallog.Print("Err ", err)
		return nil
	}

	for rows.Next() {
		var txHash []byte
		err = rows.Scan(&txHash)
		if err != nil {
			reallog.Println("Unable to scan txHash")
		}
		hash, err := chainhash.NewHash(txHash)
		if err != nil {
			reallog.Println("Unable to create Hash from bytes")
		}
		entries[*hash], err = db.FetchUtxoEntry(hash)
		if err != nil {
			reallog.Fatalf("Unable to fetch txOuts")
		}
	}
	return entries
}
