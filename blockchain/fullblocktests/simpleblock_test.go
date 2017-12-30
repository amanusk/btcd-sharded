package fullblocktests

import (
	"bytes"
	_ "encoding/binary"
	"encoding/hex"
	_ "errors"
	"fmt"
	"log"
	_ "math"
	_ "runtime"
	"testing"

	"database/sql"
	_ "github.com/lib/pq"

	_ "github.com/btcsuite/btcd/blockchain"
	_ "github.com/btcsuite/btcd/btcec"
	_ "github.com/btcsuite/btcd/chaincfg"
	_ "github.com/btcsuite/btcd/chaincfg/chainhash"
	_ "github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"

	_ "github.com/btcsuite/btcutil"
)

// Adds a block header to the headers table in the blockchain database
func addBlockHeader(db *sql.DB, h wire.BlockHeader) {
	blockHash := h.BlockHash()
	_, err := db.Exec(
		"INSERT INTO headers (blockHash, version, PervBlock, MerkleRoot, Timestamp, Bits, Nonce)"+
			"VALUES ($1, $2, $3, $4, $5, $6, $7) ", blockHash[:], h.Version, h.PrevBlock[:], h.MerkleRoot[:], h.Timestamp, h.Bits, h.Nonce)
	if err != nil {
		// Should be checked or something, left for debug
		log.Print(err)
	}
}

func addTX(db *sql.DB, blockHash []byte, idx int, tx *wire.MsgTx) {
	txHash := tx.TxHash()
	// Serialize tx to save
	var bb bytes.Buffer
	tx.Serialize(&bb)
	buf := bb.Bytes()

	_, err := db.Exec(
		"INSERT INTO txs (txhash, blockHash, blockIndex, txData)"+
			"VALUES ($1, $2, $3, $4) ", txHash[:], blockHash, idx, buf)
	if err != nil {
		// Should be checked or something, left for debug
		log.Print(err)
	}
}

func TestSimpleBlock(t *testing.T) {
	// Open the sql DB and join it
	db, err := sql.Open("postgres", "postgresql://amanusk@localhost:26257/blockchain?sslmode=disable")

	checkErr(err)
	defer db.Close()

	// Create headers table
	_, err = db.Exec(
		"CREATE TABLE IF NOT EXISTS headers (blockHash BYTES PRIMARY KEY," +
			"Version INT, " +
			"PervBlock BYTES, " +
			"MerkleRoot BYTES, " +
			"Timestamp TIMESTAMP, " +
			"Bits INT, " +
			"Nonce INT)")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(
		"CREATE TABLE IF NOT EXISTS txs (txHash BYTES PRIMARY KEY," +
			"blockHash BYTES, " +
			"blockIndex INT, " +
			"txData BYTES)")
	if err != nil {
		log.Fatal(err)
	}

	// Create the block generator, this will be used for creating and later
	// testing blocks
	g, err := makeTestGenerator(regressionNetParams)
	if err != nil {
		return
	}

	h := g.tip.Header
	t.Log(h)
	blockHash := h.BlockHash()
	t.Log("Orig blockhash", blockHash)
	byteHash := blockHash[:]
	t.Log("block hash", byteHash)
	// Trying to revert this to original causes to invert the bytes (little/big endian)
	t.Log("To string hash", []byte(byteHash))

	addBlockHeader(db, g.tip.Header)

	var fromMerkleRoot []byte
	var fromBits int
	err = db.QueryRow(
		"SELECT MerkleRoot,Bits FROM headers WHERE blockhash = $1", blockHash[:]).Scan(&fromMerkleRoot, &fromBits)
	if err != nil {
		log.Fatal(err)
	}

	t.Log("MerkleRoot", hex.EncodeToString(fromMerkleRoot))
	t.Log("Difficulty", fromBits)
	t.Log(g.tip.Transactions)
	t.Logf("%#v", g.tip.Transactions)
	// Validate serialize and deserialize transitions
	for _, val := range g.tip.Transactions {
		var bb bytes.Buffer
		val.Serialize(&bb)
		buf := bb.Bytes()

		t.Log(val)
		t.Log("TX Ins")
		for _, txval := range val.TxIn {
			t.Log(txval)
		}
		t.Log("TX outs")
		for _, txval := range val.TxOut {
			t.Log(txval)
		}

		var tx wire.MsgTx
		tx.Deserialize(bytes.NewReader(buf))
		t.Log(tx)

		t.Log("TX Ins")
		for _, txval := range tx.TxIn {
			t.Log(txval)
		}
		t.Log("TX outs")
		for _, txval := range tx.TxOut {
			t.Log(txval)
		}
	}

	//var outs []*spendableOut
	coinbaseMaturity := g.params.CoinbaseMaturity
	t.Log("Coinmbase maturity", coinbaseMaturity)
	for i := uint16(0); i < coinbaseMaturity; i++ {
		blockName := fmt.Sprintf("bm%d", i)
		g.nextBlock(blockName, nil)
		g.saveTipCoinbaseOut()
	}

	// Collect spendable outputs.  This simplifies the code below.
	var outs []*spendableOut
	for i := uint16(0); i < coinbaseMaturity; i++ {
		op := g.oldestCoinbaseOut()
		outs = append(outs, &op)
	}
	g.nextBlock("b1", outs[0])

	addBlockHeader(db, g.tip.Header)

	t.Log(g.tip.Transactions)
	t.Logf("%#v", g.tip.Transactions)

	blockHash = g.tip.BlockHash()
	for idx, val := range g.tip.Transactions {
		addTX(db, blockHash[:], idx, val)

		t.Log("TX", idx)
		t.Log(val)
		t.Log("TX ins")
		for _, txval := range val.TxIn {
			t.Log(txval)
		}
		t.Log("TX outs")
		for _, txval := range val.TxOut {
			t.Log(txval)
		}

	}
	rows, err := db.Query("SELECT * FROM txs WHERE blockHash=$1", blockHash[:])

	for rows.Next() {
		var txHash []byte
		var blockHash []byte
		var txIdx int
		var txData []byte
		err = rows.Scan(&txHash, &blockHash, &txIdx, &txData)
		checkErr(err)
		t.Log(txHash, blockHash, txIdx, txData)
	}

}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
