package fullblocktests

import (
	"bytes"
	_ "encoding/binary"
	_ "encoding/hex"
	_ "errors"
	"fmt"
	"log"
	_ "math"
	"math/big"
	"os"
	_ "runtime"
	"testing"

	"database/sql"
	_ "github.com/lib/pq"

	"github.com/btcsuite/btcd/blockchain"
	_ "github.com/btcsuite/btcd/btcec"
	_ "github.com/btcsuite/btcd/chaincfg"
	_ "github.com/btcsuite/btcd/chaincfg/chainhash"
	_ "github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"

	"github.com/btcsuite/btcutil"
)

type blockStatus byte

const (
	// statusDataStored indicates that the block's payload is stored on disk.
	statusDataStored blockStatus = 1 << iota
)

// Main function to test generating and inserting blocks
func TestSimpleBlock(t *testing.T) {
	// init log file
	f, err := os.OpenFile("testlog.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	defer f.Close()
	log.SetOutput(f)
	log.Println("This is a test log entry")

	db := initDB()
	defer db.Close()

	// Create the block generator, this will be used for creating and later
	// testing blocks
	g, err := makeTestGenerator(regressionNetParams)
	if err != nil {
		return
	}

	// Here we basically check the block and add it to the "blockchain"
	blockchain.MyCheckBlockSanity(btcutil.NewBlock(g.tip), g.params.PowLimit, blockchain.NewMedianTime(), blockchain.BFNone)
	if err != nil {
		log.Fatal(err)
	}

	// Create a new block node for the block and add it to the in-memory
	// block chain (could be either a side chain or the main chain).
	index := blockchain.MyNewBlockIndex(g.params)

	// This creates the first genesis block in the index map
	blockHeader := &g.tip.Header
	newNode := blockchain.NewBlockNode(blockHeader, 1)
	newNode.SetParent(nil)
	newNode.SetHeight(1)
	index.AddNode(newNode)

	addBlock(db, g.tip)

	// Now comes logic to connect the block to the best chain

	// Generate 100 blocks and add them to the in memory chain
	coinbaseMaturity := g.params.CoinbaseMaturity
	t.Log("Coinmbase maturity", coinbaseMaturity)
	for i := uint16(0); i < coinbaseMaturity; i++ {
		blockName := fmt.Sprintf("bm%d", i)
		g.nextBlock(blockName, nil)

		// This is where we want to add the block and all of its transactions to the DB
		processBlock(db, btcutil.NewBlock(g.tip), index, g.params.PowLimit)

		g.saveTipCoinbaseOut()
	}

	// Collect spendable outputs.  This simplifies the code below.
	var outs []*spendableOut
	for i := uint16(0); i < coinbaseMaturity; i++ {
		op := g.oldestCoinbaseOut()
		outs = append(outs, &op)
	}
	g.nextBlock("b1", outs[0])

	addBlock(db, g.tip)
	blockHash := g.tip.BlockHash()

	// Query all txs from databse
	rows, err := db.Query("SELECT * FROM txs WHERE blockHash=$1", blockHash[:])
	// Read the txs from the database after query
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

func initDB() *sql.DB {
	// Open the sql DB and join it
	db, err := sql.Open("postgres", "postgresql://amanusk@localhost:26257/blockchain?sslmode=disable")

	checkErr(err)

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

	// Create transactions table
	_, err = db.Exec(
		"CREATE TABLE IF NOT EXISTS txs (txHash BYTES PRIMARY KEY," +
			"blockHash BYTES, " +
			"BlockIndex INT, " +
			"txData BYTES)")
	if err != nil {
		log.Fatal(err)
	}
	return db
}

// Adds a block header to the headers table in the blockchain database
func addBlock(db *sql.DB, block *wire.MsgBlock) {
	addBlockHeader(db, block.Header)
	blockHash := block.BlockHash()
	for idx, val := range block.Transactions {
		addTX(db, blockHash[:], idx, val)
		//	t.Log("TX", idx)
		//	t.Log(val)
		//	t.Log("TX ins")
		//	for _, txval := range val.TxIn {
		//		t.Log(txval)
		//	}
		//	t.Log("TX outs")
		//	for _, txval := range val.TxOut {
		//		t.Log(txval)
		//	}
	}
}

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

// Stores a tx in the database
func addTX(db *sql.DB, blockHash []byte, idx int, tx *wire.MsgTx) {
	txHash := tx.TxHash()
	// Serialize tx to save
	var bb bytes.Buffer
	tx.Serialize(&bb)
	buf := bb.Bytes()

	_, err := db.Exec(
		"INSERT INTO txs (txhash, blockHash, BlockIndex, txData)"+
			"VALUES ($1, $2, $3, $4) ", txHash[:], blockHash, idx, buf)
	if err != nil {
		// Should be checked or something, left for debug
		log.Print(err)
	}
}

func processBlock(db *sql.DB, block *btcutil.Block, index *blockchain.BlockIndex, powLimit *big.Int) {

	blockHash := block.Hash()
	log.Printf("Processing block %v", blockHash)

	err := blockchain.MyCheckBlockSanity(block, powLimit, blockchain.NewMedianTime(), blockchain.BFNone)
	if err != nil {
		log.Fatal(err)
	}

	MymaybeAcceptBlock(db, index, block, blockchain.BFNone)

}

// maybeAcceptBlock potentially accepts a block into the block chain and, if
// accepted, returns whether or not it is on the main chain.  It performs
// several validation checks which depend on its position within the block chain
// before adding it.  The block is expected to have already gone through
// ProcessBlock before calling this function with it.
//
// The flags are also passed to checkBlockContext and connectBestChain.  See
// their documentation for how the flags modify their behavior.
//
// This function MUST be called with the chain state lock held (for writes).
func MymaybeAcceptBlock(db *sql.DB, index *blockchain.BlockIndex, block *btcutil.Block, flags blockchain.BehaviorFlags) (bool, error) {
	// The height of this block is one more than the referenced previous
	// block.
	prevHash := &block.MsgBlock().Header.PrevBlock
	prevNode := index.LookupNode(prevHash)
	if prevNode == nil {
		str := fmt.Sprintf("previous block %s is unknown", prevHash)
		log.Print(str)
		return false, nil
	} else if index.NodeStatus(prevNode).KnownInvalid() {
		str := fmt.Sprintf("previous block %s is known to be invalid", prevHash)
		log.Print(str)
		return false, nil
	}

	blockHeight := prevNode.GetHeight() + 1
	block.SetHeight(blockHeight)

	// The block must pass all of the validation rules which depend on the
	// position of the block within the block chain.
	//err := checkBlockContext(block, prevNode, flags)
	//if err != nil {
	//	return false, err
	//}

	// Insert the block into the database if it's not already there.  Even
	// though it is possible the block will ultimately fail to connect, it
	// has already passed all proof-of-work and validity tests which means
	// it would be prohibitively expensive for an attacker to fill up the
	// disk with a bunch of blocks that fail to connect.  This is necessary
	// since it allows block download to be decoupled from the much more
	// expensive connection logic.  It also has some other nice properties
	// such as making blocks that never become part of the main chain or
	// blocks that fail to connect available for further analysis.
	addBlock(db, block.MsgBlock())

	// Create a new block node for the block and add it to the in-memory
	// block chain (could be either a side chain or the main chain).
	blockHeader := &block.MsgBlock().Header
	newNode := blockchain.NewBlockNode(blockHeader, blockHeight)
	newNode.status = statusDataStored
	if prevNode != nil {
		newNode.SetParent(prevNode)
		newNode.SetHeight(blockHeight)
		newNode.workSum.Add(prevNode.workSum, newNode.workSum)
	}
	index.AddNode(newNode)
	//
	//	// Connect the passed block to the chain while respecting proper chain
	//	// selection according to the chain with the most proof of work.  This
	//	// also handles validation of the transaction scripts.
	//	isMainChain, err := b.connectBestChain(newNode, block, flags)
	//	if err != nil {
	//		return false, err
	//	}
	//
	//	// Notify the caller that the new block was accepted into the block
	//	// chain.  The caller would typically want to react by relaying the
	//	// inventory to other peers.
	//	b.chainLock.Unlock()
	//	b.sendNotification(NTBlockAccepted, block)
	//	b.chainLock.Lock()

	isMainChain := true // for now
	return isMainChain, nil
}

// Some prints to make sense of how the block is structured
//h := g.tip.Header
//t.Log(h)
//blockHash := h.BlockHash()
//t.Log("Orig blockhash", blockHash)
//byteHash := blockHash[:]
//t.Log("block hash", byteHash)
// Trying to revert this to original causes to invert the bytes (little/big endian)
// t.Log("To string hash", []byte(byteHash))

// Example query of block from DB
//var fromMerkleRoot []byte
//var fromBits int
//err = db.QueryRow(
//	"SELECT MerkleRoot,Bits FROM headers WHERE blockhash = $1", blockHash[:]).Scan(&fromMerkleRoot, &fromBits)
//if err != nil {
//	log.Fatal(err)
//}
