package blockchain

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/database"
	_ "github.com/btcsuite/btcd/database/ffldb"
	"github.com/btcsuite/btcutil"
)

const (
	testdb = "/home/amanusk/go/src/github.com/btcsuite/btcd/blockchain/testdb"
)

// This example demonstrates how to create a new chain instance and use
// ProcessBlock to attempt to attempt add a block to the chain.  As the package
// overview documentation describes, this includes all of the Bitcoin consensus
// rules.  This example intentionally attempts to insert a duplicate genesis
// block to illustrate how an invalid block is handled.
func TestExampleBlockChainProcessBlock(t *testing.T) {
	// Create a new database to store the accepted blocks into.  Typically
	// this would be opening an existing database and would not be deleting
	// and creating a new database like this, but it is done here so this is
	// a complete working example and does not leave temporary files laying
	// around.
	dbPath := filepath.Join(testdb, "exampleprocessblock")
	_ = os.RemoveAll(dbPath)
	db, err := database.Create("ffldb", dbPath, chaincfg.MainNetParams.Net)
	if err != nil {
		t.Logf("Failed to create database: %v\n", err)
		return
	}
	//defer os.RemoveAll(dbPath)
	defer db.Close()

	// Create a new BlockChain instance using the underlying database for
	// the main bitcoin network.  This example does not demonstrate some
	// of the other available configuration options such as specifying a
	// notification callback and signature cache.  Also, the caller would
	// ordinarily keep a reference to the median time source and add time
	// values obtained from other peers on the network so the local time is
	// adjusted to be in agreement with other peers.
	chain, err := blockchain.New(&blockchain.Config{
		DB:          db,
		ChainParams: &chaincfg.MainNetParams,
		TimeSource:  blockchain.NewMedianTime(),
	})
	if err != nil {
		t.Logf("Failed to create chain instance: %v\n", err)
		return
	}

	// Process a block.  For this example, we are going to intentionally
	// cause an error by trying to process the genesis block which already
	// exists.
	genesisBlock := btcutil.NewBlock(chaincfg.MainNetParams.GenesisBlock)
	isMainChain, isOrphan, err := chain.ProcessBlock(genesisBlock,
		blockchain.BFNone)
	if err != nil {
		t.Logf("Failed to process block: %v\n", err)
		return
	}
	t.Logf("Block accepted. Is it on the main chain?: %v", isMainChain)
	t.Logf("Block accepted. Is it an orphan?: %v", isOrphan)

	// Output:
	// Failed to process block: already have block 000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f
}
