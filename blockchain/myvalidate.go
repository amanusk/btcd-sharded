package blockchain

import (
	_ "encoding/binary"
	"fmt"
	reallog "log"
	_ "math"
	"math/big"
	_ "time"

	_ "github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	_ "github.com/btcsuite/btcd/txscript"
	_ "github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
)

// checkBlockSanity performs some preliminary checks on a block to ensure it is
// sane before continuing with block processing.  These checks are context free.
//
// The flags do not modify the behavior of this function directly, however they
// are needed to pass along to checkBlockHeaderSanity.
func MyCheckBlockSanity(block *btcutil.Block, powLimit *big.Int, timeSource MedianTimeSource, flags BehaviorFlags) error {
	msgBlock := block.MsgBlock()
	header := &msgBlock.Header
	err := checkBlockHeaderSanity(header, powLimit, timeSource, flags)
	if err != nil {
		return err
	}

	// A block must have at least one transaction.
	numTx := len(msgBlock.Transactions)
	if numTx == 0 {
		return ruleError(ErrNoTransactions, "block does not contain "+
			"any transactions")
	}

	// A block must not have more transactions than the max block payload or
	// else it is certainly over the weight limit.
	if numTx > MaxBlockBaseSize {
		str := fmt.Sprintf("block contains too many transactions - "+
			"got %d, max %d", numTx, MaxBlockBaseSize)
		return ruleError(ErrBlockTooBig, str)
	}

	// A block must not exceed the maximum allowed block payload when
	// serialized.
	serializedSize := msgBlock.SerializeSizeStripped()
	if serializedSize > MaxBlockBaseSize {
		str := fmt.Sprintf("serialized block is too big - got %d, "+
			"max %d", serializedSize, MaxBlockBaseSize)
		return ruleError(ErrBlockTooBig, str)
	}

	// The first transaction in a block must be a coinbase.
	transactions := block.Transactions()
	if !IsCoinBase(transactions[0]) {
		return ruleError(ErrFirstTxNotCoinbase, "first transaction in "+
			"block is not a coinbase")
	}

	// A block must not have more than one coinbase.
	for i, tx := range transactions[1:] {
		if IsCoinBase(tx) {
			str := fmt.Sprintf("block contains second coinbase at "+
				"index %d", i+1)
			return ruleError(ErrMultipleCoinbases, str)
		}
	}

	// Do some preliminary checks on each transaction to ensure they are
	// sane before continuing.
	for _, tx := range transactions {
		// Import log as reallog
		reallog.Println("Checking", tx)
		err := CheckTransactionSanity(tx)
		if err != nil {
			return err
		}
	}

	// NOTE: We do not do the merkle root at this moment
	// Build merkle tree and ensure the calculated merkle root matches the
	// entry in the block header.  This also has the effect of caching all
	// of the transaction hashes in the block to speed up future hash
	// checks.  Bitcoind builds the tree here and checks the merkle root
	// after the following checks, but there is no reason not to check the
	// merkle root matches here.
	//merkles := BuildMerkleTreeStore(block.Transactions(), false)
	//calculatedMerkleRoot := merkles[len(merkles)-1]
	//if !header.MerkleRoot.IsEqual(calculatedMerkleRoot) {
	//	str := fmt.Sprintf("block merkle root is invalid - block "+
	//		"header indicates %v, but calculated value is %v",
	//		header.MerkleRoot, calculatedMerkleRoot)
	//	return ruleError(ErrBadMerkleRoot, str)
	//}

	// Check for duplicate transactions.  This check will be fairly quick
	// since the transaction hashes are already cached due to building the
	// merkle tree above.
	existingTxHashes := make(map[chainhash.Hash]struct{})
	for _, tx := range transactions {
		hash := tx.Hash()
		if _, exists := existingTxHashes[*hash]; exists {
			str := fmt.Sprintf("block contains duplicate "+
				"transaction %v", hash)
			return ruleError(ErrDuplicateTx, str)
		}
		existingTxHashes[*hash] = struct{}{}
	}

	// The number of signature operations must be less than the maximum
	// allowed per block.
	totalSigOps := 0
	for _, tx := range transactions {
		// We could potentially overflow the accumulator so check for
		// overflow.
		lastSigOps := totalSigOps
		totalSigOps += (CountSigOps(tx) * WitnessScaleFactor)
		if totalSigOps < lastSigOps || totalSigOps > MaxBlockSigOpsCost {
			str := fmt.Sprintf("block contains too many signature "+
				"operations - got %v, max %v", totalSigOps,
				MaxBlockSigOpsCost)
			return ruleError(ErrTooManySigOps, str)
		}
	}

	return nil
}
