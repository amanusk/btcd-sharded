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
//func MymaybeAcceptBlock(block *btcutil.Block, flags BehaviorFlags) (bool, error) {
//	// The height of this block is one more than the referenced previous
//	// block.
//	prevHash := &block.MsgBlock().Header.PrevBlock
//	//prevNode := b.index.LookupNode(prevHash)
//	if prevNode == nil {
//		str := fmt.Sprintf("previous block %s is unknown", prevHash)
//		return false, ruleError(ErrPreviousBlockUnknown, str)
//	} else if b.index.NodeStatus(prevNode).KnownInvalid() {
//		str := fmt.Sprintf("previous block %s is known to be invalid", prevHash)
//		return false, ruleError(ErrInvalidAncestorBlock, str)
//	}
//
//	blockHeight := prevNode.height + 1
//	block.SetHeight(blockHeight)
//
//	// The block must pass all of the validation rules which depend on the
//	// position of the block within the block chain.
//	err := b.checkBlockContext(block, prevNode, flags)
//	if err != nil {
//		return false, err
//	}
//
//	// Insert the block into the database if it's not already there.  Even
//	// though it is possible the block will ultimately fail to connect, it
//	// has already passed all proof-of-work and validity tests which means
//	// it would be prohibitively expensive for an attacker to fill up the
//	// disk with a bunch of blocks that fail to connect.  This is necessary
//	// since it allows block download to be decoupled from the much more
//	// expensive connection logic.  It also has some other nice properties
//	// such as making blocks that never become part of the main chain or
//	// blocks that fail to connect available for further analysis.
//	err = b.db.Update(func(dbTx database.Tx) error {
//		return dbMaybeStoreBlock(dbTx, block)
//	})
//	if err != nil {
//		return false, err
//	}
//
//	// Create a new block node for the block and add it to the in-memory
//	// block chain (could be either a side chain or the main chain).
//	blockHeader := &block.MsgBlock().Header
//	newNode := newBlockNode(blockHeader, blockHeight)
//	newNode.status = statusDataStored
//	if prevNode != nil {
//		newNode.parent = prevNode
//		newNode.height = blockHeight
//		newNode.workSum.Add(prevNode.workSum, newNode.workSum)
//	}
//	b.index.AddNode(newNode)
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
//
//	return isMainChain, nil
//}
