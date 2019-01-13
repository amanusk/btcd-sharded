// Copyright (c) 2013-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package blockchain

import (
	"bytes"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcutil"
)

const (
	// CoinbaseWitnessDataLen is the required length of the only element within
	// the coinbase's witness data if the coinbase transaction contains a
	// witness commitment.
	CoinbaseWitnessDataLen = 32

	// CoinbaseWitnessPkScriptLength is the length of the public key script
	// containing an OP_RETURN, the WitnessMagicBytes, and the witness
	// commitment itself. In order to be a valid candidate for the output
	// containing the witness commitment
	CoinbaseWitnessPkScriptLength = 38
)

var (
	// WitnessMagicBytes is the prefix marker within the public key script
	// of a coinbase output to indicate that this output holds the witness
	// commitment for a block.
	WitnessMagicBytes = []byte{
		txscript.OP_RETURN,
		txscript.OP_DATA_36,
		0xaa,
		0x21,
		0xa9,
		0xed,
	}
)

// nextPowerOfTwo returns the next highest power of two from a given number if
// it is not already a power of two.  This is a helper function used during the
// calculation of a merkle tree.
func nextPowerOfTwo(n int) int {
	// Return the number if it's already a power of 2.
	if n&(n-1) == 0 {
		return n
	}

	// Figure out and return the next power of two.
	exponent := uint(math.Log2(float64(n))) + 1
	return 1 << exponent // 2^exponent
}

// HashMerkleBranches takes two hashes, treated as the left and right tree
// nodes, and returns the hash of their concatenation.  This is a helper
// function used to aid in the generation of a merkle tree.
func HashMerkleBranches(left *chainhash.Hash, right *chainhash.Hash) *chainhash.Hash {
	// Concatenate the left and right nodes.
	var hash [chainhash.HashSize * 2]byte
	copy(hash[:chainhash.HashSize], left[:])
	copy(hash[chainhash.HashSize:], right[:])

	newHash := chainhash.DoubleHashH(hash[:])
	return &newHash
}

// BuildMerkleTreeStore creates a merkle tree from a slice of transactions,
// stores it using a linear array, and returns a slice of the backing array.  A
// linear array was chosen as opposed to an actual tree structure since it uses
// about half as much memory.  The following describes a merkle tree and how it
// is stored in a linear array.
//
// A merkle tree is a tree in which every non-leaf node is the hash of its
// children nodes.  A diagram depicting how this works for bitcoin transactions
// where h(x) is a double sha256 follows:
//
//	         root = h1234 = h(h12 + h34)
//	        /                           \
//	  h12 = h(h1 + h2)            h34 = h(h3 + h4)
//	   /            \              /            \
//	h1 = h(tx1)  h2 = h(tx2)    h3 = h(tx3)  h4 = h(tx4)
//
// The above stored as a linear array is as follows:
//
// 	[h1 h2 h3 h4 h12 h34 root]
//
// As the above shows, the merkle root is always the last element in the array.
//
// The number of inputs is not always a power of two which results in a
// balanced tree structure as above.  In that case, parent nodes with no
// children are also zero and parent nodes with only a single left node
// are calculated by concatenating the left node with itself before hashing.
// Since this function uses nodes that are pointers to the hashes, empty nodes
// will be nil.
//
// The additional bool parameter indicates if we are generating the merkle tree
// using witness transaction id's rather than regular transaction id's. This
// also presents an additional case wherein the wtxid of the coinbase transaction
// is the zeroHash.
func BuildMerkleTreeStore(transactions []*btcutil.Tx, witness bool) []*chainhash.Hash {
	// Calculate how many entries are required to hold the binary merkle
	// tree as a linear array and create an array of that size.
	nextPoT := nextPowerOfTwo(len(transactions))
	//fmt.Println("nextPoT", nextPoT)
	//fmt.Println("Exponent", exponent)
	arraySize := nextPoT*2 - 1
	//fmt.Println("ArraySize", arraySize)

	merkles := make([]*chainhash.Hash, arraySize)
	startTime := time.Now()
	// Create the base transaction hashes and populate the array with them.
	for i, tx := range transactions {
		// If we're computing a witness merkle root, instead of the
		// regular txid, we use the modified wtxid which includes a
		// transaction's witness data within the digest. Additionally,
		// the coinbase's wtxid is all zeroes.
		switch {
		case witness && i == 0:
			var zeroHash chainhash.Hash
			merkles[i] = &zeroHash
		case witness:
			wSha := tx.MsgTx().WitnessHash()
			merkles[i] = &wSha
		default:
			merkles[i] = tx.Hash()
		}
	}
	endTime := time.Since(startTime)
	fmt.Println("Orig Tx Hashes only", endTime)

	startTime2 := time.Now()
	// Start the array offset after the last transaction and adjusted to the
	// next power of two.
	offset := nextPoT
	for i := 0; i < arraySize-1; i += 2 {
		switch {
		// When there is no left child node, the parent is nil too.
		case merkles[i] == nil:
			merkles[offset] = nil

		// When there is no right child, the parent is generated by
		// hashing the concatenation of the left child with itself.
		case merkles[i+1] == nil:
			newHash := HashMerkleBranches(merkles[i], merkles[i])
			merkles[offset] = newHash

		// The normal case sets the parent node to the double sha256
		// of the concatentation of the left and right children.
		default:
			newHash := HashMerkleBranches(merkles[i], merkles[i+1])
			merkles[offset] = newHash
		}
		offset++
	}
	endTime = time.Since(startTime2)
	fmt.Println("Orig root took", endTime)

	endTime = time.Since(startTime)
	fmt.Println("Orig Merkle took", endTime)

	exponent := uint(math.Log2(float64(nextPoT)))
	newmerkles := make([]*chainhash.Hash, arraySize)
	startTime3 := time.Now()
	// New merkle root
	length := len(transactions)
	wg := new(sync.WaitGroup)
	workers := 2
	if length < workers {
		workers = 1
	}
	fmt.Println("TxCount:", length)
	wg.Add(workers)
	chunckSize := length / workers
	fmt.Println("chunksize", chunckSize)
	if workers == 1 {
		go func(length int, txs []*btcutil.Tx, merkles []*chainhash.Hash) {
			for i := 0; i < length; i++ {
				if txs[i] == nil {
					merkles[i] = nil
				} else {
					merkles[i] = txs[i].Hash()
				}
			}
			wg.Done()
		}(length, transactions[:], newmerkles[:])
		wg.Wait()
	} else {
		for j := 0; j < workers-1; j++ {
			go func(length int, txs []*btcutil.Tx, merkles []*chainhash.Hash) {
				for i := 0; i < length; i++ {
					if txs[i] == nil {
						merkles[i] = nil
					} else {
						merkles[i] = txs[i].Hash()
					}
				}
				wg.Done()
			}(chunckSize, transactions[chunckSize*j:chunckSize*(j+1)], newmerkles[chunckSize*j:chunckSize*(j+1)])
		}
		go func(length int, txs []*btcutil.Tx, merkles []*chainhash.Hash) {
			for i := 0; i < length; i++ {
				if txs[i] == nil {
					merkles[i] = nil
				} else {
					merkles[i] = txs[i].Hash()
				}
			}
			wg.Done()
		}(length-(chunckSize*(workers-1)), transactions[chunckSize*(workers-1):], newmerkles[chunckSize*(workers-1):])
		wg.Wait()
	}

	endTime3 := time.Since(startTime3)
	fmt.Println("New Tx Hashes only", endTime3)
	startTime4 := time.Now()

	//fmt.Println("Transactions number", len(transactions))
	offsetFromArrayStart := 0
	//fmt.Println("Expononet is", exponent)
	// cpus := nextPowerOfTwo(runtime.NumCPU())
	cpus := nextPowerOfTwo(6)
	for level := uint(0); level < uint(exponent); level++ {
		wg := new(sync.WaitGroup)
		levelLength := 1 << (exponent - level)
		workers = cpus
		if (levelLength / 2) < cpus {
			workers = levelLength / 2
		}
		wg.Add(workers)
		readStart := offsetFromArrayStart
		writeStart := offsetFromArrayStart + levelLength
		items := levelLength / workers
		//fmt.Println("Workers", workers)
		for n := 0; n < workers; n++ {
			readSlice := newmerkles[readStart+(items*n) : readStart+(items*(n+1))]
			writeSlice := newmerkles[writeStart+((items/2)*n) : writeStart+((items/2)*(n+1))]
			go func(read []*chainhash.Hash, write []*chainhash.Hash, items int) {
				// fmt.Println("items", items)
				// fmt.Println("iterations", iterations)
				for i := 0; i < items-1; i += 2 {
					// fmt.Println("i", i)
					// if read[i] != nil {
					// 	fmt.Println("Read i", i, read[i].String())
					// } else {
					// 	fmt.Println("Read i", i, nil)
					// }
					// if read[i+1] != nil {
					// 	fmt.Println("Read i+1", i+1, read[i+1].String())
					// } else {
					// 	fmt.Println("Read i+1", i+1, nil)
					// }
					switch {
					// When there is no left child node, the parent is nil too.
					case read[i] == nil:
						write[i/2] = nil

					// When there is no right child, the parent is generated by
					// hashing the concatenation of the left child with itself.
					case read[i+1] == nil:
						newHash := HashMerkleBranches(read[i], read[i])
						write[i/2] = newHash

					// The normal case sets the parent node to the double sha256
					// of the concatentation of the left and right children.
					default:
						newHash := HashMerkleBranches(read[i], read[i+1])
						write[i/2] = newHash
					}
				}
				wg.Done()

			}(readSlice, writeSlice, items)
		}
		wg.Wait()

		// fmt.Println("Offset is", offsetFromArrayStart)
		// fmt.Println("Orig Merkle")
		// for idx := range merkles {
		// 	if merkles[idx] != nil {
		// 		fmt.Println("Idx", idx, ":", merkles[idx].String())
		// 	} else {
		// 		fmt.Println("Idx", idx, ":", nil)
		// 	}
		// }
		// fmt.Println("New Merkle")
		// for idx := range merkles {
		// 	if newmerkles[idx] != nil {
		// 		fmt.Println("Idx", idx, ":", newmerkles[idx].String())
		// 	} else {
		// 		fmt.Println("Idx", idx, ":", nil)
		// 	}
		// }

		offsetFromArrayStart += levelLength
	}

	endTime3 = time.Since(startTime4)
	fmt.Println("New root took", endTime3)

	endTime3 = time.Since(startTime3)
	fmt.Println("New Merkle root took", endTime3)

	for idx := range merkles {
		if merkles[idx] != nil && newmerkles[idx] == nil {
			panic(fmt.Sprintf("At idx %v No new merkle", idx))
		}
		if newmerkles[idx] != nil && merkles[idx] != nil {
			if !(merkles[idx].IsEqual(newmerkles[idx])) {
				panic(fmt.Sprintf("origh hash %v != %v", merkles[idx].String(), newmerkles[idx].String()))
			}
		}
	}

	return newmerkles
}

// ExtractWitnessCommitment attempts to locate, and return the witness
// commitment for a block. The witness commitment is of the form:
// SHA256(witness root || witness nonce). The function additionally returns a
// boolean indicating if the witness root was located within any of the txOut's
// in the passed transaction. The witness commitment is stored as the data push
// for an OP_RETURN with special magic bytes to aide in location.
func ExtractWitnessCommitment(tx *btcutil.Tx) ([]byte, bool) {
	// The witness commitment *must* be located within one of the coinbase
	// transaction's outputs.
	if !IsCoinBase(tx) {
		return nil, false
	}

	msgTx := tx.MsgTx()
	for i := len(msgTx.TxOut) - 1; i >= 0; i-- {
		// The public key script that contains the witness commitment
		// must shared a prefix with the WitnessMagicBytes, and be at
		// least 38 bytes.
		pkScript := msgTx.TxOut[i].PkScript
		if len(pkScript) >= CoinbaseWitnessPkScriptLength &&
			bytes.HasPrefix(pkScript, WitnessMagicBytes) {

			// The witness commitment itself is a 32-byte hash
			// directly after the WitnessMagicBytes. The remaining
			// bytes beyond the 38th byte currently have no consensus
			// meaning.
			start := len(WitnessMagicBytes)
			end := CoinbaseWitnessPkScriptLength
			return msgTx.TxOut[i].PkScript[start:end], true
		}
	}

	return nil, false
}

// ValidateWitnessCommitment validates the witness commitment (if any) found
// within the coinbase transaction of the passed block.
func ValidateWitnessCommitment(blk btcutil.Block) error {
	// If the block doesn't have any transactions at all, then we won't be
	// able to extract a commitment from the non-existent coinbase
	// transaction. So we exit early here.
	if len(blk.Transactions()) == 0 {
		str := "cannot validate witness commitment of block without " +
			"transactions"
		return ruleError(ErrNoTransactions, str)
	}

	coinbaseTx := blk.Transactions()[0]
	if len(coinbaseTx.MsgTx().TxIn) == 0 {
		return ruleError(ErrNoTxInputs, "transaction has no inputs")
	}

	witnessCommitment, witnessFound := ExtractWitnessCommitment(coinbaseTx)

	// If we can't find a witness commitment in any of the coinbase's
	// outputs, then the block MUST NOT contain any transactions with
	// witness data.
	if !witnessFound {
		for _, tx := range blk.Transactions() {
			msgTx := tx.MsgTx()
			if msgTx.HasWitness() {
				str := fmt.Sprintf("block contains transaction with witness" +
					" data, yet no witness commitment present")
				return ruleError(ErrUnexpectedWitness, str)
			}
		}
		return nil
	}

	// At this point the block contains a witness commitment, so the
	// coinbase transaction MUST have exactly one witness element within
	// its witness data and that element must be exactly
	// CoinbaseWitnessDataLen bytes.
	coinbaseWitness := coinbaseTx.MsgTx().TxIn[0].Witness
	if len(coinbaseWitness) != 1 {
		str := fmt.Sprintf("the coinbase transaction has %d items in "+
			"its witness stack when only one is allowed",
			len(coinbaseWitness))
		return ruleError(ErrInvalidWitnessCommitment, str)
	}
	witnessNonce := coinbaseWitness[0]
	if len(witnessNonce) != CoinbaseWitnessDataLen {
		str := fmt.Sprintf("the coinbase transaction witness nonce "+
			"has %d bytes when it must be %d bytes",
			len(witnessNonce), CoinbaseWitnessDataLen)
		return ruleError(ErrInvalidWitnessCommitment, str)
	}

	// Finally, with the preliminary checks out of the way, we can check if
	// the extracted witnessCommitment is equal to:
	// SHA256(witnessMerkleRoot || witnessNonce). Where witnessNonce is the
	// coinbase transaction's only witness item.
	witnessMerkleTree := BuildMerkleTreeStore(blk.Transactions(), true)
	witnessMerkleRoot := witnessMerkleTree[len(witnessMerkleTree)-1]

	var witnessPreimage [chainhash.HashSize * 2]byte
	copy(witnessPreimage[:], witnessMerkleRoot[:])
	copy(witnessPreimage[chainhash.HashSize:], witnessNonce)

	computedCommitment := chainhash.DoubleHashB(witnessPreimage[:])
	if !bytes.Equal(computedCommitment, witnessCommitment) {
		str := fmt.Sprintf("witness commitment does not match: "+
			"computed %v, coinbase includes %v", computedCommitment,
			witnessCommitment)
		return ruleError(ErrWitnessCommitmentMismatch, str)
	}

	return nil
}
