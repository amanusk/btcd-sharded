package blockchain

import (
	"encoding/gob"
	"fmt"
	logging "log"
	"net"

	"github.com/btcsuite/btcd/database"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/btcsuite/btcutil/bloom"
)

// Shard is a single node in a sharded bitcoin client cluster
type Shard struct {
	Socket                 net.Conn
	shards                 map[*Shard]bool // A map of shards connected to this shard
	terminate              chan bool
	registerShard          chan *Shard
	unregisterShard        chan *Shard
	Port                   int // Saves the port the shard is listening to other shards
	Index                  *BlockIndex
	DB                     database.DB
	ShardListener          net.Listener
	intersectFilter        chan *FilterGob
	missingTxOuts          chan (map[wire.OutPoint]*UtxoEntry)
	ReceivedCombinedFilter *FilterGob
}

// NewShardConnection Creates a new shard connection for a coordinator to use.
// It has a connection and a channel to receive data from the server
func NewShardConnection(connection net.Conn, shardPort int) *Shard {
	shard := &Shard{
		Socket: connection, // The connection to the coordinator
		Port:   shardPort,  // Will store the prot the shard is listening to for other shards
	}
	return shard
}

// NewShard Creates a new shard for a coordinator to use.
// It has a connection and a channel to receive data from the server
func NewShard(shardListener net.Listener, connection net.Conn, index *BlockIndex, db database.DB) *Shard {
	shard := &Shard{
		Index:           index,
		DB:              db,
		registerShard:   make(chan *Shard),
		unregisterShard: make(chan *Shard),
		shards:          make(map[*Shard]bool),
		intersectFilter: make(chan *FilterGob),
		missingTxOuts:   make(chan map[wire.OutPoint]*UtxoEntry),
		Socket:          connection,
		ShardListener:   shardListener,
	}
	return shard
}

// Handle instruction from coordinator to request the block
func (shard *Shard) handleRequestBlock(header *HeaderGob, conn net.Conn) {
	logging.Println("Received request to request block")

	// Send a request for txs from other shards
	// Thes should have some logic, currently its 1:1
	for s := range shard.shards {

		enc := gob.NewEncoder(s.Socket)

		headerToSend := Message{
			Cmd:  "SNDBLOCK",
			Data: header,
		}

		err := enc.Encode(headerToSend)
		if err != nil {
			logging.Println(err, "Encode failed for struct: %#v", headerToSend)
		}

	}

}

// Handle the received combined filter and missing Txs from coordinator
func (shard *Shard) handleFilterCombined(filterData *FilterGob, conn net.Conn) {
	logging.Println("Recived filter", filterData)
	shard.intersectFilter <- filterData

}

// Handle request for a block shard by another shard
//func (shard *Shard) handleSendBlock(header *HeaderGob, conn net.Conn) {
//	logging.Println("Received request to send a block shard")
//
//	msgBlock := shard.SQLDB.FetchTXs(header.Header.BlockHash())
//	msgBlock.Header = *header.Header
//
//	// Create a gob of serialized msgBlock
//	msg := Message{
//		Cmd: "PRCBLOCK",
//		Data: RawBlockGob{
//			Block:  msgBlock,
//			Flags:  BFNone,
//			Height: header.Height,
//		},
//	}
//	//Actually write the GOB on the socket
//	enc := gob.NewEncoder(conn)
//	err := enc.Encode(msg)
//	if err != nil {
//		logging.Println(err, "Encode failed for struct: %#v", msg)
//	}
//
//}
//
// Receive a list of ip:port from coordinator, to which this shard will connect
func (shard *Shard) handleShardConnect(receivedShardAddresses *AddressesGob, conn net.Conn) {
	logging.Println("Received request to connect to shards")

	for _, address := range receivedShardAddresses.Addresses {
		logging.Println("Shard connecting to shard on " + address.String())
		connection, err := net.Dial("tcp", address.String())
		if err != nil {
			fmt.Println(err)
		}

		shardConn := NewShardConnection(connection, address.Port)
		shard.RegisterShard(shardConn)
		go shard.ReceiveShard(shardConn)
	}
	// Send connection done to coordinator
	enc := gob.NewEncoder(conn)
	msg := Message{
		Cmd: "CONCTDONE",
	}
	err := enc.Encode(msg)
	if err != nil {
		logging.Println(err, "Encode failed for struct: %#v", msg)
	}

}

func (shard *Shard) handleProcessBlock(receivedBlock *RawBlockGob, conn net.Conn) {
	logging.Print("Received GOB data")

	msgBlockShard := receivedBlock.Block

	coordEnc := gob.NewEncoder(shard.Socket)

	doneMsg := Message{
		Cmd: "SHARDDONE",
	}

	// If blockShard is empty (could happen), just send SHARDDONE
	if len(msgBlockShard.Transactions) == 0 {
		shard.SendEmptyBloomFilter()
		return
	}

	// Process the transactions
	// Create a new block node for the block and add it to the in-memory
	block := btcutil.NewBlockShard(msgBlockShard)

	// Store the txs in database, this could be postponed until after validation
	//StoreBlockShard(shard.SQLDB, msgBlockShard)

	// shard does not need the parent node
	blockNode := NewBlockNode(&msgBlockShard.Header, nil)
	// Set node height to block height according to coordinator
	blockNode.height = receivedBlock.Height

	_, err := shard.ShardConnectBestChain(blockNode, block)
	if err != nil {
		logging.Println(err)
		shard.NotifyOfBadBlock()
		// return!
		return
	}

	logging.Println("Done processing block, sending SHARDDONE")

	// Send conformation to coordinator!
	err = coordEnc.Encode(doneMsg)
	if err != nil {
		logging.Println(err, "Encode failed for struct: %#v", doneMsg)
	}

}

func (shard *Shard) receive() {
	fmt.Println("Shard started recieving")

	shard.handleSmartMessages(shard.Socket)

}

func (shard *Shard) handleSmartMessages(conn net.Conn) {

	gob.Register(RawBlockGob{})
	gob.Register(HeaderGob{})
	gob.Register(AddressesGob{})
	gob.Register(FilterGob{})
	gob.Register(MatchingMissingTxOutsGob{})

	for {
		dec := gob.NewDecoder(conn)
		var msg Message
		err := dec.Decode(&msg)
		if err != nil {
			logging.Println("Error decoding GOB data:", err)
			return
		}
		cmd := msg.Cmd
		logging.Println("Got cmd ", cmd)

		// handle according to received command
		switch cmd {
		case "PRCBLOCK":
			block := msg.Data.(RawBlockGob)
			shard.handleProcessBlock(&block, conn)
		case "SHARDCON":
			addresses := msg.Data.(AddressesGob)
			shard.handleShardConnect(&addresses, conn)
		// handle an instruction from coordinator to request a block
		case "REQBLOCK":
			header := msg.Data.(HeaderGob)
			shard.handleRequestBlock(&header, conn)
		// handle a request for block shard coming from another shard
		//case "SNDBLOCK":
		//	header := msg.Data.(HeaderGob)
		//	shard.handleSendBlock(&header, conn)
		// Receive a combined bloom filter from coordinator
		case "BLOOMCOM":
			filter := msg.Data.(FilterGob)
			shard.handleFilterCombined(&filter, conn)
		// Get a message from coordinator that block is bad, drop process
		// of current block
		case "BADBLOCK":
			shard.handleBadBlock(conn)
		case "MISSOUTS":
			missingTxOutsGob := msg.Data.(MatchingMissingTxOutsGob)
			shard.handleMissingTxOuts(conn, missingTxOutsGob.MissingTxOuts)
		default:
			logging.Println("Command '", cmd, "' is not registered.")
		}

	}
}

// StartShard starts the shard, the shard starts receiveing on its listening port
func (shard *Shard) StartShard() {
	// Receive messages from coordinator
	go shard.receive()
	for {
		select {
		// Handle shard connect/disconnect
		case connection := <-shard.registerShard:
			shard.shards[connection] = true
			fmt.Println("Added new shard to shard!")
		case connection := <-shard.unregisterShard:
			if _, ok := shard.shards[connection]; ok {
				delete(shard.shards, connection)
				fmt.Println("A connection has terminated!")
			}
		}
	}
}

// RegisterShard Recoreds a connection to another shard
func (shard *Shard) RegisterShard(s *Shard) {
	shard.registerShard <- s
}

// ReceiveShard is respoinsible for receiving messages from other shards
func (shard *Shard) ReceiveShard(s *Shard) {
	//shard.handleMessages(s.Socket)
	shard.handleSmartMessages(s.Socket)
}

// SendEmptyBloomFilter sends an empty bloomfilter to the coordintor. This
// is an indication that the shard has no transactions to process, and
// is the same as SHARDDONE
func (shard *Shard) SendEmptyBloomFilter() {
	logging.Println("Sending Empty bloomfilter to coordinator")
	enc := gob.NewEncoder(shard.Socket)

	err := enc.Encode(
		Message{
			Cmd: "BLOOMFLT",
			Data: FilterGob{
				InputFilter: nil,
				TxFilter:    nil,
			},
		})
	if err != nil {
		logging.Println(err, "Encode failed for empty filter")
	}
}

// SendInputBloomFilter sends a bloom filter of transaction inputs to the
// coordinator and waits for either conformation for that no intersection is
// possible or request for additional information
// In addtion, a filter of all transaction hashes is sent
// TODO: change list to map[]struct{}
func (shard *Shard) SendInputBloomFilter(inputFilter *bloom.Filter, txFilter *bloom.Filter, missingTxOutsList *[]*wire.OutPoint) {
	logging.Println("Sending bloom filter of transactions to coordinator")
	enc := gob.NewEncoder(shard.Socket)

	// Print all missing inputs
	logging.Println("Sending missing Txs:")
	for _, input := range *missingTxOutsList {
		logging.Println(input)
	}

	err := enc.Encode(
		Message{
			Cmd: "BLOOMFLT",
			Data: FilterGob{
				InputFilter:   inputFilter.MsgFilterLoad(),
				TxFilter:      txFilter.MsgFilterLoad(),
				MissingTxOuts: *missingTxOutsList,
			},
		})
	if err != nil {
		logging.Println(err, "Encode failed for struct: %#v", inputFilter)
	}

	// Wait for intersectFilter from coordinator
	shard.ReceivedCombinedFilter = <-shard.intersectFilter
	logging.Println("Received combined filter")
	return
}

// SendMatchingAndMissingTxOuts sends a list of matching transaction inputs to the coordinator
// This list could be empty,
func (shard *Shard) SendMatchingAndMissingTxOuts(matchingTxOuts []*wire.OutPoint, missingTxOuts map[wire.OutPoint]*UtxoEntry) map[wire.OutPoint]*UtxoEntry {
	logging.Println("Sending bloom filter of transactions to coordinator")
	enc := gob.NewEncoder(shard.Socket)

	err := enc.Encode(
		Message{
			Cmd: "MATCHTXS",
			Data: MatchingMissingTxOutsGob{
				MatchingTxOuts: matchingTxOuts,
				MissingTxOuts:  missingTxOuts,
			},
		})
	if err != nil {
		logging.Println(err, "Encode failed for struct: %#v", matchingTxOuts)
	}

	receivedMissingTxOuts := <-shard.missingTxOuts
	logging.Println("Received list of missing TxOuts from coordinator")
	return receivedMissingTxOuts
}

// NotifyOfBadBlock sends a message to the coordinator that the block is illegal
// for some reason, the block will not be connected
func (shard *Shard) NotifyOfBadBlock() {
	logging.Println("Illegal block, send BADBLOCK to coordinator")
	enc := gob.NewEncoder(shard.Socket)

	err := enc.Encode(
		Message{
			Cmd: "BADBLOCK",
		})
	if err != nil {
		logging.Println(err, "Encode failed for struct BADBLOCK")
	}

}

// handleBadBlock receives a notice of a bad block, needs to stop processing
// the current block
func (shard *Shard) handleBadBlock(conn net.Conn) {
	logging.Println("Received bad block indication form coordinator")
	// TODO: we need to add some rules, in case this is received after,
	// we need to revert the changes made by this shard

	// Release the no colide lock if the block is bad
	shard.missingTxOuts <- make(map[wire.OutPoint]*UtxoEntry)
}

// handleMissingTxOuts receives a map of the missing Txs
func (shard *Shard) handleMissingTxOuts(conn net.Conn, missingTxOuts map[wire.OutPoint]*UtxoEntry) {
	logging.Println("Received map of missing TxOuts")
	shard.missingTxOuts <- missingTxOuts

}
