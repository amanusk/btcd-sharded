package blockchain

import (
	_ "bufio"
	"encoding/gob"
	"fmt"
	_ "io"
	reallog "log"
	"net"
	_ "os"
	_ "strings"
	_ "sync"

	_ "github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	_ "github.com/davecgh/go-spew/spew"
)

type Shard struct {
	Socket          net.Conn
	shards          map[*Shard]bool // A map of shards connected to this shard
	terminate       chan bool
	registerShard   chan *Shard
	unregisterShard chan *Shard
	Port            int // Saves the port the shard is listening to other shards
	Index           *BlockIndex
	SqlDB           *SqlBlockDB
	ShardListener   net.Listener
}

// Creates a new shard connection for a coordintor to use.
// It has a connection and a channel to receive data from the server
func NewShardConnection(connection net.Conn, shardPort int) *Shard {
	shard := &Shard{
		Socket: connection, // The connection to the coordinator
		Port:   shardPort,  // Will store the prot the shard is listening to for other shards
	}
	return shard
}

// Creates a new shard for a coordinator to use.
// It has a connection and a channel to receive data from the server
func NewShard(shardListener net.Listener, connection net.Conn, index *BlockIndex, db *SqlBlockDB) *Shard {
	shard := &Shard{
		Index:           index,
		SqlDB:           db,
		registerShard:   make(chan *Shard),
		unregisterShard: make(chan *Shard),
		shards:          make(map[*Shard]bool),
		Socket:          connection,
		ShardListener:   shardListener,
	}
	return shard
}

// Handle instruction from coordinator to request the block
func (shard *Shard) handleRequestBlock(header *HeaderGob, conn net.Conn) {
	reallog.Println("Received request to request block")

	// Send a request for txs from other shards
	// Thes should have some logic, currently its 1:1
	for s, _ := range shard.shards {

		enc := gob.NewEncoder(s.Socket)

		headerToSend := Message{
			Cmd:  "SNDBLOCK",
			Data: header,
		}

		err := enc.Encode(headerToSend)
		if err != nil {
			reallog.Println(err, "Encode failed for struct: %#v", headerToSend)
		}

	}

}

// Handle request for a block shard by another shard
func (shard *Shard) handleSendBlock(header *HeaderGob, conn net.Conn) {
	reallog.Println("Received request to send a block shard")

	msgBlock := shard.SqlDB.FetchTXs(header.Header.BlockHash())
	msgBlock.Header = *header.Header

	// Create a gob of serialized msgBlock
	msg := Message{
		Cmd: "PRCBLOCK",
		Data: RawBlockGob{
			Block:  msgBlock,
			Flags:  BFNone,
			Height: header.Height,
		},
	}
	//Actually write the GOB on the socket
	enc := gob.NewEncoder(conn)
	err := enc.Encode(msg)
	if err != nil {
		reallog.Println(err, "Encode failed for struct: %#v", msg)
	}

}

// Receive a list of ip:port from coordinator, to which this shard will connect
func (shard *Shard) handleShardConnect(receivedShardAddresses *AddressesGob, conn net.Conn) {
	reallog.Println("Received request to connect to shards")

	for _, address := range receivedShardAddresses.Addresses {
		reallog.Println("Shard connecting to shard on " + address.String())
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
		reallog.Println(err, "Encode failed for struct: %#v", msg)
	}

}

func (shard *Shard) handleProcessBlock(receivedBlock *RawBlockGob, conn net.Conn) {
	reallog.Print("Received GOB data")

	msgBlockShard := receivedBlock.Block

	coordEnc := gob.NewEncoder(shard.Socket)

	msg := Message{
		Cmd: "SHARDDONE",
	}

	// If blockShard is empty (could happen), just send SHARDDONE
	if len(msgBlockShard.Transactions) == 0 {
		err := coordEnc.Encode(msg)
		if err != nil {
			reallog.Println(err, "Encode failed for struct: %#v", msg)
		}
		return
	}

	// Process the transactions
	// Create a new block node for the block and add it to the in-memory
	// TODO this creates a new block with mostly the same informtion,
	// TODO: Go over the transactions and save them accroding to the Index in msgBlockShard
	block := btcutil.NewBlock(wire.NewMsgBlockFromShard(msgBlockShard))

	// Store the txs in database, this could be postponed until after validation
	ShardStoreBlockShard(shard.SqlDB, msgBlockShard)

	blockNode := NewBlockNode(&msgBlockShard.Header, receivedBlock.Height)

	shard.ShardConnectBestChain(blockNode, block)

	reallog.Println("Done processing block, sending SHARDDONE")

	// Send conformation to coordinator!
	err := coordEnc.Encode(msg)
	if err != nil {
		reallog.Println(err, "Encode failed for struct: %#v", msg)
	}

}

func (shard *Shard) receive() {
	fmt.Println("Shard started recieving")

	shard.handleSmartMessages(shard.Socket)

}

func (shard *Shard) handleSmartMessages(conn net.Conn) {

	gob.Register(Complex{})
	gob.Register(RawBlockGob{})
	gob.Register(HeaderGob{})
	gob.Register(AddressesGob{})

	for {
		dec := gob.NewDecoder(conn)
		var msg Message
		err := dec.Decode(&msg)
		if err != nil {
			reallog.Println("Error decoding GOB data:", err)
			return
		}
		cmd := msg.Cmd
		reallog.Println("Got cmd ", cmd)

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
		case "SNDBLOCK":
			header := msg.Data.(HeaderGob)
			shard.handleSendBlock(&header, conn)
		default:
			reallog.Println("Command '", cmd, "' is not registered.")
		}

	}
}

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
	<-shard.terminate
}

// Recored a connection to another shard
func (shard *Shard) RegisterShard(s *Shard) {
	shard.registerShard <- s
}

// Receive messages from other shards
func (shard *Shard) ReceiveShard(s *Shard) {
	//shard.handleMessages(s.Socket)
	shard.handleSmartMessages(s.Socket)
}
