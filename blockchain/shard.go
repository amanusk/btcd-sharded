package blockchain

import (
	_ "bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
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
	handler         map[string]ShardHandleFunc
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
		handler:         map[string]ShardHandleFunc{},
		registerShard:   make(chan *Shard),
		unregisterShard: make(chan *Shard),
		shards:          make(map[*Shard]bool),
		Socket:          connection,
		ShardListener:   shardListener,
	}
	return shard
}

type ShardHandleFunc func(conn net.Conn, shard *Shard)

// Handle instruction from coordinator to request the block
func handleRequestBlock(conn net.Conn, shard *Shard) {
	reallog.Println("Received request to request block")

	var header HeaderGob

	dec := gob.NewDecoder(conn)
	err := dec.Decode(&header)
	if err != nil {
		reallog.Println("Error decoding GOB data:", err)
		return
	}
	// Send a request for txs from other shards
	// Thes should have some logic, currently its 1:1
	for s, _ := range shard.shards {
		// Send a request to the shard to send back transactions for this header
		s.Socket.Write([]byte("SNDBLOCK"))

		enc := gob.NewEncoder(s.Socket)
		// Generate a header gob to send to coordinator
		headerToSend := HeaderGob{
			Header: header.Header,
			Flags:  BFNone,
		}
		err = enc.Encode(headerToSend)
		if err != nil {
			reallog.Println(err, "Encode failed for struct: %#v", headerToSend)
		}

	}

}

// Handle request for a block shard by another shard
func handleSendBlock(conn net.Conn, shard *Shard) {
	reallog.Println("Received request to send a block shard")

	var header HeaderGob

	dec := gob.NewDecoder(conn)
	err := dec.Decode(&header)
	if err != nil {
		reallog.Println("Error decoding GOB data:", err)
		return
	}

	msgBlock := shard.SqlDB.FetchTXs(header.Header.BlockHash())

	var bb bytes.Buffer
	msgBlock.Serialize(&bb)

	// Create a gob of serialized msgBlock
	blockToSend := BlockGob{
		Block: bb.Bytes(),
	}

	conn.Write([]byte("BLOCKGOB"))

	//Actually write the GOB on the socket
	enc := gob.NewEncoder(conn)
	err = enc.Encode(blockToSend)
	if err != nil {
		reallog.Println(err, "Encode failed for struct: %#v", blockToSend)
	}

}

// Receive a list of ip:port from coordinator, to which this shard will connect
func handleShardConnect(conn net.Conn, shard *Shard) {
	reallog.Println("Received request to connect to shards")

	var receivedShardAddresses AddressesGob

	dec := gob.NewDecoder(conn)
	err := dec.Decode(&receivedShardAddresses)
	if err != nil {
		reallog.Println("Error decoding GOB data:", err)
		return
	}

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

}

func handleBlockGob(conn net.Conn, shard *Shard) {
	reallog.Print("Received GOB data")

	var receivedBlock BlockGob
	// Gen information from shard!
	dec := gob.NewDecoder(conn)
	err := dec.Decode(&receivedBlock)
	if err != nil {
		reallog.Println("Error decoding GOB data:", err)
		return
	}

	var msgBlockShard wire.MsgBlockShard
	rbuf := bytes.NewReader(receivedBlock.Block)
	err = msgBlockShard.Deserialize(rbuf)
	if err != nil {
		reallog.Println("Error decoding GOB data:", err)
		return
	} else {
		//fmt.Printf("%s ", spew.Sdump(&msgBlockShard))
	}

	// Process the transactions
	// Create a new block node for the block and add it to the in-memory
	// TODO this creates a new block with mostly the same informtion,
	// This needs to be optimized
	block := btcutil.NewBlock(wire.NewMsgBlockFromShard(&msgBlockShard))

	ShardStoreBlockShard(shard.SqlDB, &msgBlockShard)

	ShardConnectBestChain(shard.SqlDB, block)

	reallog.Println("Done processing block, sending SHARDONE")

	// Sending a shardDone message to the coordinator
	message := "SHARDDONE"
	// Send conformation to coordinator!
	shard.Socket.Write([]byte(message))

}

func handleBlockDad(conn net.Conn, shard *Shard) {
	reallog.Print("Received DAD data")
	// Sending a shardDone message to the coordinator
	message := "SHARDDONE"
	shard.Socket.Write([]byte(message))

}

func (shard *Shard) receive() {
	fmt.Println("Shard started recieving")

	shard.handleMessages(shard.Socket)

}

func (shard *Shard) handleMessages(conn net.Conn) {
	for {
		message := make([]byte, 8)
		n, err := conn.Read(message)
		switch {
		case err == io.EOF:
			reallog.Println("Reached EOF - close this connection.\n   ---")
			return
		}

		reallog.Print("Receive command " + string(message))
		cmd := (string(message[:n]))
		reallog.Print(cmd)

		// handle according to received command
		switch cmd {
		case "BLOCKGOB":
			handleBlockGob(conn, shard)
		case "BLOCKDAD":
			handleBlockDad(conn, shard)
		case "SHARDCON":
			handleShardConnect(conn, shard)
		// handle an instruction from coordinator to request a block
		case "REQBLOCK":
			handleRequestBlock(conn, shard)
		// handle a request for block shard coming from another shard
		case "SNDBLOCK":
			handleSendBlock(conn, shard)
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
	shard.handleMessages(s.Socket)
}
