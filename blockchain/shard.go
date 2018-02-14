package blockchain

import (
	_ "bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/pkg/errors"
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
	data            chan []byte
	handler         map[string]ShardHandleFunc
	terminate       chan bool
	registerShard   chan *Shard
	unregisterShard chan *Shard
	Index           *BlockIndex
	SqlDB           *SqlBlockDB
	ShardListener   net.Listener
}

// Creates a new shard connection for a coordintor to use.
// It has a connection and a channel to receive data from the server
func NewShardConnection(connection net.Conn) *Shard {
	shard := &Shard{
		Socket: connection,
		data:   make(chan []byte),
	}
	return shard
}

// Creates a new shard for a coordintor to use.
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
		data:            make(chan []byte),
		ShardListener:   shardListener,
	}
	return shard
}

type ShardHandleFunc func(conn net.Conn, shard *Shard)

func handleStatusRequest(conn net.Conn) {
	reallog.Print("Receive request for status:")
	//var data stringData

	//dec := gob.NewDecoder(conn)
	//err := dec.Decode(&data)
	//if err != nil {
	//	reallog.Println("Error decoding GOB data:", err)
	//	return
	//}

	//reallog.Printf("Outer stringData struct: \n%#v\n", data)

	//manager.broadcast <- []byte(data.S)
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

	ShardConnectBestChain(shard.SqlDB, block)

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
	fmt.Printf("Shard started recieving")

	shard.handleMessages(shard.Socket)

}

func (shard *Shard) sendGob() error {
	testStruct := complexData{
		N: 23,
		S: "string data",
		M: map[string]int{"one": 1, "two": 2, "three": 3},
		P: []byte("abc"),
		C: &complexData{
			N: 256,
			S: "Recursive structs? Piece of cake!",
			M: map[string]int{"01": 1, "10": 2, "11": 3},
		},
	}

	reallog.Println("Send a struct as GOB:")
	reallog.Printf("Outer complexData struct: \n%#v\n", testStruct)
	reallog.Printf("Inner complexData struct: \n%#v\n", testStruct.C)
	enc := gob.NewEncoder(shard.Socket)
	err := enc.Encode(testStruct)
	if err != nil {
		return errors.Wrapf(err, "Encode failed for struct: %#v", testStruct)
	}
	return nil
}

func (shard *Shard) sendStringGob() error {
	testStruct := stringData{
		S: "string data",
	}

	reallog.Println("Send a struct as GOB:")
	reallog.Printf("Outer stringData struct: \n%#v\n", testStruct)
	enc := gob.NewEncoder(shard.Socket)
	err := enc.Encode(testStruct)
	if err != nil {
		return errors.Wrapf(err, "Encode failed for struct: %#v", testStruct)
	}
	return nil
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
				close(connection.data)
				delete(shard.shards, connection)
				fmt.Println("A connection has terminated!")
			}
		}
	}
	<-shard.terminate
}

// Recored a connection to another shard
func (s *Shard) RegisterShard(shard *Shard) {
	s.registerShard <- shard
}

// Receive messages from other shards
func (s *Shard) ReceiveShard(shard *Shard) {
	s.handleMessages(shard.Socket)
}
