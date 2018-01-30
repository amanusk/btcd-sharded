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
	"sync"

	_ "github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	_ "github.com/davecgh/go-spew/spew"
)

type Client struct {
	Socket    net.Conn
	data      chan []byte
	handler   map[string]ClientHandleFunc
	terminate chan bool
	Index     *BlockIndex
	SqlDB     *SqlBlockDB
	m         sync.RWMutex
}

// Creates a new client connection for a coordintor to use.
// It has a connection and a channel to receive data from the server
func NewClientConnection(connection net.Conn) *Client {
	client := &Client{
		Socket: connection,
		data:   make(chan []byte),
	}
	return client
}

// Creates a new client for a coordintor to use.
// It has a connection and a channel to receive data from the server
func NewClient(connection net.Conn, index *BlockIndex, db *SqlBlockDB) *Client {
	client := &Client{
		Index:   index,
		SqlDB:   db,
		handler: map[string]ClientHandleFunc{},
		Socket:  connection,
		data:    make(chan []byte),
	}
	return client
}

func (client *Client) AddHandleFunc(name string, f ClientHandleFunc) {
	client.m.Lock()
	client.handler[name] = f
	client.m.Unlock()
}

type ClientHandleFunc func(conn net.Conn, client *Client)

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

func handleBlockGob(conn net.Conn, client *Client) {
	reallog.Print("Receive GOB data:")

	var recievedBlock BlockGob

	dec := gob.NewDecoder(client.Socket)
	err := dec.Decode(&recievedBlock)
	if err != nil {
		reallog.Println("Error decoding GOB data:", err)
		return
	}

	var msgBlockShard wire.MsgBlockShard
	rbuf := bytes.NewReader(recievedBlock.Block)
	err = msgBlockShard.Deserialize(rbuf)
	if err != nil {
		reallog.Println("Error decoding GOB data:", err)
		return
	} else {
		//fmt.Printf("%s ", spew.Sdump(&block))
	}
	// TODO this should all be in seperate functions!

	// Process the transactions
	// Create a new block node for the block and add it to the in-memory
	// TODO this creates a new block with mostly the same informtion,
	// This needs to be optimized
	block := btcutil.NewBlock(wire.NewMsgBlockFromShard(&msgBlockShard))

	ShardConnectBestChain(client.SqlDB, block)

	// Sending a shardDone message to the coordinator
	message := "SHARDDONE"
	client.Socket.Write([]byte(message))

}

func (client *Client) receive() {
	fmt.Printf("Client started recieving")
	client.AddHandleFunc("BLOCKGOB", handleBlockGob)
	client.handleMessages(client.Socket)

}

func (client *Client) sendGob() error {
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
	enc := gob.NewEncoder(client.Socket)
	err := enc.Encode(testStruct)
	if err != nil {
		return errors.Wrapf(err, "Encode failed for struct: %#v", testStruct)
	}
	return nil
}

func (client *Client) sendStringGob() error {
	testStruct := stringData{
		S: "string data",
	}

	reallog.Println("Send a struct as GOB:")
	reallog.Printf("Outer stringData struct: \n%#v\n", testStruct)
	enc := gob.NewEncoder(client.Socket)
	err := enc.Encode(testStruct)
	if err != nil {
		return errors.Wrapf(err, "Encode failed for struct: %#v", testStruct)
	}
	return nil
}

func (client *Client) handleMessages(conn net.Conn) {
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

		client.m.RLock()
		handleCommand, ok := client.handler[cmd]
		client.m.RUnlock()
		reallog.Println(handleCommand)

		if !ok {
			reallog.Println("Command '", cmd, "' is not registered.")
		} else {
			handleCommand(conn, client)
		}
	}
}

func (client *Client) StartClient() {
	go client.receive()
	<-client.terminate
}
