package blockchain

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/pkg/errors"
	"io"
	reallog "log"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/btcsuite/btcd/wire"
)

type Client struct {
	Socket  net.Conn
	data    chan []byte
	handler map[string]ClientHandleFunc
	Index   *BlockIndex
	SqlDB   *SqlBlockDB
	m       sync.RWMutex
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

type ClientHandleFunc func(conn net.Conn)

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

func (client *Client) receive() {
	fmt.Printf("Client started recieving")
	//client.AddHandleFunc("STATUS", handleStatusRequest)
	//client.handleMessages(client.Socket)
	// NOTE: This should be replaced with various message handling
	for {
		reallog.Print("Receive GOB data:")
		var data TxGob

		dec := gob.NewDecoder(client.Socket)
		err := dec.Decode(&data)
		if err != nil {
			reallog.Println("Error decoding GOB data:", err)
			return
		}

		// Process the transactions
		for idx, txBytes := range data.TXs {
			var msgTx wire.MsgTx
			err = msgTx.Deserialize(bytes.NewReader(txBytes))
			if err != nil {
				reallog.Println("Error decoding TX data:", err)
			}
			fmt.Println(msgTx)
			// TODO the idx should be uniqe and passed as part of the GOB
			client.SqlDB.AddTX(data.BlockHash[:], idx, &msgTx)
		}

		// Sending a shardDone message to the coordinator
		message := "SHARDDONE"
		client.Socket.Write([]byte(message))

	}

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
		message := make([]byte, 4096)
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
			handleCommand(conn)
		}
	}
}

func (c *Client) StartClient() {
	go c.receive()
	for {
		fmt.Println("Wait for user input")
		reader := bufio.NewReader(os.Stdin)
		message, _ := reader.ReadString('\n')
		message = strings.TrimRight(message, "\n")
		fmt.Println("Message")
		if message == "STRING" {
			c.Socket.Write([]byte(message))
			c.sendStringGob()
		} else if message == "GOB" {
			c.Socket.Write([]byte(message))
			c.sendGob()
		}
	}
}
