package blockchain

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"github.com/pkg/errors"
	"io"
	reallog "log"
	"net"
	"os"
	"strings"
	"sync"
)

type Client struct {
	socket  net.Conn
	data    chan []byte
	handler map[string]ClientHandleFunc
	m       sync.RWMutex
}

func NewClient(connection net.Conn) *Client {
	client := &Client{
		socket: connection,
		data:   make(chan []byte),
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
	client.AddHandleFunc("STATUS", handleStatusRequest)
	client.handleMessages(client.socket)
	//defer os.Exit(0)
	//for {
	//	message := make([]byte, 4096)
	//	length, err := client.socket.Read(message)
	//	if err != nil {
	//		client.socket.Close()
	//		break
	//	}
	//	// Here the client needs to send back his state
	//	if length > 0 {
	//		fmt.Println("RECEIVED: " + string(message))
	//	}
	//}
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
	enc := gob.NewEncoder(client.socket)
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
	enc := gob.NewEncoder(client.socket)
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

func StartClientMode() {

	fmt.Println("Starting client...")
	connection, error := net.Dial("tcp", "localhost:12345")
	if error != nil {
		fmt.Println(error)
	}
	client := &Client{
		socket:  connection,
		handler: map[string]ClientHandleFunc{},
	}
	go client.receive()
	for {
		reader := bufio.NewReader(os.Stdin)
		message, _ := reader.ReadString('\n')
		message = strings.TrimRight(message, "\n")
		if message == "STRING" {
			connection.Write([]byte(message))
			client.sendStringGob()
		} else if message == "GOB" {
			connection.Write([]byte(message))
			client.sendGob()
		}
	}
}
