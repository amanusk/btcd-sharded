package blockchain

import (
	"encoding/gob"
	"fmt"
	"io"
	reallog "log"
	"net"
	_ "strconv"
	"sync"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
)

type Coordinator struct {
	clients       map[*Client]bool
	broadcast     chan []byte
	register      chan *Client
	unregister    chan *Client
	handler       map[string]HandleFunc
	allShardsDone chan bool
	Connected     chan bool // Sends a sigal that a client connection completed
	Listener      net.Listener
	Chain         *BlockChain

	m sync.RWMutex

	ClientsMutex sync.RWMutex
}

// Cerates and returns a new coordinator
func NewCoordinator(listener net.Listener, blockchain *BlockChain) *Coordinator {
	coord := Coordinator{
		clients:       make(map[*Client]bool),
		broadcast:     make(chan []byte),
		register:      make(chan *Client),
		unregister:    make(chan *Client),
		allShardsDone: make(chan bool),
		handler:       map[string]HandleFunc{},
		Connected:     make(chan bool),
		Chain:         blockchain,
		Listener:      listener,
	}
	return &coord
}

type HandleFunc func(conn net.Conn, coord *Coordinator)

type complexData struct {
	N int
	S string
	M map[string]int
	P []byte
	C *complexData
}

type TxGob struct {
	BlockHash chainhash.Hash
	TXs       [][]byte
}

type stringData struct {
	S string
}

func (c *Coordinator) GetNumClientes() int {
	return len(c.clients)
}

func (c *Coordinator) Register(client *Client) {
	c.register <- client
}

func (coord *Coordinator) Start() {
	coord.AddHandleFunc("STRING", handleStrings)
	coord.AddHandleFunc("GOB", handleGob)
	coord.AddHandleFunc("SHARDDONE", handleBlockCheck)
	for {
		select {
		case connection := <-coord.register:
			coord.clients[connection] = true
			fmt.Println("Added new connection!")
			coord.Connected <- true
		case connection := <-coord.unregister:
			if _, ok := coord.clients[connection]; ok {
				close(connection.data)
				delete(coord.clients, connection)
				fmt.Println("A connection has terminated!")
			}
		// TODO Replace with fucntion
		case message := <-coord.broadcast:
			for connection := range coord.clients {
				select {
				case connection.data <- message:
				default:
					close(connection.data)
					delete(coord.clients, connection)
				}
			}
		}
	}
}

// This function receives a connection. The connection expects to get a message
// with inoformation on what is the request from the server. The server will
// handle the message according to the type of request it receives
func (coord *Coordinator) HandleMessages(conn net.Conn) {

	for {
		message := make([]byte, 4096)
		n, err := conn.Read(message)
		switch {
		case err == io.EOF:
			reallog.Println("Reached EOF - close this connection.\n   ---")
			return
		}

		cmd := (string(message[:n]))
		reallog.Print("Recived command", cmd)

		coord.m.RLock()
		handleCommand, ok := coord.handler[cmd]
		coord.m.RUnlock()
		reallog.Println(handleCommand)

		if !ok {
			reallog.Println("Command '", cmd, "' is not registered.")
		} else {
			handleCommand(conn, coord)
		}
	}
}

func (coord *Coordinator) AddHandleFunc(name string, f HandleFunc) {
	coord.m.Lock()
	coord.handler[name] = f
	coord.m.Unlock()
}

func (coord *Coordinator) Receive(client *Client) {
	coord.HandleMessages(client.Socket)
}

func handleStrings(conn net.Conn, coord *Coordinator) {
	reallog.Print("Receive STATUS REQUEST data:")
	var data stringData

	dec := gob.NewDecoder(conn)
	err := dec.Decode(&data)
	if err != nil {
		reallog.Println("Error decoding GOB data:", err)
		return
	}

	//reallog.Printf("Outer stringData struct: \n%#v\n", data)

	coord.broadcast <- []byte("STATUS")
}

func handleGob(conn net.Conn, coord *Coordinator) {
	reallog.Print("Receive GOB data:")
	var data complexData

	dec := gob.NewDecoder(conn)
	err := dec.Decode(&data)
	if err != nil {
		reallog.Println("Error decoding GOB data:", err)
		return
	}

	reallog.Printf("Outer complexData struct: \n%#v\n", data)
	reallog.Printf("Inner complexData struct: \n%#v\n", data.C)
}

func handleBlockCheck(conn net.Conn, coord *Coordinator) {
	reallog.Print("Receive Block Confirmation")
	coord.allShardsDone <- true

}

func (coord *Coordinator) Send(client *Client) {
	defer client.Socket.Close()
	for {
		select {
		case message, ok := <-client.data:
			if !ok {
				return
			}
			client.Socket.Write(message)
		}
	}

}

// Returns all the clients in the coordinator clients maps
func (coord *Coordinator) GetClients() []*Client {
	clients := make([]*Client, 0, len(coord.clients))

	for key, _ := range coord.clients {
		clients = append(clients, key)
	}
	return clients

}

// Process block will make a sanity check on the block header and will wait for confirmations from all the shards
// that the block has been processed
func (coord *Coordinator) ProcessBlock(header *wire.BlockHeader, flags BehaviorFlags) error {
	err := CheckBlockHeaderSanity(header, coord.Chain.GetChainParams().PowLimit, coord.Chain.GetTimeSource(), flags)
	if err != nil {
		return err
	}
	// Wait for all the shards to send finish report
	for i := 0; i < len(coord.clients); i++ {
		<-coord.allShardsDone
	}
	return nil
}
