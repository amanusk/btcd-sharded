package blockchain

import (
	"encoding/gob"
	"fmt"
	"io"
	reallog "log"
	"net"
	_ "strconv"
	"sync"
)

type Coordinator struct {
	clients    map[*Client]bool
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
	handler    map[string]HandleFunc
	Connected  chan bool
	Listener   net.Listener
	Chain      *BlockChain

	m sync.RWMutex

	ClientsMutex sync.RWMutex
}

func NewCoordinator(listener net.Listener, blockchain *BlockChain) *Coordinator {
	manager := Coordinator{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan []byte),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		handler:    map[string]HandleFunc{},
		Connected:  make(chan bool),
		Chain:      blockchain,
		Listener:   listener,
	}
	return &manager
}

type HandleFunc func(conn net.Conn, manager *Coordinator)

type complexData struct {
	N int
	S string
	M map[string]int
	P []byte
	C *complexData
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

func (manager *Coordinator) Start() {
	manager.AddHandleFunc("STRING", handleStrings)
	manager.AddHandleFunc("GOB", handleGob)
	for {
		select {
		case connection := <-manager.register:
			manager.clients[connection] = true
			fmt.Println("Added new connection!")
			manager.Connected <- true
		case connection := <-manager.unregister:
			if _, ok := manager.clients[connection]; ok {
				close(connection.data)
				delete(manager.clients, connection)
				fmt.Println("A connection has terminated!")
			}
		// TODO Replace with fucntion
		case message := <-manager.broadcast:
			for connection := range manager.clients {
				select {
				case connection.data <- message:
				default:
					close(connection.data)
					delete(manager.clients, connection)
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

		reallog.Print("Receive command " + string(message))
		cmd := (string(message[:n]))
		reallog.Print(cmd)

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

func (manager *Coordinator) AddHandleFunc(name string, f HandleFunc) {
	manager.m.Lock()
	manager.handler[name] = f
	manager.m.Unlock()
}

func (manager *Coordinator) Receive(client *Client) {
	manager.HandleMessages(client.socket)
}

func handleStrings(conn net.Conn, manager *Coordinator) {
	reallog.Print("Receive STATUS REQUEST data:")
	var data stringData

	dec := gob.NewDecoder(conn)
	err := dec.Decode(&data)
	if err != nil {
		reallog.Println("Error decoding GOB data:", err)
		return
	}

	//reallog.Printf("Outer stringData struct: \n%#v\n", data)

	manager.broadcast <- []byte("STATUS")
}

func handleGob(conn net.Conn, manager *Coordinator) {
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

func (manager *Coordinator) Send(client *Client) {
	defer client.socket.Close()
	for {
		select {
		case message, ok := <-client.data:
			if !ok {
				return
			}
			client.socket.Write(message)
		}
	}

}
