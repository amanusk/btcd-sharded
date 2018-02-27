package blockchain

import (
	"encoding/gob"
	"fmt"
	"io"
	reallog "log"
	"net"
	_ "strconv"
	_ "sync"

	"github.com/btcsuite/btcd/wire"
)

type Coordinator struct {
	Socket          net.Conn              // Receive information from other coordinators
	shards          map[*Shard]bool       // A map of shards connected to this coordinator
	coords          map[*Coordinator]bool // A map of coords connected to this coordinator
	broadcast       chan []byte           // A channel to broadcast to shards
	registerShard   chan *Shard
	unregisterShard chan *Shard
	registerCoord   chan *Coordinator
	unregisterCoord chan *Coordinator
	handler         map[string]HandleFunc
	allShardsDone   chan bool
	Connected       chan bool // Sends a sigal that a shard connection completed
	KeepAlive       chan interface{}
	ShardListener   net.Listener
	CoordListener   net.Listener
	Chain           *BlockChain
}

// Creates a new shard connection for a coordintor to use.
// It has a connection and a channel to receive data from the server
func NewCoordConnection(connection net.Conn) *Coordinator {
	coord := &Coordinator{
		Socket: connection, // A connection to the connected peer
	}
	return coord
}

// Cerates and returns a new coordinator
func NewCoordinator(shardListener net.Listener, coordListener net.Listener, blockchain *BlockChain) *Coordinator {
	coord := Coordinator{
		shards:          make(map[*Shard]bool),
		coords:          make(map[*Coordinator]bool),
		broadcast:       make(chan []byte),
		registerShard:   make(chan *Shard),
		unregisterShard: make(chan *Shard),
		registerCoord:   make(chan *Coordinator),
		unregisterCoord: make(chan *Coordinator),
		allShardsDone:   make(chan bool),
		handler:         map[string]HandleFunc{},
		Connected:       make(chan bool),
		Chain:           blockchain,
		ShardListener:   shardListener,
		CoordListener:   coordListener,
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

type BlockGob struct {
	Block []byte
}

// A struct to send a list of tcp connections
type AddressesGob struct {
	Addresses []*net.TCPAddr
}

type HeaderGob struct {
	Header       *wire.BlockHeader
	Flags        BehaviorFlags
	ActiveShards int
}

type stringData struct {
	S string
}

func (c *Coordinator) GetNumShardes() int {
	return len(c.shards)
}

func (c *Coordinator) RegisterShard(shard *Shard) {
	c.registerShard <- shard
}

func (c *Coordinator) RegisterCoord(coord *Coordinator) {
	c.registerCoord <- coord
}

func (coord *Coordinator) Start() {
	for {
		select {
		// Handle shard connect/disconnect
		case connection := <-coord.registerShard:
			coord.shards[connection] = true
			fmt.Println("Added new shard!")
			coord.Connected <- true
		case connection := <-coord.unregisterShard:
			if _, ok := coord.shards[connection]; ok {
				close(connection.data)
				delete(coord.shards, connection)
				fmt.Println("A connection has terminated!")
			}

		// Register a new connected coordinator
		case connection := <-coord.registerCoord:
			coord.coords[connection] = true
			fmt.Println("Added new peer!")
		// TODO Replace with fucntion this is how we broadcast messages
		case message := <-coord.broadcast:
			for connection := range coord.shards {
				select {
				case connection.data <- message:
				default:
					close(connection.data)
					delete(coord.shards, connection)
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
		message := make([]byte, 9)
		n, err := conn.Read(message)
		switch {
		case err == io.EOF:
			reallog.Println("Reached EOF - close this connection.\n   ---")
			return
		}

		cmd := (string(message[:n]))
		reallog.Print("Recived command", cmd)

		// handle according to received command
		switch cmd {
		case "SHARDDONE":
			handleBlockCheck(conn, coord)
		case "GETSHARDS":
			handleGetShards(conn, coord)
		case "PROCBLOCK":
			handleProcessBlock(conn, coord)
		case "REQBLOCKS":
			handleRequestBlocks(conn, coord)

		default:
			reallog.Println("Command '", cmd, "' is not registered.")
		}
	}
}

// Receive a shard and handle messages from shard
func (coord *Coordinator) ReceiveShard(shard *Shard) {
	coord.HandleMessages(shard.Socket)
}

// This function sends a message to each of the shards connected to the coordinator
// informing it of the connections to other shards it needs to establish
func (coord *Coordinator) NotifyShards(addressList []*net.TCPAddr) {
	for shard, _ := range coord.shards {
		shard.Socket.Write([]byte("SHARDCON"))

		// TODO here there should be some logic to sort which shard gets what
		shardsToSend := AddressesGob{
			Addresses: addressList,
		}
		reallog.Printf("Shards gob", shardsToSend.Addresses[0])
		//Actually write the GOB on the socket
		enc := gob.NewEncoder(shard.Socket)
		err := enc.Encode(shardsToSend)
		if err != nil {
			reallog.Println("Error encoding addresses GOB data:", err)
			return
		}

	}

}

// Receive messates from a coordinator
// TODO: possiblly make shard/coordinator fit an interface
func (coord *Coordinator) ReceiveCoord(coordinator *Coordinator) {
	coord.HandleMessages(coordinator.Socket)
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

// Return send a list of all the shards
func handleGetShards(conn net.Conn, coord *Coordinator) {
	reallog.Print("Receive shards request")
	shardConnections := coord.GetShardsConnections()

	reallog.Printf("Shards to send", shardConnections)

	// All data is sent in gobs
	shardsToSend := AddressesGob{
		Addresses: shardConnections,
	}
	reallog.Printf("Shards gob", shardsToSend.Addresses[0])

	//Actually write the GOB on the socket
	enc := gob.NewEncoder(conn)
	err := enc.Encode(shardsToSend)
	if err != nil {
		reallog.Println("Error encoding addresses GOB data:", err)
		return
	}
}

// This function handle receiving a block over a connection
// and processing it
// The coordinator validates the header and waits for conformation
// from all the shards
func handleProcessBlock(conn net.Conn, coord *Coordinator) {
	reallog.Print("Receivd process block request")

	var header HeaderGob

	dec := gob.NewDecoder(conn)
	err := dec.Decode(&header)
	if err != nil {
		reallog.Println("Error decoding GOB data:", err)
		return
	}
	coord.ProcessBlock(header.Header, header.Flags, header.ActiveShards)
	conn.Write([]byte("BLOCKDONE"))
}

// This function handle receiving a request for blocks from another coordinator
func handleRequestBlocks(conn net.Conn, coord *Coordinator) {
	reallog.Print("Receivd request for blocks request")

	reallog.Println("The fist block in chain")
	reallog.Println(coord.Chain.BlockHashByHeight(0))

	for i := 0; i < coord.Chain.BestChainLength(); i++ {
		blockHash, err := coord.Chain.BlockHashByHeight(int32(i))
		if err != nil {
			reallog.Println("Unable to fetch hash of block ", i)
		}
		reallog.Println("Block hash ", i, " ", blockHash)
		coord.Chain.BlockShardByHash(blockHash)
	}

}

// Send the information in shard.data to the shard via socket
// This is used for broadcast
func (coord *Coordinator) Send(shard *Shard) {
	defer shard.Socket.Close()
	for {
		select {
		case message, ok := <-shard.data:
			if !ok {
				return
			}
			shard.Socket.Write(message)
		}
	}
}

// Returns all the shards in the coordinator shards maps
func (coord *Coordinator) GetShardsConnections() []*net.TCPAddr {
	connections := make([]*net.TCPAddr, 0, len(coord.shards))

	for key, _ := range coord.shards {
		conn := key.Socket.RemoteAddr().(*net.TCPAddr)
		conn.Port = key.Port // The port the shard is listening to other shards
		connections = append(connections, conn)
	}
	return connections

}

// Process block will make a sanity check on the block header and will wait for confirmations from all the shards
// that the block has been processed
func (coord *Coordinator) ProcessBlock(header *wire.BlockHeader, flags BehaviorFlags, activeShards int) error {
	err := CheckBlockHeaderSanity(header, coord.Chain.GetChainParams().PowLimit, coord.Chain.GetTimeSource(), flags)
	if err != nil {
		return err
	}
	// Wait for all the shards to send finish report
	for i := 0; i < activeShards; i++ {
		<-coord.allShardsDone
		reallog.Println("Done processing block")
	}
	coord.Chain.CoordMaybeAcceptBlock(header, flags)
	return nil
}

// A go routine to listen for other coordinator (peers) to connect
func (coord *Coordinator) ListenToCoordinators() error {
	// Wait for connections from other coordinators
	fmt.Println("Waiting for coordinators to connect")
	for {
		connection, _ := coord.CoordListener.Accept()
		fmt.Println("Receive connection", connection)
		c := NewCoordConnection(connection)
		coord.RegisterCoord(c)
		go coord.ReceiveCoord(c)
	}
}

// Send a request to a peer to get all the blocks in its database
// This only needs to request the blocks, and ProcessBlock should handle receiving them
func (coord *Coordinator) SendBlocksRequest() {
	for c, _ := range coord.coords {
		c.Socket.Write([]byte("REQBLOCKS"))
	}
}
