package blockchain

import (
	"encoding/gob"
	"fmt"
	_ "io"
	reallog "log"
	"net"
	_ "strconv"
	_ "sync"
	_ "time"

	"github.com/btcsuite/btcd/wire"
)

// A struct to send a list of tcp connections
type AddressesGob struct {
	Addresses []*net.TCPAddr
}

type HeaderGob struct {
	Header *wire.BlockHeader
	Flags  BehaviorFlags
	Height int32
}

type RawBlockGob struct {
	Block  *wire.MsgBlockShard
	Flags  BehaviorFlags
	Height int32
}

// Just a simple example
type Complex struct {
	A int
	B string
}

type Message struct {
	Cmd  string
	Data interface{}
}

type Coordinator struct {
	Socket          net.Conn              // Receive information from other coordinators
	shards          map[*Shard]bool       // A map of shards connected to this coordinator
	coords          map[*Coordinator]bool // A map of coords connected to this coordinator
	registerShard   chan *Shard
	unregisterShard chan *Shard
	registerCoord   chan *Coordinator
	unregisterCoord chan *Coordinator
	allShardsDone   chan bool
	Connected       chan bool // Sends a sigal that a shard connection completed
	ConnectedOut    chan bool // Sends shards finished connecting to shards
	BlockDone       chan bool // channel to sleep untill blockDone signal is sent from peer before sending new block
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
		registerShard:   make(chan *Shard),
		unregisterShard: make(chan *Shard),
		registerCoord:   make(chan *Coordinator),
		unregisterCoord: make(chan *Coordinator),
		allShardsDone:   make(chan bool),
		Connected:       make(chan bool),
		ConnectedOut:    make(chan bool),
		BlockDone:       make(chan bool),
		Chain:           blockchain,
		ShardListener:   shardListener,
		CoordListener:   coordListener,
	}
	return &coord
}

type HandleFunc func(conn net.Conn, coord *Coordinator)

func (coord *Coordinator) GetNumShardes() int {
	return len(coord.shards)
}

func (coord *Coordinator) RegisterShard(shard *Shard) {
	coord.registerShard <- shard
}

func (coord *Coordinator) RegisterCoord(c *Coordinator) {
	coord.registerCoord <- c
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
				delete(coord.shards, connection)
				fmt.Println("A connection has terminated!")
			}

		// Register a new connected coordinator
		case connection := <-coord.registerCoord:
			coord.coords[connection] = true
			fmt.Println("Added new peer!", connection.Socket.RemoteAddr())
			// Saves the message in the channel in "message"
		}
	}
}

// This function receives a connection. The connection expects to get a message
// with inoformation on what is the request from the server. The server will
// handle the message according to the type of request it receives
func (coord *Coordinator) HandleMessages(conn net.Conn) {
}

// Decode received messages with command and data
func (coord *Coordinator) HandleSmartMessages(conn net.Conn) {
	// Register the special types for decode to work

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
		case "SHARDDONE":
			coord.handleShardDone(conn)
		case "GETSHARDS":
			coord.handleGetShards(conn)
		case "PROCBLOCK":
			block := msg.Data.(RawBlockGob)
			coord.handleProcessBlock(&block, conn)
		case "REQBLOCKS":
			go coord.handleRequestBlocks(conn)
		case "DEADBEAFS":
			coord.handleDeadBeaf(conn)
		case "CONCTDONE":
			coord.handleConnectDone(conn)
		case "BLOCKDONE":
			coord.handleBlockDone(conn)

		default:
			reallog.Println("Command '", cmd, "' is not registered.")
		}
	}

}

// Receive a shard and handle messages from shard
func (coord *Coordinator) ReceiveShard(shard *Shard) {
	//coord.HandleMessages(shard.Socket)
	coord.HandleSmartMessages(shard.Socket)
}

// This function sends a message to each of the shards connected to the coordinator
// informing it of the connections to other shards it needs to establish
func (coord *Coordinator) NotifyShards(addressList []*net.TCPAddr) {
	for shard, _ := range coord.shards {
		enc := gob.NewEncoder(shard.Socket)

		// TODO here there should be some logic to sort which shard gets what
		msg := Message{
			Cmd: "SHARDCON",
			Data: AddressesGob{
				Addresses: addressList,
			},
		}
		reallog.Print("Shards gob", msg.Data)
		//Actually write the GOB on the socket
		err := enc.Encode(msg)
		if err != nil {
			reallog.Println("Error encoding addresses GOB data:", err)
			return
		}
	}
}

// Receive messates from a coordinator
// TODO: possiblly make shard/coordinator fit an interface
func (coord *Coordinator) ReceiveCoord(c *Coordinator) {
	//coord.HandleMessages(c.Socket)
	coord.HandleSmartMessages(c.Socket)
}

// Once a shards finishes processing a block this message is received
func (coord *Coordinator) handleShardDone(conn net.Conn) {
	reallog.Print("Receive Block Confirmation from shard")
	coord.allShardsDone <- true

}

// Once a shards finishes processing a block this message is received
func (coord *Coordinator) handleThis(c Complex) {
	fmt.Println("Complex data", c)
}

// Receive a conformation a shard is sucessfuly connected
func (coord *Coordinator) handleConnectDone(conn net.Conn) {
	reallog.Print("Receive Conformation shard is connected sucessfuly")
	coord.ConnectedOut <- true
}

// Receive a conformation a shard is sucessfuly connected
func (coord *Coordinator) handleBlockDone(conn net.Conn) {
	reallog.Print("Receive conformation block finised processing")
	coord.BlockDone <- true
}

// Return send a list of all the shards
func (coord *Coordinator) handleGetShards(conn net.Conn) {

	reallog.Print("Receive shards request")
	// TODO TODO TODO Change this to work with messages like evrything else
	shardConnections := coord.GetShardsConnections()

	reallog.Print("Shards to send", shardConnections)

	// All data is sent in gobs
	shardsToSend := AddressesGob{
		Addresses: shardConnections,
	}
	reallog.Print("Shards gob", shardsToSend.Addresses[0])

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
func (coord *Coordinator) handleProcessBlock(headerBlock *RawBlockGob, conn net.Conn) {
	reallog.Print("Receivd process block request")

	coord.ProcessBlock(headerBlock.Block, headerBlock.Flags, headerBlock.Height)
	reallog.Println("Sending BLOCKDONE")

	enc := gob.NewEncoder(conn)

	// TODO here there should be some logic to sort which shard gets what
	msg := Message{
		Cmd: "BLOCKDONE",
	}
	err := enc.Encode(msg)
	if err != nil {
		reallog.Println("Error encoding addresses GOB data:", err)
		return
	}
}

// This function handle receiving a block over a connection
// and processing it
// The coordinator validates the header and waits for conformation
// from all the shards
func (coord *Coordinator) handleDeadBeaf(conn net.Conn) {
	reallog.Print("Received dead beef command")
}

// This function handle receiving a request for blocks from another coordinator
func (coord *Coordinator) handleRequestBlocks(conn net.Conn) {
	reallog.Print("Receivd request for blocks request")

	reallog.Println("Sending request to ", conn)
	reallog.Println("The fist block in chain")
	reallog.Println(coord.Chain.BlockHashByHeight(0))

	for i := 1; i < coord.Chain.BestChainLength(); i++ {
		blockHash, err := coord.Chain.BlockHashByHeight(int32(i))
		if err != nil {
			reallog.Println("Unable to fetch hash of block ", i)
		}
		// TODO change to fetch header + coinbase
		header, err := coord.Chain.SqlFetchHeader(blockHash)

		headerBlock := wire.NewMsgBlockShard(&header)
		coinbase := coord.Chain.SqlDB.FetchCoinbase(blockHash)
		headerBlock.AddTransaction(coinbase)

		reallog.Println("sending block hash ", header.BlockHash())
		reallog.Println("Sending block on", conn)

		// Send block to coordinator

		coordEnc := gob.NewEncoder(conn)
		// Generate a header gob to send to coordinator
		msg := Message{
			Cmd: "PROCBLOCK",
			Data: RawBlockGob{
				Block:  headerBlock,
				Flags:  BFNone,
				Height: int32(i),
			},
		}
		err = coordEnc.Encode(msg)
		if err != nil {
			reallog.Println(err, "Encode failed for struct: %#v", msg)
		}
		// Wait for BLOCKDONE to send next block
		reallog.Println("Waiting for conformation on block")

		<-coord.BlockDone

	}
	return

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
func (coord *Coordinator) ProcessBlock(headerBlock *wire.MsgBlockShard, flags BehaviorFlags, height int32) error {

	header := headerBlock.Header

	reallog.Println("Processing block ", header.BlockHash(), " height ", height)

	// TODO add more checks as per btcd + coinbase checks
	err := CheckBlockHeaderSanity(&header, coord.Chain.GetChainParams().PowLimit, coord.Chain.GetTimeSource(), flags)
	if err != nil {
		return err
	}

	// Send block header to request to all shards
	for shard, _ := range coord.shards {
		enc := gob.NewEncoder(shard.Socket)
		// Generate a header gob to send to coordinator
		msg := Message{
			Cmd: "REQBLOCK",
			Data: HeaderGob{
				Header: &header,
				Flags:  BFNone,
				Height: height, // optionally this will be done after the coord accept block is performed
			},
		}
		err = enc.Encode(msg)
		if err != nil {
			reallog.Println(err, "Encode failed for struct: %#v", msg)
		}
	}

	// Wait for all the shards to send finish report
	for i := 0; i < len(coord.shards); i++ {
		<-coord.allShardsDone
	}
	reallog.Println("Done processing block")

	coord.Chain.CoordMaybeAcceptBlock(headerBlock, flags)
	return nil
}

// A go routine to listen for other coordinator (peers) to connect
func (coord *Coordinator) ListenToCoordinators() error {
	// Wait for connections from other coordinators
	fmt.Println("Waiting for coordinators to connect")
	for {
		connection, _ := coord.CoordListener.Accept()
		fmt.Println("Received connection from", connection.RemoteAddr())
		c := NewCoordConnection(connection)
		coord.RegisterCoord(c)
		go coord.ReceiveCoord(c)
	}
}

// Send a request to a peer to get all the blocks in its database
// This only needs to request the blocks, and ProcessBlock should handle receiving them
func (coord *Coordinator) SendBlocksRequest() {
	reallog.Println("Sending blocks request")
	for c, _ := range coord.coords {
		reallog.Println("Sending request to ", c.Socket)
		enc := gob.NewEncoder(c.Socket)

		msg := Message{
			Cmd: "REQBLOCKS",
		}
		err := enc.Encode(msg)
		if err != nil {
			reallog.Println("Error encoding addresses GOB data:", err)
			return
		}
	}
}
