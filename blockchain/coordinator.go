package blockchain

import (
	"encoding/gob"
	"fmt"
	logging "log"
	"net"
	"time"

	"github.com/btcsuite/btcd/wire"
)

// AddressesGob is a struct to send a list of tcp connections
type AddressesGob struct {
	Index     int
	Addresses []*net.TCPAddr
}

// DHTGob is used to pass the shard index and addresses of all other
// shards to each shards
type DHTGob struct {
	Index    int
	DHTTable map[int]net.TCPAddr
}

// HeaderGob is a struct to send headers over tcp connections
type HeaderGob struct {
	Header *wire.BlockHeader
	Flags  BehaviorFlags
	Height int32
}

// RawBlockGob is a struct to send full blocks
type RawBlockGob struct {
	Block  *wire.MsgBlockShard
	Flags  BehaviorFlags
	Height int32
}

// Message holds the message type and the data as interface
// The data should be cast accroding to the message
type Message struct {
	Cmd  string
	Data interface{}
}

// Coordinator is the coordinator in a sharded bitcoin cluster
type Coordinator struct {
	shards             map[net.Conn]*Shard           // A map of shards connected to this coordinator
	dht                map[int]net.Conn              // Mapping  between dht index and connection
	shardsIntraAddr    map[int]net.TCPAddr           // Mapping  between shard index and intra address
	coords             map[net.Conn]*Coordinator     // A map of coords connected to this coordinator
	missingInputs      map[net.Conn][]*wire.OutPoint // A map between connections and missing Txs
	registerShard      chan *Shard
	unregisterShard    chan *Shard
	registerCoord      chan *Coordinator
	unregisterCoord    chan *Coordinator
	shardDone          chan bool
	allShardsDone      chan bool
	ConnectectionAdded chan bool // Sends a sigal that a shard connection completed
	ConnectedOut       chan bool // Sends shards finished connecting to shards
	BlockDone          chan bool // channel to sleep untill blockDone signal is sent from peer before sending new block
	KeepAlive          chan interface{}
	ShardListener      net.Listener
	CoordListener      net.Listener
	Chain              *BlockChain
	numShards          int
	// For Coordinator Connection only!
	Socket net.Conn     // Connection to the shard you connected to
	Enc    *gob.Encoder // Used to send information to this connection
	Dec    *gob.Decoder // Used to decode information from this connection
}

// NewCoordConnection creates a new coordinator connection for a coordintor to use.
// It has a connection and a channel to receive data from the server
func NewCoordConnection(connection net.Conn, enc *gob.Encoder, dec *gob.Decoder) *Coordinator {
	coord := &Coordinator{
		Socket: connection, // A connection to the connected peer
		Enc:    enc,
		Dec:    dec,
	}
	return coord
}

// NewCoordinator Cerates and returns a new coordinator
func NewCoordinator(shardListener net.Listener, coordListener net.Listener, blockchain *BlockChain, numShards int) *Coordinator {
	coord := Coordinator{
		shards:             make(map[net.Conn]*Shard),
		dht:                make(map[int]net.Conn),
		shardsIntraAddr:    make(map[int]net.TCPAddr),
		coords:             make(map[net.Conn]*Coordinator),
		registerShard:      make(chan *Shard),
		unregisterShard:    make(chan *Shard),
		registerCoord:      make(chan *Coordinator),
		unregisterCoord:    make(chan *Coordinator),
		allShardsDone:      make(chan bool),
		shardDone:          make(chan bool),
		ConnectectionAdded: make(chan bool),
		ConnectedOut:       make(chan bool),
		BlockDone:          make(chan bool),
		Chain:              blockchain,
		ShardListener:      shardListener,
		CoordListener:      coordListener,
		numShards:          numShards,
	}
	return &coord
}

// GetNumShardes returns the number of shards connected to the coordinator
func (coord *Coordinator) GetNumShardes() int {
	return len(coord.shards)
}

// RegisterShard saves the connection to a shard of the cluster
func (coord *Coordinator) RegisterShard(shard *Shard) {
	coord.registerShard <- shard
}

// RegisterCoord registers a coordinator(peer) to the map of other peers
func (coord *Coordinator) RegisterCoord(c *Coordinator) {
	coord.registerCoord <- c
}

// Start the coordintor, listening begins here
func (coord *Coordinator) Start() {
	for {
		select {
		// Handle shard connect/disconnect
		case shard := <-coord.registerShard:
			coord.shards[shard.Socket] = shard
			fmt.Println("Added new shard!")
			coord.ConnectectionAdded <- true
		case shard := <-coord.unregisterShard:
			if _, ok := coord.shards[shard.Socket]; ok {
				delete(coord.shards, shard.Socket)
				fmt.Println("A connection has terminated!")
			}

		// Register a new connected coordinator
		case c := <-coord.registerCoord:
			coord.coords[c.Socket] = c
			fmt.Println("Added new peer!", c.Socket.RemoteAddr())
			// Unlock when add to map finished
			coord.ConnectectionAdded <- true
		}
	}
}

// HandleShardMessages decodes received messages with command and data
func (coord *Coordinator) HandleShardMessages(conn net.Conn) {
	// Register the special types for decode to work

	gob.Register(RawBlockGob{})
	gob.Register(HeaderGob{})
	gob.Register(AddressesGob{})

	dec := coord.shards[conn].Dec
	for {
		var msg Message
		err := dec.Decode(&msg)
		if err != nil {
			logging.Println("Error decoding GOB data:", err)
			return
		}
		cmd := msg.Cmd
		logging.Println("Got cmd ", cmd)

		// handle according to received command
		switch cmd {
		case "SHARDDONE":
			coord.handleShardDone(conn)
		case "CONCTDONE":
			coord.handleConnectDone(conn)
		case "BADBLOCK":
			coord.handleBadBlock(conn)
		default:
			logging.Println("Command '", cmd, "' is not registered.")
		}
	}

}

// HandleCoordMessages decodes received messages with command and data
func (coord *Coordinator) HandleCoordMessages(conn net.Conn) {
	// Register the special types for decode to work

	gob.Register(RawBlockGob{})
	gob.Register(HeaderGob{})
	gob.Register(AddressesGob{})

	dec := coord.coords[conn].Dec
	for {
		var msg Message
		logging.Println("Waiting on message on", &dec)
		err := dec.Decode(&msg)
		if err != nil {
			logging.Println("Error decoding GOB data:", err)
			return
		}
		cmd := msg.Cmd
		logging.Println("Got cmd ", cmd)

		// handle according to received command
		switch cmd {
		case "GETSHARDS":
			coord.handleGetShards(conn)
		case "PROCBLOCK":
			block := msg.Data.(RawBlockGob)
			coord.handleProcessBlock(&block, conn)
		case "REQBLOCKS":
			go coord.handleRequestBlocks(conn)
		case "BLOCKDONE":
			logging.Println("Message BLOCKDONE")
			coord.handleBlockDone(conn)
		default:
			logging.Println("Command '", cmd, "' is not registered.")
		}
	}

}

// ReceiveIntraShard receives a shard and handle messages from shard
func (coord *Coordinator) ReceiveIntraShard(shard *Shard) {
	coord.HandleShardMessages(shard.Socket)
}

// NotifyShards function sends a message to each of the shards connected to the coordinator
// informing it of the connections to other shards it needs to establish
func (coord *Coordinator) NotifyShards(dhtTable map[int]net.TCPAddr) {
	for remoteShardIdx, remoteShardAddress := range dhtTable {
		logging.Println("Sending to shard at index", remoteShardIdx)
		enc := coord.shards[coord.dht[remoteShardIdx]].Enc
		logging.Println("Sending address", remoteShardIdx, "to", remoteShardIdx)

		var addresses []*net.TCPAddr
		// Only need one address
		addresses = append(addresses, &remoteShardAddress)

		msg := Message{
			Cmd: "SHARDCON",
			Data: AddressesGob{
				Addresses: addresses,
			},
		}
		logging.Print("Shards gob", msg.Data)
		//Actually write the GOB on the socket
		err := enc.Encode(msg)
		if err != nil {
			logging.Println("Error encoding addresses GOB data:", err)
			return
		}
	}
}

// Sends a BADBLOCK message to all shards, instructing to move on
func (coord *Coordinator) sendBadBlockToAll() {
	logging.Println("Sending BADBLOCK to all")
	for _, shard := range coord.shards {
		enc := shard.Enc

		msg := Message{
			Cmd: "BADBLOCK",
		}
		logging.Println("Sending bad block to all shards")

		err := enc.Encode(msg)
		if err != nil {
			logging.Println(err, "Encode failed for struct: BADBLOCK")
		}
	}
	// TODO: fix this to revert any changes made by the last block
	coord.shardDone <- true

	// TODO: Only send to the relevant coord
	for _, c := range coord.coords {
		enc := c.Enc

		msg := Message{
			Cmd: "BADBLOCK",
		}
		err := enc.Encode(msg)
		if err != nil {
			logging.Println("Error encoding addresses GOB data:", err)
			return
		}
	}
	return
}

func (coord *Coordinator) handleBadBlock(conn net.Conn) {
	coord.sendBadBlockToAll()
}

// ReceiveCoord receives messages from a coordinator
// TODO: possiblly make shard/coordinator fit an interface
func (coord *Coordinator) ReceiveCoord(c *Coordinator) {
	coord.HandleCoordMessages(c.Socket)
}

// Once a shards finishes processing a block this message is received
func (coord *Coordinator) handleShardDone(conn net.Conn) {
	logging.Print("Receive Block Confirmation from shard")
	coord.shardDone <- true

}

// Receive a conformation a shard is sucessfuly connected
func (coord *Coordinator) handleConnectDone(conn net.Conn) {
	logging.Print("Receive Conformation shard is connected sucessfuly")
	coord.ConnectedOut <- true
}

// Receive a conformation a block was processed by the other peer
func (coord *Coordinator) handleBlockDone(conn net.Conn) {
	logging.Print("Receive conformation block finised processing")
	coord.BlockDone <- true
}

// Return send a list of all the shards
func (coord *Coordinator) handleGetShards(conn net.Conn) {

	logging.Print("Receive shards request")
	shardConnections := coord.GetShardsConnections()

	logging.Print("Shards to send", shardConnections)

	// All data is sent in gobs
	msg := DHTGob{
		DHTTable: shardConnections,
	}

	//Actually write the GOB on the socket
	enc := coord.coords[conn].Enc
	err := enc.Encode(msg)
	if err != nil {
		logging.Println("Error encoding addresses GOB data:", err)
		return
	}
}

// This function handle receiving a block over a connection
// and processing it
// The coordinator validates the header and waits for conformation
// from all the shards
func (coord *Coordinator) handleProcessBlock(headerBlock *RawBlockGob, conn net.Conn) {
	logging.Println("Receivd process block request")

	startTime := time.Now()
	err := coord.ProcessBlock(headerBlock.Block, headerBlock.Flags, headerBlock.Height)
	if err != nil {
		logging.Fatal("Coordinator unable to process block")
	}
	coord.sendBlockDone(conn)
	endTime := time.Since(startTime)
	//logging.Println("Block", headerBlock.Height, "took", endTime)
	fmt.Println("Block", headerBlock.Height, "took", endTime)
}

func (coord *Coordinator) sendBlockDone(conn net.Conn) {
	logging.Println("Sending BLOCKDONE")
	// TODO: This should be sent to a specific coordinator
	enc := coord.coords[conn].Enc
	if enc != nil {
		logging.Println("Sending block done on enc", &enc)
	} else {
		logging.Println("Could not find enc", &enc)
	}

	msg := Message{
		Cmd: "BLOCKDONE",
	}
	err := enc.Encode(msg)
	if err != nil {
		logging.Println("Error encoding addresses GOB data:", err)
		return
	}
}

// This function handle receiving a block over a connection
// and processing it
// The coordinator validates the header and waits for conformation
// from all the shards
func (coord *Coordinator) handleDeadBeaf(conn net.Conn) {
	logging.Print("Received dead beef command")
}

// This function handle receiving a request for blocks from another coordinator
func (coord *Coordinator) handleRequestBlocks(conn net.Conn) {
	logging.Print("Receivd request for blocks request")

	logging.Println("Sending request to ", conn)
	logging.Println("The fist block in chain")
	logging.Println(coord.Chain.BlockHashByHeight(0))

	for i := 1; i < coord.Chain.BestChainLength(); i++ {
		blockHash, err := coord.Chain.BlockHashByHeight(int32(i))
		if err != nil {
			logging.Println("Unable to fetch hash of block ", i)
		}
		// TODO change to fetch header + coinbase
		header, err := coord.Chain.FetchHeader(blockHash)

		headerBlock := wire.NewMsgBlockShard(&header)

		logging.Println("sending block hash ", header.BlockHash())
		logging.Println("Sending block on", conn)

		// Send block to coordinator

		coordEnc := coord.coords[conn].Enc
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
			logging.Println(err, "Encode failed for struct: %#v", msg)
		}
		// Wait for BLOCKDONE to send next block
		logging.Println("Waiting for conformation on block")

		<-coord.BlockDone
		logging.Println("Received block done")

	}
	return

}

// GetShardsConnections returns all the shards in the coordinator shards maps
func (coord *Coordinator) GetShardsConnections() map[int]net.TCPAddr {
	addressMap := make(map[int]net.TCPAddr)

	for _, shard := range coord.shards {
		var address net.TCPAddr
		address.IP = shard.IP
		address.Port = shard.Port
		addressMap[shard.Index] = address
		logging.Println("Adding shard", shard.Index, "IP:", shard.IP, "Port", shard.Port)
	}
	return addressMap

}

// ProcessBlock will make a sanity check on the block header and will wait for confirmations from all the shards
// that the block has been processed
func (coord *Coordinator) ProcessBlock(headerBlock *wire.MsgBlockShard, flags BehaviorFlags, height int32) error {
	coord.Chain.chainLock.Lock()
	defer coord.Chain.chainLock.Unlock()

	header := headerBlock.Header

	logging.Println("Processing block ", header.BlockHash(), " height ", height)

	// Start waiting for shards done before telling them to request it
	go coord.waitForShardsDone()

	// TODO add more checks as per btcd + coinbase checks
	err := CheckBlockHeaderSanity(&header, coord.Chain.GetChainParams().PowLimit, coord.Chain.GetTimeSource(), flags)
	if err != nil {
		return err
	}

	// Send block header to request to all shards
	for _, shard := range coord.shards {
		enc := shard.Enc
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
			logging.Println(err, "Encode failed for struct: %#v", msg)
		}
	}
	logging.Println("Wait for all shards to finish")
	<-coord.allShardsDone
	logging.Println("All shards finished")

	coord.Chain.CoordMaybeAcceptBlock(headerBlock, flags)
	logging.Println("Done processing block")
	return nil
}

func (coord *Coordinator) waitForShardsDone() {
	// Wait for all the shards to send finish report
	for i := 0; i < coord.numShards; i++ {
		<-coord.shardDone
	}
	coord.allShardsDone <- true
}

// ListenToCoordinators is go routine to listen for other coordinator (peers) to connect
func (coord *Coordinator) ListenToCoordinators() error {
	// Wait for connections from other coordinators
	fmt.Println("Waiting for coordinators to connect")
	for {
		connection, _ := coord.CoordListener.Accept()
		fmt.Println("Received connection from", connection.RemoteAddr())
		enc := gob.NewEncoder(connection)
		dec := gob.NewDecoder(connection)
		c := NewCoordConnection(connection, enc, dec)
		coord.RegisterCoord(c)
		<-coord.ConnectectionAdded
		go coord.ReceiveCoord(c)
	}
}

// SendBlocksRequest sends a request for a block to a peer to get all the blocks in its database
// This only needs to request the blocks, and ProcessBlock should handle receiving them
func (coord *Coordinator) SendBlocksRequest() {
	logging.Println("Sending blocks request")
	for _, c := range coord.coords {

		enc := c.Enc

		msg := Message{
			Cmd: "REQBLOCKS",
		}

		logging.Println("Sending REQBLOCKS on", enc)
		err := enc.Encode(msg)
		if err != nil {
			logging.Println("Error encoding addresses GOB data:", err)
			return
		}
	}
}

// RequestShardsInfo sends a request to the shards for their IP and port number
// on which they listen to other shards
func (coord *Coordinator) RequestShardsInfo(conn net.Conn, shardIndex int) {
	logging.Println("Requesting Shard Info")
	gob.Register(AddressesGob{})

	enc := coord.shards[conn].Enc

	msg := Message{
		Cmd:  "REQINFO",
		Data: shardIndex,
	}
	err := enc.Encode(msg)
	if err != nil {
		logging.Println("Error encoding addresses GOB data:", err)
		return
	}
	// Wait for shard to reply info
	dec := coord.shards[conn].Dec
	err = dec.Decode(&msg)
	if err != nil {
		logging.Panicln("Error decoding GOB data:", err)
	}
	if msg.Cmd != "RPLYINFO" {
		logging.Panicln("Error receiving index from coord", err)
	}
	AddressesGob := msg.Data.(AddressesGob)
	coord.handleRepliedInfo(AddressesGob.Addresses, AddressesGob.Index, conn)

}

// SendDHT sends the map between index and IP to each shard
func (coord *Coordinator) SendDHT(conn net.Conn) {
	gob.Register(DHTGob{})
	enc := coord.shards[conn].Enc

	// Shards will get the DHT of all who is current connected
	msg := Message{
		Cmd: "SHARDDHT",
		Data: DHTGob{
			DHTTable: coord.shardsIntraAddr,
		},
	}
	logging.Print("Sending DHT", msg.Data)
	//Actually write the GOB on the socket
	err := enc.Encode(msg)
	if err != nil {
		logging.Println("Error encoding addresses GOB data:", err)
		return
	}
}

func (coord *Coordinator) handleRepliedInfo(addresses []*net.TCPAddr, shardIndex int, conn net.Conn) {
	// Set information on current shard port and IP
	// address[0] is the shard inter port
	// address[1] is the shard intra port
	coord.shards[conn].Port = addresses[0].Port
	coord.shards[conn].IP = addresses[0].IP
	// Map shard to index (needed for other peers)
	coord.dht[shardIndex] = conn
	// Map Address to index, needed for other shards
	coord.shardsIntraAddr[shardIndex] = *addresses[1]
	logging.Println("Receive shards addresses")
	logging.Println("Inter", addresses[0])
	logging.Println("Intra", addresses[1])
	coord.SendDHT(conn)

}
