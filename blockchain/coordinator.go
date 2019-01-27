package blockchain

import (
	"encoding/gob"
	"fmt"
	logging "log"
	"net"
	"sync"
	"time"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
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
	Header   *wire.BlockHeader
	Flags    BehaviorFlags
	Height   int32
	Index    int
	Salt     string
	ShardNum int
}

// RawBlockGob is a struct to send full blocks
type RawBlockGob struct {
	Block  *wire.MsgBlock
	Flags  BehaviorFlags
	Height int32
}

// Message holds the message type and the data as interface
// The data should be cast accroding to the message
type Message struct {
	Cmd  string
	Data interface{}
}

// FetchedBlocksLockedMap save a map between shards and fetched blocks
type FetchedBlocksLockedMap struct {
	*sync.RWMutex
	fetchedBlocks map[net.Conn]*wire.MsgBlock
	Flags         BehaviorFlags
}

// LockedTxHashesMap stores slices of calculated Tx hashes, to calculate merkle tree
type LockedTxHashesMap struct {
	*sync.RWMutex
	TxHasehs map[net.Conn]*chainhash.Hash
}

// FetchedBlockToSend holds the current block being requested by other peers
// This is to cache the fetch to only be executed once.
type FetchedBlockToSend struct {
	*sync.RWMutex
	fetchedBlock *btcutil.Block
}

// LockedShardsMap is a lockable map between connections and shards
type LockedShardsMap struct {
	*sync.RWMutex
	Shards map[net.Conn]*Shard
}

// InsertShard inserts a new shard to the locked connection-shard map
func (m *LockedShardsMap) InsertShard(conn net.Conn, shard *Shard) {
	m.Lock()
	defer m.Unlock()
	m.Shards[conn] = shard
}

// RemoveShard removes a shard from the registered shards
func (m *LockedShardsMap) RemoveShard(conn net.Conn) {
	m.Lock()
	defer m.Unlock()
	delete(m.Shards, conn)
}

// GetShard returns a shard from a lockabale shards Map
func (m *LockedShardsMap) GetShard(conn net.Conn) *Shard {
	m.RLock()
	defer m.RUnlock()
	return m.Shards[conn]
}

// NumShards returns the number of shards currently registered in the map
func (m *LockedShardsMap) NumShards() int {
	m.RLock()
	defer m.RUnlock()
	return len(m.Shards)
}

// Coordinator is the coordinator in a sharded bitcoin cluster
type Coordinator struct {
	shards          *LockedShardsMap // A map of shards connected to this coordinator
	shardsIntraAddr map[int]net.TCPAddr
	// Used to connect in initial connection
	shardsInterAddr map[int]net.TCPAddr
	// shardIntraAddr: Mapping  between shard index and inter address
	// Used to connect to other nodes
	coords             map[net.Conn]*Coordinator // A map of coords connected to this coordinator
	registerShard      chan *Shard
	unregisterShard    chan *Shard
	registerCoord      chan *Coordinator
	unregisterCoord    chan *Coordinator
	shardDone          chan bool
	allShardsDone      chan bool
	ConnectectionAdded chan bool // Sends a sigal that a shard connection completed
	ConnectedOut       chan bool // Sends shards finished connecting to shards
	BlockDone          chan bool // channel to sleep untill blockDone signal is sent from peer before sending new block
	fetchedBlockShards *FetchedBlocksLockedMap
	KeepAlive          chan interface{}
	ShardListener      net.Listener
	CoordListener      net.Listener
	Chain              *BlockChain
	numShards          int
	// For Coordinator Connection, for use in DHT maps
	Socket net.Conn     // Connection to a coordinator
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
		shards:             &LockedShardsMap{&sync.RWMutex{}, map[net.Conn]*Shard{}},
		shardsIntraAddr:    make(map[int]net.TCPAddr),
		shardsInterAddr:    make(map[int]net.TCPAddr),
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
	return coord.shards.NumShards()
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
			coord.shards.InsertShard(shard.Socket, shard)
			fmt.Println("Added new shard!")
			coord.ConnectectionAdded <- true
		case shard := <-coord.unregisterShard:
			coord.shards.RemoveShard(shard.Socket)

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

	shard := coord.shards.GetShard(conn)
	dec := shard.Dec
	for {
		var msg Message
		err := dec.Decode(&msg)
		if err != nil {
			logging.Println("Error decoding GOB data:", err)
			return
		}
		cmd := msg.Cmd
		// logging.Println("Got cmd ", cmd)

		// handle according to received command
		switch cmd {
		case "SHARDDONE":
			coord.handleShardDone(conn)
		case "FTCHBLOCK":
			block := msg.Data.(RawBlockGob)
			coord.handleFetchedBlock(&block, conn)
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
		// logging.Println("Waiting on message on", &dec)
		err := dec.Decode(&msg)
		if err != nil {
			logging.Println("Error decoding GOB data:", err)
			return
		}
		cmd := msg.Cmd
		// logging.Println("Got cmd ", cmd)

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
			// logging.Println("Message BLOCKDONE")
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
	coord.shards.RLock()
	defer coord.shards.RUnlock()
	for _, shard := range coord.shards.Shards {
		enc := shard.Enc
		msg := Message{
			Cmd: "SHARDCON",
			Data: DHTGob{
				DHTTable: dhtTable,
			},
		}
		logging.Print("Sending dht to shard", shard.Index, msg.Data)
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
	coord.shards.RLock()
	defer coord.shards.RUnlock()
	for _, shard := range coord.shards.Shards {
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

//  receive slice of tx hashes to construct merkle tree
func (coord *Coordinator) handleHashSlice(conn net.Conn) {

}

// Once a shards finishes processing a block this message is received
func (coord *Coordinator) handleShardDone(conn net.Conn) {
	// logging.Print("Receive Block Confirmation from shard")
	coord.shardDone <- true

}

// Receive a conformation a shard is sucessfuly connected
func (coord *Coordinator) handleConnectDone(conn net.Conn) {
	logging.Print("Receive Conformation shard is connected sucessfuly")
	coord.ConnectedOut <- true
}

// Receive a conformation a block was processed by the other peer
func (coord *Coordinator) handleBlockDone(conn net.Conn) {
	// logging.Print("Receive conformation block finised processing")
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
	// logging.Println("Receivd process block request")

	startTime := time.Now()
	err := coord.ProcessBlock(headerBlock.Block, headerBlock.Flags, headerBlock.Height)
	if err != nil {
		logging.Println(err)
		coord.sendBadBlockToAll()
		logging.Fatal("Coordinator unable to process block")
	}
	coord.sendBlockDone(conn)
	endTime := time.Since(startTime).Seconds()
	logging.Println("Block", headerBlock.Height, "took", endTime, "to process")
	fmt.Println("Block", headerBlock.Height, "took", endTime, "to process")
}

func (coord *Coordinator) sendBlockDone(conn net.Conn) {
	// logging.Println("Sending BLOCKDONE")
	// TODO: This should be sent to a specific coordinator
	enc := coord.coords[conn].Enc
	if enc != nil {
		// logging.Println("Sending block done on enc", &enc)
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

func (coord *Coordinator) handleFetchedBlock(receivedBlock *RawBlockGob, conn net.Conn) {
	msgBlockShard := receivedBlock.Block
	coord.fetchedBlockShards.Lock()
	coord.fetchedBlockShards.fetchedBlocks[conn] = msgBlockShard
	coord.fetchedBlockShards.Unlock()
	coord.shardDone <- true
}

// This function handle receiving a request for blocks from another coordinator
func (coord *Coordinator) handleRequestBlocks(conn net.Conn) {
	// logging.Print("Receivd request for blocks request")

	// logging.Println("Sending request to ", conn)
	// logging.Println("The fist block in chain")
	logging.Println(coord.Chain.BlockHashByHeight(0))

	startTime := time.Now()
	for i := 1; i < coord.Chain.BestChainLength(); i++ {
		startTime := time.Now()
		blockHash, err := coord.Chain.BlockHashByHeight(int32(i))
		if err != nil {
			logging.Println("Unable to fetch hash of block ", i)
		}
		header, err := coord.Chain.FetchHeader(blockHash)

		headerBlock := wire.NewMsgBlock(&header)

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
		endTime := time.Since(startTime)
		logging.Println("Block", i, "took", endTime, "to send")
		fmt.Println("Block", i, "took", endTime, "to send")

	}
	endTime := time.Since(startTime).Seconds()
	logging.Println("Sending all blocks took", endTime)
	fmt.Println("Sending all blocks took", endTime)
	return

}

// GetShardsConnections returns all the shards in the coordinator shards maps
func (coord *Coordinator) GetShardsConnections() map[int]net.TCPAddr {
	logging.Println("Returning shard remote addresses")
	//addressMap := make(map[int]net.TCPAddr)

	//for _, shard := range coord.shards {
	//var address net.TCPAddr
	//address.IP = shard.IP
	//address.Port = shard.Port
	//addressMap[shard.Index] = address
	//logging.Println("Adding shard", shard.Index, "IP:", shard.IP, "Port", shard.Port)
	//}
	for shardIdx, address := range coord.shardsInterAddr {
		logging.Println("Shard", shardIdx, "Address", address)
	}
	return coord.shardsInterAddr

}

// ProcessBlock will make a sanity check on the block header and will wait for confirmations from all the shards
// that the block has been processed
func (coord *Coordinator) ProcessBlock(headerBlock *wire.MsgBlock, flags BehaviorFlags, height int32) error {
	coord.Chain.chainLock.Lock()
	defer coord.Chain.chainLock.Unlock()

	header := headerBlock.Header

	startTime := time.Now()
	// logging.Println("Processing block ", header.BlockHash(), " height ", height)

	// Start waiting for shards done before telling them to request it
	go coord.waitForShardsDone()

	// TODO add more checks as per btcd + coinbase checks
	err := CheckBlockHeaderSanity(&header, coord.Chain.GetChainParams().PowLimit, coord.Chain.GetTimeSource(), flags)
	if err != nil {
		return err
	}

	// Send block header to request to all shards
	coord.shards.RLock()
	for _, shard := range coord.shards.Shards {
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
	coord.shards.RUnlock()

	logging.Println("Wait for all shards to finish")
	<-coord.allShardsDone
	logging.Println("All shards finished")

	endTime := time.Since(startTime).Seconds()
	logging.Println("Processing bshards took", endTime)
	fmt.Println("Processing bshards took", endTime)

	coord.Chain.CoordMaybeAcceptBlock(headerBlock, flags)
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

	shard := coord.shards.GetShard(conn)
	enc := shard.Enc

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
	dec := shard.Dec
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
	shard := coord.shards.GetShard(conn)
	enc := shard.Enc

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
	// address[0] is the shard inter address
	// address[1] is the shard intra address
	logging.Println("The shard conn IP", conn.RemoteAddr())
	logging.Println("The shard saved IP", addresses[0].IP)
	// Map shard to index for coordinator communication
	// Map intra Address to index, needed for other shards
	coord.shardsIntraAddr[shardIndex] = *addresses[1]
	// Map inter Address to index, needed by other inter shards
	coord.shardsInterAddr[shardIndex] = *addresses[0]
	logging.Println("Receive shards addresses")
	logging.Println("Inter", addresses[0])
	logging.Println("Intra", addresses[1])
	coord.SendDHT(conn)

}
