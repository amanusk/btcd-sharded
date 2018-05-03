package blockchain

import (
	"encoding/gob"
	"fmt"
	reallog "log"
	"net"

	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil/bloom"
)

// AddressesGob is a struct to send a list of tcp connections
type AddressesGob struct {
	Addresses []*net.TCPAddr
}

// HeaderGob is a struct to send headers over tcp connections
type HeaderGob struct {
	Header *wire.BlockHeader
	Flags  BehaviorFlags
	Height int32
}

// FilterGob struct to send bloomFilters. The regular does not register
type FilterGob struct {
	Filter *wire.MsgFilterLoad
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

// ConnAndFilter is used to pass the connection and the filter, the coordinator
// Creates a map between connections and filters received
type ConnAndFilter struct {
	Conn   net.Conn
	Filter *wire.MsgFilterLoad
}

// Coordinator is the coordinator in a sharded bitcoin cluster
type Coordinator struct {
	Socket              net.Conn              // Connection to the shard you connected to
	shards              map[net.Conn]*Shard   // A map of shards connected to this coordinator
	coords              map[*Coordinator]bool // A map of coords connected to this coordinator
	registerShard       chan *Shard
	unregisterShard     chan *Shard
	registerCoord       chan *Coordinator
	unregisterCoord     chan *Coordinator
	registerBloomFilter chan *ConnAndFilter // A channel to receive bloom filters on
	shardDone           chan bool
	allShardsDone       chan bool
	Connected           chan bool // Sends a sigal that a shard connection completed
	ConnectedOut        chan bool // Sends shards finished connecting to shards
	BlockDone           chan bool // channel to sleep untill blockDone signal is sent from peer before sending new block
	KeepAlive           chan interface{}
	ShardListener       net.Listener
	CoordListener       net.Listener
	Chain               *BlockChain
}

// NewCoordConnection creates a new coordinator connection for a coordintor to use.
// It has a connection and a channel to receive data from the server
func NewCoordConnection(connection net.Conn) *Coordinator {
	coord := &Coordinator{
		Socket: connection, // A connection to the connected peer
	}
	return coord
}

// NewCoordinator Cerates and returns a new coordinator
func NewCoordinator(shardListener net.Listener, coordListener net.Listener, blockchain *BlockChain) *Coordinator {
	coord := Coordinator{
		shards:              make(map[net.Conn]*Shard),
		coords:              make(map[*Coordinator]bool),
		registerShard:       make(chan *Shard),
		unregisterShard:     make(chan *Shard),
		registerCoord:       make(chan *Coordinator),
		unregisterCoord:     make(chan *Coordinator),
		registerBloomFilter: make(chan *ConnAndFilter),
		allShardsDone:       make(chan bool),
		Connected:           make(chan bool),
		ConnectedOut:        make(chan bool),
		BlockDone:           make(chan bool),
		Chain:               blockchain,
		ShardListener:       shardListener,
		CoordListener:       coordListener,
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
			coord.Connected <- true
		case shard := <-coord.unregisterShard:
			if _, ok := coord.shards[shard.Socket]; ok {
				delete(coord.shards, shard.Socket)
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

// HandleSmartMessages decodes received messages with command and data
func (coord *Coordinator) HandleSmartMessages(conn net.Conn) {
	// Register the special types for decode to work

	gob.Register(RawBlockGob{})
	gob.Register(HeaderGob{})
	gob.Register(AddressesGob{})
	gob.Register(FilterGob{})

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
			reallog.Println("Message BLOCKDONE")
			coord.handleBlockDone(conn)
		case "BLOOMFLT":
			filter := msg.Data.(FilterGob)
			coord.handleBloomFilter(conn, filter.Filter)

		default:
			reallog.Println("Command '", cmd, "' is not registered.")
		}
	}

}

// ReceiveShard receives a shard and handle messages from shard
func (coord *Coordinator) ReceiveShard(shard *Shard) {
	coord.HandleSmartMessages(shard.Socket)
}

// NotifyShards function sends a message to each of the shards connected to the coordinator
// informing it of the connections to other shards it needs to establish
func (coord *Coordinator) NotifyShards(addressList []*net.TCPAddr) {
	for cons := range coord.shards {
		enc := gob.NewEncoder(cons)

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

// ReceiveCoord receives messages from a coordinator
// TODO: possiblly make shard/coordinator fit an interface
func (coord *Coordinator) ReceiveCoord(c *Coordinator) {
	coord.HandleSmartMessages(c.Socket)
}

// Once a shards finishes processing a block this message is received
func (coord *Coordinator) handleShardDone(conn net.Conn) {
	reallog.Print("Receive Block Confirmation from shard")
	coord.shardDone <- true

}

// Receive a conformation a shard is sucessfuly connected
func (coord *Coordinator) handleConnectDone(conn net.Conn) {
	reallog.Print("Receive Conformation shard is connected sucessfuly")
	coord.ConnectedOut <- true
}

// Receive a conformation a block was processed by the other peer
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

	err := coord.ProcessBlock(headerBlock.Block, headerBlock.Flags, headerBlock.Height)
	if err != nil {
		reallog.Fatal("Coordinator unable to process block")
	}
	reallog.Println("Sending BLOCKDONE")

	enc := gob.NewEncoder(conn)

	// TODO here there should be some logic to sort which shard gets what
	msg := Message{
		Cmd: "BLOCKDONE",
	}
	err = enc.Encode(msg)
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
		header, err := coord.Chain.SQLFetchHeader(blockHash)

		headerBlock := wire.NewMsgBlockShard(&header)
		coinbase := coord.Chain.SQLDB.FetchCoinbase(blockHash)
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

// GetShardsConnections returns all the shards in the coordinator shards maps
func (coord *Coordinator) GetShardsConnections() []*net.TCPAddr {
	connections := make([]*net.TCPAddr, 0, len(coord.shards))

	for con, shard := range coord.shards {
		conn := con.RemoteAddr().(*net.TCPAddr)
		conn.Port = shard.Port // The port the shard is listening to other shards
		connections = append(connections, conn)
	}
	return connections

}

// ProcessBlock will make a sanity check on the block header and will wait for confirmations from all the shards
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
	for con := range coord.shards {
		enc := gob.NewEncoder(con)
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

	go coord.waitForShardsDone()

	// Handle bloom filters
	coord.waitForBloomFilters()

	// All shards must be done to unlock this channel
	<-coord.allShardsDone
	reallog.Println("Done processing block")

	coord.Chain.CoordMaybeAcceptBlock(headerBlock, flags)
	return nil
}

func (coord *Coordinator) waitForShardsDone() {
	coord.shardDone = make(chan bool, len(coord.shards))
	// Wait for all the shards to send finish report
	for i := 0; i < len(coord.shards); i++ {
		<-coord.shardDone
	}
	coord.allShardsDone <- true
}

// waitForBloomFilters waits for bloom filters according the number of shards
// Then process the union of the bloom filters and send them back to the shards
func (coord *Coordinator) waitForBloomFilters() {
	filters := make(map[net.Conn]*wire.MsgFilterLoad)
	for i := 0; i < len(coord.shards); i++ {
		conAndFilter := <-coord.registerBloomFilter
		filters[conAndFilter.Conn] = conAndFilter.Filter
		checkFilter := bloom.LoadFilter(conAndFilter.Filter)
		if checkFilter.FilterIsEmpty() {
			reallog.Println("Filter is empty, shardDone")
			coord.shardDone <- true
			continue
		}
		reallog.Println("Logged new filter ", conAndFilter.Filter)
	}
	reallog.Println("Done Receiving filters")

	coord.sendCombinedFilters(filters)
}

// sendCombinedFilters sends to each shard, the union of all other bloom filters
// The shard will then calculate the intersection of his own with the union of
// all the rest
func (coord *Coordinator) sendCombinedFilters(filters map[net.Conn]*wire.MsgFilterLoad) {
	combinedFilters := make(map[net.Conn]*wire.MsgFilterLoad)
	// Here we create he combined union filters
	for con := range filters {
		emptyFilter := bloom.NewFilter(10, 0, 0.1, wire.BloomUpdateNone)
		// The empty filter load will be used to create the union with
		emptyFilterLoad := emptyFilter.MsgFilterLoad()
		combinedFilters[con] = emptyFilterLoad
		for conC, filterC := range filters {
			if conC == con {
				continue // We want to union all but the the filter itself
			}
			combinedFilter, err := bloom.FilterUnion(combinedFilters[con], filterC)
			if err != nil {
				fmt.Printf("%v\n", err)
			}
			combinedFilters[con] = combinedFilter.MsgFilterLoad()
		}
	}

	// Once the filters are created, send a message to each shard with its combination,
	// unless the original filter is empty, then the shard will not be waiting for
	// a reply

	for con, filter := range combinedFilters {
		// No need to send if the original bloom filter is empty, i.e. no TXs
		testFilter := bloom.LoadFilter(filters[con])
		if testFilter.FilterIsEmpty() {
			continue
		}
		reallog.Println("Sending combined bloom filter back to shard")
		enc := gob.NewEncoder(con)
		err := enc.Encode(
			Message{
				Cmd: "BLOOMCOM",
				Data: FilterGob{
					Filter: filter,
				},
			})
		if err != nil {
			reallog.Println(err, "Encode failed for struct: %#v", filter)
		}
	}
}

// ListenToCoordinators is go routine to listen for other coordinator (peers) to connect
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

// SendBlocksRequest sends a request for a block to a peer to get all the blocks in its database
// This only needs to request the blocks, and ProcessBlock should handle receiving them
func (coord *Coordinator) SendBlocksRequest() {
	reallog.Println("Sending blocks request")
	for c := range coord.coords {
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

// Receive bloom filters from shards
func (coord *Coordinator) handleBloomFilter(conn net.Conn, filterLoad *wire.MsgFilterLoad) {
	reallog.Print("Received bloom filter to process")

	coord.registerBloomFilter <- &ConnAndFilter{conn, filterLoad}
}
