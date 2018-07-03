package blockchain

import (
	"encoding/binary"
	"encoding/gob"
	"fmt"
	logging "log"
	"net"
	"time"

	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil/bloom"
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

// MissingTxOutsGob  is a struct to send a map of missing TxInputs
type MissingTxOutsGob struct {
	// TODO: Remove MatchingTxOuts
	MatchingTxOuts   []*wire.OutPoint
	MissingEmptyOuts map[wire.OutPoint]struct{}
	MissingTxOuts    map[wire.OutPoint]*UtxoEntry
}

// HeaderGob is a struct to send headers over tcp connections
type HeaderGob struct {
	Header *wire.BlockHeader
	Flags  BehaviorFlags
	Height int32
}

// FilterGob struct to send bloomFilters. The regular does not register
type FilterGob struct {
	InputFilter   *wire.MsgFilterLoad
	TxFilter      *wire.MsgFilterLoad
	MissingTxOuts []*wire.OutPoint
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
	Conn    net.Conn
	Filters *FilterGob
}

// ConnAndMatchingTxs is used to pass the connection and the matching transactions
// Creates a map between connections and transactions list
type ConnAndMatchingTxs struct {
	Conn           net.Conn
	MatchingTxOuts []*wire.OutPoint
	MissingTxOuts  map[wire.OutPoint]*UtxoEntry
}

// Coordinator is the coordinator in a sharded bitcoin cluster
type Coordinator struct {
	Socket              net.Conn                      // Connection to the shard you connected to
	shards              map[net.Conn]*Shard           // A map of shards connected to this coordinator
	dht                 map[int]net.Conn              // Mapping  between dht index and connection
	shardsIntraAddr     map[int]net.TCPAddr           // Mapping  between shard index and intra address
	coords              map[*Coordinator]bool         // A map of coords connected to this coordinator
	missingInputs       map[net.Conn][]*wire.OutPoint // A map between connections and missing Txs
	registerShard       chan *Shard
	unregisterShard     chan *Shard
	registerCoord       chan *Coordinator
	unregisterCoord     chan *Coordinator
	registerBloomFilter chan *ConnAndFilter // A channel to receive bloom filters on
	shardsToWaitFor     map[net.Conn]bool
	registerMatchingTxs chan *ConnAndMatchingTxs // A channel to receive lists of matching transactions
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
		dht:                 make(map[int]net.Conn),
		shardsIntraAddr:     make(map[int]net.TCPAddr),
		coords:              make(map[*Coordinator]bool),
		registerShard:       make(chan *Shard),
		unregisterShard:     make(chan *Shard),
		registerCoord:       make(chan *Coordinator),
		unregisterCoord:     make(chan *Coordinator),
		registerBloomFilter: make(chan *ConnAndFilter),
		registerMatchingTxs: make(chan *ConnAndMatchingTxs),
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
	gob.Register(MissingTxOutsGob{})

	for {
		dec := gob.NewDecoder(conn)
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
		case "GETSHARDS":
			coord.handleGetShards(conn)
		case "PROCBLOCK":
			block := msg.Data.(RawBlockGob)
			coord.handleProcessBlock(&block, conn)
		case "REQBLOCKS":
			coord.handleRequestBlocks(conn)
		case "DEADBEAFS":
			coord.handleDeadBeaf(conn)
		case "CONCTDONE":
			coord.handleConnectDone(conn)
		case "BADBLOCK":
			coord.handleBadBlock(conn)
		case "BLOCKDONE":
			logging.Println("Message BLOCKDONE")
			coord.handleBlockDone(conn)
		case "BLOOMFLT":
			filter := msg.Data.(FilterGob)
			coord.handleBloomFilter(conn, &filter)
		case "MATCHTXS":
			receivedTxOuts := msg.Data.(MissingTxOutsGob)
			coord.handleMatcingMissingTxOuts(conn, receivedTxOuts.MatchingTxOuts, receivedTxOuts.MissingTxOuts)
		// Receive shard information on incoming ports
		case "RPLYINFO":
			AddressesGob := msg.Data.(AddressesGob)
			coord.handleRepliedInfo(AddressesGob.Addresses, AddressesGob.Index, conn)

		default:
			logging.Println("Command '", cmd, "' is not registered.")
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
	for con := range coord.shards {
		enc := gob.NewEncoder(con)

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
	for c := range coord.coords {
		enc := gob.NewEncoder(c.Socket)

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
	coord.HandleSmartMessages(c.Socket)
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
	// TODO TODO TODO Change this to work with messages like evrything else
	// TODO TODO TODO change to consider the dht
	shardConnections := coord.GetShardsConnections()

	logging.Print("Shards to send", shardConnections)

	// All data is sent in gobs
	shardsToSend := AddressesGob{
		Addresses: shardConnections,
	}
	logging.Print("Shards gob", shardsToSend.Addresses[0])

	//Actually write the GOB on the socket
	enc := gob.NewEncoder(conn)
	err := enc.Encode(shardsToSend)
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
	go coord.sendBlockDone(conn)
	endTime := time.Since(startTime)
	logging.Println("Block", headerBlock.Height, "took", endTime)
}

func (coord *Coordinator) sendBlockDone(conn net.Conn) {
	logging.Println("Sending BLOCKDONE")
	// TODO: This should be sent to a specific coordinator
	enc := gob.NewEncoder(conn)

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
		header, err := coord.Chain.SQLFetchHeader(blockHash)

		headerBlock := wire.NewMsgBlockShard(&header)
		coinbase := coord.Chain.SQLDB.FetchCoinbase(blockHash)
		headerBlock.AddTransaction(coinbase)

		logging.Println("sending block hash ", header.BlockHash())
		logging.Println("Sending block on", conn)

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
			logging.Println(err, "Encode failed for struct: %#v", msg)
		}
		// Wait for BLOCKDONE to send next block
		logging.Println("Waiting for conformation on block")

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
	coord.Chain.chainLock.Lock()
	defer coord.Chain.chainLock.Unlock()

	header := headerBlock.Header

	logging.Println("Processing block ", header.BlockHash(), " height ", height)

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
			logging.Println(err, "Encode failed for struct: %#v", msg)
		}
	}

	go coord.waitForShardsDone()

	// Handle bloom filters
	//coord.waitForBloomFilters()

	// Wait for matching TXs in bloom filters
	//coord.waitForMatchingTxs()

	// All shards must be done to unlock this channel
	<-coord.allShardsDone
	logging.Println("Done processing block")

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
	inputFilters := make(map[net.Conn]*wire.MsgFilterLoad)
	txFilters := make(map[net.Conn]*wire.MsgFilterLoad)
	coord.missingInputs = make(map[net.Conn][]*wire.OutPoint)
	//coord.shardsToWaitFor = make(map[net.Conn]bool)

	for i := 0; i < len(coord.shards); i++ {

		connAndFilters := <-coord.registerBloomFilter
		inputFilters[connAndFilters.Conn] = connAndFilters.Filters.InputFilter
		txFilters[connAndFilters.Conn] = connAndFilters.Filters.TxFilter
		coord.missingInputs[connAndFilters.Conn] = connAndFilters.Filters.MissingTxOuts

		//if connAndFilters.Filters.TxFilter == nil {
		//	logging.Println("Filter is empty, shardDone")
		//	coord.shardDone <- true
		//	continue
		//} else {
		//	logging.Println("Waiting for shard", connAndFilters.Conn)
		//	coord.shardsToWaitFor[connAndFilters.Conn] = true
		//}

		logging.Println("Logged new filter ")
	}
	logging.Println("Done receiving filters")
	//logging.Println("Expecting", len(coord.shardsToWaitFor), " answers")
	coord.sendCombinedFilters(inputFilters, txFilters, coord.missingInputs)
}

// waitForMatchingTxs waits for each shard to send then tx inputs that have
// matched the bloom filter it received. Am empty list means no transactions
// are in possible violation
func (coord *Coordinator) waitForMatchingTxs() {
	matchingTxsMap := make(map[net.Conn][]*wire.OutPoint)
	availableTxOuts := make(map[wire.OutPoint]*UtxoEntry)
	logging.Println("Waiting for matches from", len(coord.shards), " shards")
	for i := 0; i < len(coord.shards); i++ {
		connAndTxs := <-coord.registerMatchingTxs
		matchingTxsMap[connAndTxs.Conn] = connAndTxs.MatchingTxOuts
		// Add all available TxOuts to the map of available ones
		for txOut, utxoEnry := range connAndTxs.MissingTxOuts {
			availableTxOuts[txOut] = utxoEnry
		}

		logging.Println("Registered maps of matching Txs")
		//logging.Println("Registered list ", connAndTxs.MatchingTxOuts)
	}
	// Once all the shards have sent the transactions that collide,
	// Go over the colliding list and see if these are actually the same transaction
	for name, list := range matchingTxsMap {
		// Create a combined list of all the rest
		var combinedColliding []*wire.OutPoint
		for n, l := range matchingTxsMap {
			// Do not include the txs of the the current name being checked
			if name == n {
				continue
			}
			combinedColliding = append(combinedColliding, l...)
		}
		// Search the combined list for transactions of each shard
		for _, tx := range list {
			for _, comparedTx := range combinedColliding {
				//logging.Println("Comapred ", comparedTx)
				//logging.Println("tx", tx)
				if *tx == *comparedTx {
					logging.Println("Tx,", tx, "Is double spending")
					coord.sendBadBlockToAll()
					return
				}
			}
		}
	}

	// Deal with missing Tx outputs
	// missingTxOutsMap will hold the map of UTXo to send to each shard
	missingTxOutsToSend := make(map[net.Conn]map[wire.OutPoint]*UtxoEntry)
	for shardConn, missingTxOuts := range coord.missingInputs {
		for _, txOut := range missingTxOuts {
			if utxoEntry, ok := availableTxOuts[*txOut]; ok {
				logging.Println("Missing TxOut", *txOut, "is available")
				if _, ok := missingTxOutsToSend[shardConn]; !ok {
					missingTxOutsToSend[shardConn] = make(map[wire.OutPoint]*UtxoEntry)
				}
				missingTxOutsToSend[shardConn][*txOut] = utxoEntry
				logging.Println("Added missing tx", *txOut, utxoEntry, "to conn", shardConn)
			}
		}
	}
	coord.sendMissingOutsToAll(missingTxOutsToSend)

}

// sendMissingOutsToAll sends OK to all shards, indicating no collisions
func (coord *Coordinator) sendMissingOutsToAll(missingMap map[net.Conn]map[wire.OutPoint]*UtxoEntry) {
	logging.Println("Sending missing Outs to all")

	for con := range missingMap {
		for missingOut := range missingMap[con] {
			logging.Println("Sending missingTx", missingOut)
		}
	}
	// TODO: do not send to shards that do not need it
	for con := range coord.shards {
		enc := gob.NewEncoder(con)

		msg := Message{
			Cmd: "MISSOUTS",
			Data: MissingTxOutsGob{
				MissingTxOuts: missingMap[con],
			},
		}
		err := enc.Encode(msg)
		if err != nil {
			logging.Println("Error encoding addresses GOB data:", err)
			return
		}
	}
}

// sendCombinedFilters sends to each shard, the union of all other bloom filters
// The shard will then calculate the intersection of his own with the union of
// all the rest
// For missing TXs: Find which shard is holding which missing TX, construct a
// list per shard with missing TXs to send
func (coord *Coordinator) sendCombinedFilters(inputFilters map[net.Conn]*wire.MsgFilterLoad,
	txFilters map[net.Conn]*wire.MsgFilterLoad, missingInputs map[net.Conn][]*wire.OutPoint) {
	combinedFilters := make(map[net.Conn]*wire.MsgFilterLoad)
	// Here we create the combined union inputFilters
	for con, inFilter := range inputFilters {
		if inFilter == nil {
			continue
		}
		emptyFilter := bloom.NewFilter(1000, 0, 0.001, wire.BloomUpdateNone)
		// The empty filter load will be used to create the union with
		emptyFilterLoad := emptyFilter.MsgFilterLoad()
		combinedFilters[con] = emptyFilterLoad
		for conC, filterC := range inputFilters {
			if conC == con {
				continue // We want to union all but the the filter itself
			}
			if filterC == nil {
				continue
			}
			combinedFilter, err := bloom.FilterUnion(combinedFilters[con], filterC)
			if err != nil {
				fmt.Printf("%v\n", err)
			}
			combinedFilters[con] = combinedFilter.MsgFilterLoad()
		}
	}
	// Print all missing inputs
	for conn, missing := range missingInputs {
		logging.Println("Shard", conn, "is missing:")
		for _, input := range missing {
			logging.Println(input)
		}
	}

	missingToSend := make(map[net.Conn][]*wire.OutPoint)
	shardIndexMap := make(map[int]net.Conn)

	// generate shardIndex-> conn map
	for conn, shard := range coord.shards {
		shardIndexMap[shard.Index] = conn
	}

	// Find missing TXs per shard
	// Iterate over all shards
	for _, missing := range missingInputs {
		// Iterate over list of all missing Txs
		for _, input := range missing {
			// Index this should belong to
			shardNum := binary.BigEndian.Uint64(input.Hash[:]) % uint64(len(coord.shards))
			logging.Println("ShardNum", shardNum)
			missingToSend[shardIndexMap[int(shardNum)]] = append(missingToSend[shardIndexMap[int(shardNum)]], input)
			logging.Println("Added", input, "to shard", shardNum)
		}
	}

	// Once the inputFilters are created, send a message to each shard with its combination,
	// unless the original filter is empty, then the shard will not be waiting for
	// a reply

	for con, filter := range combinedFilters {
		// No need to send if the original bloom filter is empty, i.e. no TXs
		// testFilter := bloom.LoadFilter(inputFilters[con])
		if filter == nil {
			continue
		}
		logging.Println("Sending combined bloom filter back to shard")
		enc := gob.NewEncoder(con)
		err := enc.Encode(
			Message{
				Cmd: "BLOOMCOM",
				Data: FilterGob{
					InputFilter:   filter,
					MissingTxOuts: missingToSend[con],
				},
			})
		if err != nil {
			logging.Println(err, "Encode failed for struct: %#v", filter)
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
	logging.Println("Sending blocks request")
	for c := range coord.coords {
		enc := gob.NewEncoder(c.Socket)

		msg := Message{
			Cmd: "REQBLOCKS",
		}
		err := enc.Encode(msg)
		if err != nil {
			logging.Println("Error encoding addresses GOB data:", err)
			return
		}
	}
}

// Receive bloom filters from shards
func (coord *Coordinator) handleBloomFilter(conn net.Conn, filterGob *FilterGob) {
	logging.Print("Received bloom filter to process")

	coord.registerBloomFilter <- &ConnAndFilter{conn, filterGob}
}

// Receive bloom filters from shards
func (coord *Coordinator) handleMatcingMissingTxOuts(conn net.Conn, matchingTxOuts []*wire.OutPoint, missingTxOuts map[wire.OutPoint]*UtxoEntry) {
	logging.Print("Received bloom filter to process and missing Tx outs")

	coord.registerMatchingTxs <- &ConnAndMatchingTxs{conn, matchingTxOuts, missingTxOuts}
}

// RequestShardsInfo sends a request to the shards for their IP and port number
// on which they listen to other shards
func (coord *Coordinator) RequestShardsInfo(conn net.Conn, shardIndex int) {
	logging.Println("Requesting Shard Info")
	enc := gob.NewEncoder(conn)

	msg := Message{
		Cmd:  "REQINFO",
		Data: shardIndex,
	}
	err := enc.Encode(msg)
	if err != nil {
		logging.Println("Error encoding addresses GOB data:", err)
		return
	}
}

// SendDHT sends the map between index and IP to each shard
func (coord *Coordinator) SendDHT(con net.Conn) {
	gob.Register(DHTGob{})
	enc := gob.NewEncoder(con)

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
	coord.shards[conn].Port = addresses[0].Port
	coord.shards[conn].IP = addresses[0].IP
	// Set shard dht index
	coord.shardsIntraAddr[shardIndex] = *addresses[1]
	logging.Println("Receive shards addresses")
	logging.Println("Inter", addresses[0])
	logging.Println("Intra", addresses[1])
	coord.SendDHT(conn)
	coord.ConnectedOut <- true

}
