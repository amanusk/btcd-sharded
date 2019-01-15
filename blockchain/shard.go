package blockchain

import (
	"encoding/binary"
	"encoding/gob"
	"fmt"
	logging "log"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
)

// TxOutsLockedMap save a map between output and UtxoEntry,
// The lock is used to prevent 2 threads writing to the map
type TxOutsLockedMap struct {
	*sync.RWMutex
	TxOutsMap map[int]map[wire.OutPoint]*UtxoEntry
}

// Shard is a single node in a sharded bitcoin client cluster
type Shard struct {
	Chain              *BlockChain
	CoordConn          *Coordinator     // Hold coordinator connection + enc + dec
	interShards        *LockedShardsMap // A map of shards connected to this shard
	intraShards        *LockedShardsMap // A reverse map between sockent and shard number
	ShardNumToShard    map[int]*Shard   // A map between shard index and shard connection
	registerShard      chan *Shard
	unregisterShard    chan *Shard
	Finish             chan bool
	Port               int // Saves the port the shard is listening to other shards
	Index              int // Save the shard index in the coords array
	ShardInterListener net.Listener
	ShardIntraListener net.Listener
	missingTxOuts      chan (map[wire.OutPoint]*UtxoEntry)
	requestedTxOutsMap *TxOutsLockedMap
	retrievedTxOutsMap *TxOutsLockedMap
	NumShards          int // Save the number of shards in total, needed to calc the mod
	// Shard-to-shard comms
	connected                 chan bool // Pass true once you finshed connecting to a shard from dht
	ConnectionAdded           chan bool // Lock to wait for write to connections map to finish
	InterIP                   net.IP
	IntraIP                   net.IP
	InterPort                 int
	IntraPort                 int
	fetchedBlockShards        *FetchedBlocksLockedMap
	cachedBlock               *FetchedBlockToSend
	receiveAllMissingRequests chan bool // A channel to unlock the requests from all shards
	receiveMissingRequest     chan bool // A channel to unlock the requests from all shards
	receiveAllRetrieved       chan bool // A channel to unlock when all retrieved TxOuts received
	receiveRetrieved          chan bool // A channel to unlock per retrived TxOut received
	receiveAllRequestedShards chan bool
	receiveRequestedShard     chan bool
	// For use in DHTmaps
	Socket net.Conn     // Connection
	Enc    *gob.Encoder // The encoder used to send information on connection
	Dec    *gob.Decoder // Decoder for receiving information on connection
}

// NewShardConnection Creates a new shard connection for a coordinator to use.
// It has a connection and a channel to receive data from the server
func NewShardConnection(connection net.Conn, shardPort int, index int, enc *gob.Encoder, dec *gob.Decoder) *Shard {
	shard := &Shard{
		Socket: connection, // The connection to use with coord/other shard
		Port:   shardPort,  // Will store the prot the shard is listening to for other shards
		Index:  index,
		Enc:    enc,
		Dec:    dec,
	}
	return shard
}

// NewShard Creates a new shard for a coordinator to use.
// It has a connection and a channel to receive data from the server
func NewShard(shardInterListener net.Listener, shardIntraListener net.Listener, coordConn *Coordinator, blockchain *BlockChain, interIP net.IP, intraIP net.IP, interShardPort int, intraShardPort int) *Shard {
	shard := &Shard{
		Chain:           blockchain,
		registerShard:   make(chan *Shard),
		unregisterShard: make(chan *Shard),
		missingTxOuts:   make(chan map[wire.OutPoint]*UtxoEntry),
		Finish:          make(chan bool),
		CoordConn:       coordConn,
		// Shard-to-shard comms
		interShards:        &LockedShardsMap{&sync.RWMutex{}, map[net.Conn]*Shard{}},
		intraShards:        &LockedShardsMap{&sync.RWMutex{}, map[net.Conn]*Shard{}},
		ShardNumToShard:    make(map[int]*Shard),
		ShardInterListener: shardInterListener,
		// Shard to intra shards
		// Consider removing shardIntraShadListener and only have it during boot
		ShardIntraListener:        shardIntraListener,
		InterIP:                   interIP,
		IntraIP:                   intraIP,
		InterPort:                 interShardPort,
		IntraPort:                 intraShardPort,
		connected:                 make(chan bool),
		ConnectionAdded:           make(chan bool),
		receiveMissingRequest:     make(chan bool),
		receiveAllMissingRequests: make(chan bool),
		receiveRetrieved:          make(chan bool),
		receiveAllRetrieved:       make(chan bool),
		receiveAllRequestedShards: make(chan bool),
		receiveRequestedShard:     make(chan bool),
		requestedTxOutsMap:        &TxOutsLockedMap{&sync.RWMutex{}, map[int]map[wire.OutPoint]*UtxoEntry{}},
		retrievedTxOutsMap:        &TxOutsLockedMap{&sync.RWMutex{}, map[int]map[wire.OutPoint]*UtxoEntry{}},
		cachedBlock:               &FetchedBlockToSend{&sync.RWMutex{}, nil},
	}
	return shard
}

// Handle instruction from coordinator to request the block
func (shard *Shard) handleRequestBlock(header *HeaderGob) {
	logging.Println("Received request to request block")
	// Start waiting for requests and responds from shard immediately
	// Start the waiting routine before starting to send anything
	shard.fetchedBlockShards = &FetchedBlocksLockedMap{&sync.RWMutex{}, map[net.Conn]*wire.MsgBlock{}, 0}

	go shard.AwaitRequestedTxOuts()
	// Start Listening for responses on your missing TxOuts from intershard
	go shard.AwaitRetrievedTxOuts()

	header.Salt = "bar"
	header.Index = shard.Index
	header.ShardNum = shard.NumShards

	// Send a request for txs from other shards
	// Thes should have some logic, currently its 1:1
	shard.interShards.RLock()
	for _, s := range shard.interShards.Shards {
		enc := s.Enc

		headerToSend := Message{
			Cmd:  "SNDBLOCK",
			Data: header,
		}

		err := enc.Encode(headerToSend)
		if err != nil {
			logging.Println(err, "Encode failed for struct: %#v", headerToSend)
		}

	}
	shard.interShards.RUnlock()

	start := time.Now()
	for i := 0; i < shard.interShards.NumShards(); i++ {
		<-shard.receiveRequestedShard
	}
	elapsed := time.Since(start).Seconds()
	logging.Println("Fetching from remote shards took", elapsed)

	msgBlockShard := wire.NewMsgBlock(header.Header)

	start = time.Now()
	for _, blockShard := range shard.fetchedBlockShards.fetchedBlocks {
		for _, tx := range blockShard.Transactions {
			msgBlockShard.AddTransaction(tx)
		}
	}
	elapsed = time.Since(start).Seconds()
	logging.Println("Constructing block to process took", elapsed)

	start = time.Now()
	_, err := shard.ShardMaybeAcceptBlock(msgBlockShard, shard.fetchedBlockShards.Flags)
	elapsed = time.Since(start).Seconds()
	logging.Println("Processing block shard took", elapsed)

	if err != nil {
		logging.Println(err)
		shard.NotifyOfBadBlock()
		// return!
		return
	}

	coordEnc := shard.CoordConn.Enc
	doneMsg := Message{
		Cmd: "SHARDDONE",
	}

	// Send conformation to coordinator!
	err = coordEnc.Encode(doneMsg)
	if err != nil {
		logging.Println(err, "Encode failed for struct: %#v", doneMsg)
	}
}

func (shard *Shard) cacheFetchedBlock(currentHash *chainhash.Hash) {
	shard.cachedBlock.Lock()
	defer shard.cachedBlock.Unlock()
	if shard.cachedBlock.fetchedBlock == nil {
		var err error
		shard.cachedBlock.fetchedBlock, err = shard.Chain.BlockByHash(currentHash)
		if err != nil {
			logging.Println("Unable to fetch block", err)
		}
		return
	}
	if shard.cachedBlock.fetchedBlock.Hash().IsEqual(currentHash) {
		// logging.Println("Block", shard.cachedBlock.fetchedBlock.Hash(), "already fetched")
		return
	}
	var err error
	shard.cachedBlock.fetchedBlock, err = shard.Chain.BlockByHash(currentHash)
	if err != nil {
		logging.Println("Unable to fetch block", err)
	}
	return
}

// Handle request for a block shard by another shard
func (shard *Shard) handleSendBlock(header *HeaderGob, conn net.Conn) {
	logging.Println("Received request to send a block shard")

	start := time.Now()
	blockHash := header.Header.BlockHash()
	shard.cacheFetchedBlock(&blockHash)
	elapsed := time.Since(start).Seconds()
	logging.Println("Fetching block shard took", elapsed)

	// salt := header.Salt
	requestingShardIndex := header.Index

	logging.Println("Got a request from shard idx", requestingShardIndex)

	// for _, tx := range block.TransactionsMap() {
	// 	spew.Dump(tx)
	// }
	start = time.Now()
	shard.cachedBlock.RLock()
	blockHeader := shard.cachedBlock.fetchedBlock.MsgBlock().Header
	blockShardToSend := wire.NewMsgBlock(&blockHeader)
	for _, tx := range shard.cachedBlock.fetchedBlock.Transactions() {
		// logging.Println("Hash exists?", tx.Hash())
		txHash := tx.Hash()

		shardNum := binary.BigEndian.Uint64(txHash[:]) % uint64(header.ShardNum)
		if shardNum == uint64(requestingShardIndex) {
			blockShardToSend.AddTransaction(tx.MsgTx())
		}
	}
	elapsed = time.Since(start).Seconds()
	logging.Println("Constructing", elapsed)

	// Create a gob of serialized msgBlock
	start = time.Now()
	msg := Message{
		Cmd: "PRCBLOCK",
		Data: RawBlockGob{
			Block:  blockShardToSend,
			Flags:  BFNone,
			Height: header.Height,
		},
	}
	//Actually write the GOB on the socket
	interShard := shard.interShards.GetShard(conn)
	enc := interShard.Enc
	err := enc.Encode(msg)
	if err != nil {
		logging.Println(err, "Encode failed for struct: %#v", msg)
	}
	shard.cachedBlock.RUnlock()
	logging.Println("Sent block for process", shard.cachedBlock.fetchedBlock.Hash())
	elapsed = time.Since(start).Seconds()
	logging.Println("Sending blockshard", elapsed)

}

// Receive a list of ip:port from coordinator, to which this shard will connect
func (shard *Shard) handleConnectInterShard(receivedShardAddresses *DHTGob) {
	logging.Println("My Index is ", shard.Index)

	addressesDHT := receivedShardAddresses.DHTTable
	logging.Println("Connecting to", len(addressesDHT), "intershards")

	for shardIdx, address := range addressesDHT {
		logging.Println("Connecting to shard idx", shardIdx)
		logging.Println("Shard connecting to shard on " + address.String())
		connection, err := net.Dial("tcp", address.String())
		if err != nil {
			fmt.Println(err)
		}
		logging.Println("Dial connection", connection)

		enc := gob.NewEncoder(connection)
		dec := gob.NewDecoder(connection)

		shardConn := NewShardConnection(connection, address.Port, 0, enc, dec) // TODO: change
		shard.RegisterShard(shardConn)
		<-shard.ConnectionAdded
		go shard.ReceiveInterShard(shardConn)
	}

	// Send connection done to coordinator
	enc := shard.CoordConn.Enc
	msg := Message{
		Cmd: "CONCTDONE",
	}
	err := enc.Encode(msg)
	if err != nil {
		logging.Println(err, "Encode failed for struct: %#v", msg)
	}

}

func (shard *Shard) handleProcessBlock(receivedBlock *RawBlockGob, conn net.Conn) {
	logging.Print("Received GOB data")

	msgBlockShard := receivedBlock.Block
	shard.fetchedBlockShards.Lock()
	shard.fetchedBlockShards.fetchedBlocks[conn] = msgBlockShard
	shard.fetchedBlockShards.Flags = receivedBlock.Flags
	shard.fetchedBlockShards.Unlock()
	shard.receiveRequestedShard <- true

}

func (shard *Shard) receive() {
	fmt.Println("Shard started recieving")

	shard.handleCoordMessages()

}

func (shard *Shard) handleCoordMessages() {

	gob.Register(HeaderGob{})
	gob.Register(AddressesGob{})
	gob.Register(DHTGob{})

	dec := shard.CoordConn.Dec
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
		// Handle connecting to intershards
		case "SHARDCON":
			dhtTable := msg.Data.(DHTGob)
			shard.handleConnectInterShard(&dhtTable)
		// Get DHT from coordintor and connect to all
		case "SHARDDHT":
			DHTGob := msg.Data.(DHTGob)
			shard.handleConnectIntraShards(DHTGob.Index, DHTGob.DHTTable)
		// handle an instruction from coordinator to request a block
		case "REQBLOCK":
			header := msg.Data.(HeaderGob)
			shard.handleRequestBlock(&header)
		// Receive a combined bloom filter from coordinator
		case "BADBLOCK":
			shard.handleBadBlock()
		default:
			logging.Println("Command '", cmd, "' is not registered.")
		}

	}
}

func (shard *Shard) handleInterShardMessages(conn net.Conn) {

	gob.Register(RawBlockGob{})
	gob.Register(HeaderGob{})

	interShard := shard.interShards.GetShard(conn)
	dec := interShard.Dec
	for {
		var msg Message
		err := dec.Decode(&msg)
		if err != nil {
			logging.Println("Error decoding GOB data:", err)
			// Clean terminate
			// shard.Finish <- true
			return
		}
		cmd := msg.Cmd
		logging.Println("Got cmd ", cmd)

		// handle according to received command
		switch cmd {
		// handle a request for block shard coming from another shard
		case "SNDBLOCK":
			header := msg.Data.(HeaderGob)
			go shard.handleSendBlock(&header, conn)

		// Messages related to processing a block
		case "PRCBLOCK":
			logging.Println("Received instruction to process block from intrashard")
			block := msg.Data.(RawBlockGob)
			shard.handleProcessBlock(&block, conn)
		default:
			logging.Println("Command '", cmd, "' is not registered.")
		}

	}
}

// Handle messages from intra shards
func (shard *Shard) handleIntraShardMessages(conn net.Conn) {

	gob.Register(map[wire.OutPoint]*UtxoEntry{})

	intraShard := shard.intraShards.GetShard(conn)
	dec := intraShard.Dec
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
		case "REQOUTS":
			missingTxOuts := msg.Data.(map[wire.OutPoint]*UtxoEntry)
			shard.handleRequestedOuts(conn, missingTxOuts)
		// handle received requested TxOuts
		case "RETOUTS":
			retrievedTxOuts := msg.Data.(map[wire.OutPoint]*UtxoEntry)
			shard.handleRetreivedTxOuts(conn, retrievedTxOuts)
		default:
			logging.Println("Command '", cmd, "' is not registered.")
		}

	}
}

// StartShard starts the shard, the shard starts receiveing on its listening port
func (shard *Shard) StartShard() {
	// Receive messages from coordinator
	go shard.receive()
	for {
		select {
		// Handle shard connect/disconnect
		case s := <-shard.registerShard:
			shard.interShards.InsertShard(s.Socket, s)
			fmt.Println("Added new shard to shard!")
			shard.ConnectionAdded <- true
		case s := <-shard.unregisterShard:
			shard.interShards.RemoveShard(s.Socket)
		}
	}
	// TODO: Here we can add listening to intra shards rejoining
}

// RegisterShard Recoreds a connection to another shard
func (shard *Shard) RegisterShard(s *Shard) {
	shard.registerShard <- s
}

// ReceiveIntraShard is respoinsible for receiving messages from intra shards
func (shard *Shard) ReceiveIntraShard(s *Shard) {
	logging.Println("Started receiving from intra shard on conn", s.Socket)
	shard.handleIntraShardMessages(s.Socket)
}

// ReceiveInterShard is respoinsible for receiving messages from inter shards
func (shard *Shard) ReceiveInterShard(s *Shard) {
	logging.Println("Started receiving from inter shard on conn", s.Socket)
	shard.handleInterShardMessages(s.Socket)
}

// NotifyOfBadBlock sends a message to the coordinator that the block is illegal
// for some reason, the block will not be connected
func (shard *Shard) NotifyOfBadBlock() {
	logging.Println("Illegal block, send BADBLOCK to coordinator")
	enc := shard.CoordConn.Enc

	err := enc.Encode(
		Message{
			Cmd: "BADBLOCK",
		})
	if err != nil {
		logging.Println(err, "Encode failed for struct BADBLOCK")
	}

}

// handleBadBlock receives a notice of a bad block, needs to stop processing
// the current block
func (shard *Shard) handleBadBlock() {
	logging.Println("Received bad block indication form coordinator")
	// TODO: we need to add some rules, in case this is received after,
	// we need to revert the changes made by this shard

	// Release the no colide lock if the block is bad
	shard.missingTxOuts <- make(map[wire.OutPoint]*UtxoEntry)
}

// handleMissingTxOuts receives a map of the missing Txs from coordinator
func (shard *Shard) handleMissingTxOuts(conn net.Conn, missingTxOuts map[wire.OutPoint]*UtxoEntry) {
	logging.Println("Received map of missing TxOuts")
	shard.missingTxOuts <- missingTxOuts

}

// HandleRequestInfo sends the coordinator the IP the shard is listening to inter-shard
// and intera-shard communication
func (shard *Shard) HandleRequestInfo(shardIndex int) {
	shard.Index = shardIndex
	logging.Println("Registered own index as", shard.Index)
	var addresses []*net.TCPAddr

	interAddr := net.TCPAddr{
		IP:   shard.InterIP,
		Port: shard.InterPort,
	}

	intraAddr := net.TCPAddr{
		IP:   shard.IntraIP,
		Port: shard.IntraPort,
	}

	addresses = append(addresses, &interAddr)
	addresses = append(addresses, &intraAddr)
	// All data is sent in gobs

	msg := Message{
		Cmd: "RPLYINFO",
		Data: AddressesGob{
			Addresses: addresses,
			Index:     shard.Index,
		},
	}
	logging.Print("Sending shard ports inter", addresses[0])
	logging.Print("Sending shard ports intra", addresses[1])

	gob.Register(AddressesGob{})
	//Actually write the GOB on the socket
	enc := shard.CoordConn.Enc
	err := enc.Encode(msg)
	if err != nil {
		logging.Println("Error encoding addresses GOB data:", err)
		return
	}

}

// AwaitShards waits for numShards - 1 shards to connect, to form
// an n-to-n network of shards
func (shard *Shard) AwaitShards(numShards int) {
	shard.NumShards = numShards
	logging.Println("Awaiting shards")
	// The awaiting shards are waiting for all the shards above them
	logging.Println("Waiting for", numShards-shard.Index-1, "To connect to me")
	logging.Println("Waiting for", shard.Index, "connection to be made")
	for i := 0; i < numShards-shard.Index-1; i++ {
		connection, _ := shard.ShardIntraListener.Accept()
		logging.Println("Received connection", connection)
		// The connected shard is expected to sleep before sending its index
		dec := gob.NewDecoder(connection)
		enc := gob.NewEncoder(connection)
		var msg Message
		err := dec.Decode(&msg)
		if err != nil {
			logging.Println("Error decoding GOB data:", err)
			return
		}
		cmd := msg.Cmd
		logging.Println("Got cmd ", cmd)
		if cmd == "SHARDNUM" {
		} else {
			logging.Println("Did not receive shard number")
		}
		shardIndex := msg.Data.(int)
		shardConn := NewShardConnection(connection, 0, shardIndex, enc, dec)
		logging.Println("Connected to shard", shardIndex, "on", shardConn.Socket)
		// Save mappings both ways for send and receive
		// TODO: This is basically Register shard
		shard.ShardNumToShard[shardIndex] = shardConn
		shard.intraShards.InsertShard(connection, shardConn)
		go shard.ReceiveIntraShard(shardConn)
	}
	// Send connection done to coordinator
	logging.Println("Received all shards connection")
	for i := 0; i < shard.Index; i++ {
		<-shard.connected
	}

	enc := shard.CoordConn.Enc
	msg := Message{
		Cmd: "CONCTDONE",
	}
	err := enc.Encode(msg)
	if err != nil {
		logging.Println(err, "Encode failed for struct: %#v", msg)
	}
}

// handleConnectIntraShards receives a map of addresses, connects to all, and registers
// the connection in a local int->conn table
func (shard *Shard) handleConnectIntraShards(index int, dhtTable map[int]net.TCPAddr) {
	for idx, address := range dhtTable {
		logging.Println("Idx", idx)
		logging.Println("Address", address)
	}
	logging.Println("Connecting to", len(dhtTable)-1, "shards")
	for shardIndex, address := range dhtTable {
		// Only connect to shards "before" you, this should not
		// go above it anyway
		if shardIndex >= shard.Index {
			continue
		}
		// Too bad this must be a string
		connection, err := net.Dial("tcp", address.IP.String()+":"+strconv.Itoa(address.Port))
		if err != nil {
			fmt.Println(err)
		}

		logging.Println("Dial connection", connection)

		enc := gob.NewEncoder(connection)
		dec := gob.NewDecoder(connection)
		msg := Message{
			Cmd:  "SHARDNUM",
			Data: shard.Index,
		}
		err = enc.Encode(msg)
		if err != nil {
			logging.Println(err, "Encode failed for struct: %#v", msg)
		}
		shardConn := NewShardConnection(connection, 0, shardIndex, enc, dec)
		logging.Println("Connected to shard", shardIndex, "on", shardConn.Socket)
		// Save mappings both ways for send and receive
		shard.ShardNumToShard[shardIndex] = shardConn
		shard.intraShards.InsertShard(connection, shardConn)
		go shard.ReceiveIntraShard(shardConn)
		// Release the lock on CONCTDONE
		shard.connected <- true
	}
}

// SendMissingTxOuts sends each shard a map of all the outputs the sending shard is missing
// It then awaits for a response from all shards (Response can be empty)
func (shard *Shard) SendMissingTxOuts(missingTxOuts map[int]map[wire.OutPoint]*UtxoEntry) {
	for shardNum, intraShard := range shard.ShardNumToShard {
		// Send on the open socket with the relevant shard
		logging.Println("Sending missing outs to", shardNum)
		// for txOut := range missingTxOuts[shardNum] {
		// 	logging.Println("Missing", txOut)
		// }
		enc := intraShard.Enc
		// logging.Println("Sending info on", intraShard.Socket)

		msg := Message{
			Cmd:  "REQOUTS",
			Data: missingTxOuts[shardNum],
		}
		err := enc.Encode(msg)
		if err != nil {
			logging.Println("Error encoding missing TxOuts:", err)
			return
		}
	}
}

// SendRequestedTxOuts sends each shard a map of all the TxOuts it has earlier
// requested.
func (shard *Shard) SendRequestedTxOuts(requestedTxOuts map[int]map[wire.OutPoint]*UtxoEntry) {
	// for shardIdx, fetched := range requestedTxOuts {
	// 	for utxo := range fetched {
	// 		logging.Println("For shard", shardIdx, "Fetched", utxo)
	// 	}
	// }
	for shardNum, intraShard := range shard.ShardNumToShard {
		// Send on the open socket with the relevant shard
		logging.Println("Sending retrieved outs to", shardNum)
		enc := intraShard.Enc

		msg := Message{
			Cmd: "RETOUTS",
			// A Gob struct is needed so nil maps can also be sent
			Data: requestedTxOuts[shardNum],
		}
		err := enc.Encode(msg)
		if err != nil {
			logging.Println("Error encoding addresses GOB data:", err)
			return
		}
	}
}

// AwaitRequestedTxOuts waits for missing TxOuts from all shards
// Once all answers are received, unlocks the waiting channel
func (shard *Shard) AwaitRequestedTxOuts() {
	logging.Println("Waiting for requests")
	// Create the map that the will wait for requests in
	for i := 0; i < shard.NumShards-1; i++ {
		<-shard.receiveMissingRequest
	}
	// Unlock wait for all shards to send requests
	shard.receiveAllMissingRequests <- true
}

// AwaitRetrievedTxOuts waits for the retrieved TxOuts which were
// previously requested by this shard.
// Unlocks when all are received
func (shard *Shard) AwaitRetrievedTxOuts() {
	logging.Println("Waiting for retrieved")
	// Create the map that the will wait for requests in
	for i := 0; i < shard.NumShards-1; i++ {
		<-shard.receiveRetrieved
	}
	// Unlock wait for all shards to send response
	shard.receiveAllRetrieved <- true
}

func (shard *Shard) handleRequestedOuts(conn net.Conn, missingTxOuts map[wire.OutPoint]*UtxoEntry) {
	// socketToShard maps sockets to shard indexes
	shard.requestedTxOutsMap.Lock()
	shardIndex := shard.intraShards.GetShard(conn).Index
	shard.requestedTxOutsMap.TxOutsMap[shardIndex] = missingTxOuts
	shard.requestedTxOutsMap.Unlock()
	// for txOut := range missingTxOuts {
	// 	logging.Println("Got missing out", txOut)
	// }
	// Unlock wait for each shard to send missing
	shard.receiveMissingRequest <- true
}

func (shard *Shard) handleRetreivedTxOuts(conn net.Conn, retreivedTxOuts map[wire.OutPoint]*UtxoEntry) {
	// socketToShard maps sockets to shard indexes
	shard.retrievedTxOutsMap.Lock()
	shardIndex := shard.intraShards.GetShard(conn).Index
	shard.retrievedTxOutsMap.TxOutsMap[shardIndex] = retreivedTxOuts
	shard.retrievedTxOutsMap.Unlock()
	//for txOut := range retreivedTxOuts {
	//	logging.Println("Got the retreived TxOut", txOut)
	//}
	// Unlock wait for each shard to send missing
	shard.receiveRetrieved <- true
}
