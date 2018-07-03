package blockchain

import (
	"encoding/gob"
	"fmt"
	logging "log"
	"net"
	"strconv"

	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/btcsuite/btcutil/bloom"
)

// Shard is a single node in a sharded bitcoin client cluster
type Shard struct {
	Chain                  *BlockChain
	Socket                 net.Conn         // Connection to coordinator
	interShards            map[*Shard]bool  // A map of shards connected to this shard
	ShardNumToShard        map[int]*Shard   // A map between shard index and shard connection
	socketToShardNum       map[net.Conn]int // A reverse map between sockent and shard number
	registerShard          chan *Shard
	unregisterShard        chan *Shard
	Port                   int // Saves the port the shard is listening to other shards
	Index                  int // Save the shard index in the coords array
	ShardInterListener     net.Listener
	ShardIntraListener     net.Listener
	intersectFilter        chan *FilterGob
	missingTxOuts          chan (map[wire.OutPoint]*UtxoEntry)
	ReceivedCombinedFilter *FilterGob
	NumShards              int // Save the number of shards in total, needed to calc the mod
	// Shard-to-shard comms
	IP                        net.IP
	InterPort                 int
	IntraPort                 int
	receiveAllMissingRequests chan bool // A channel to unlock the requests from all shards
	receiveMissingRequest     chan bool // A channel to unlock the requests from all shards
}

// NewShardConnection Creates a new shard connection for a coordinator to use.
// It has a connection and a channel to receive data from the server
func NewShardConnection(connection net.Conn, shardPort int, index int) *Shard {
	shard := &Shard{
		Socket: connection, // The connection to the coordinator
		Port:   shardPort,  // Will store the prot the shard is listening to for other shards
		Index:  index,
	}
	return shard
}

// NewShard Creates a new shard for a coordinator to use.
// It has a connection and a channel to receive data from the server
func NewShard(shardInterListener net.Listener, shardIntraListener net.Listener, connection net.Conn, blockchain *BlockChain, ip net.IP, interShardPort int, intraShardPort int) *Shard {
	shard := &Shard{
		Chain:           blockchain,
		registerShard:   make(chan *Shard),
		unregisterShard: make(chan *Shard),
		intersectFilter: make(chan *FilterGob),
		missingTxOuts:   make(chan map[wire.OutPoint]*UtxoEntry),
		Socket:          connection,
		// Shard-to-shard comms
		interShards:        make(map[*Shard]bool),
		ShardNumToShard:    make(map[int]*Shard),
		socketToShardNum:   make(map[net.Conn]int),
		ShardInterListener: shardInterListener,
		// Shard to intra shards
		// Consider removing shardIntraShadListener and only have it during boot
		ShardIntraListener:        shardIntraListener,
		IP:                        ip,
		InterPort:                 interShardPort,
		IntraPort:                 intraShardPort,
		receiveMissingRequest:     make(chan bool),
		receiveAllMissingRequests: make(chan bool),
	}
	return shard
}

// Handle instruction from coordinator to request the block
func (shard *Shard) handleRequestBlock(header *HeaderGob, conn net.Conn) {
	logging.Println("Received request to request block")

	// Send a request for txs from other shards
	// Thes should have some logic, currently its 1:1
	for s := range shard.interShards {

		enc := gob.NewEncoder(s.Socket)

		headerToSend := Message{
			Cmd:  "SNDBLOCK",
			Data: header,
		}

		err := enc.Encode(headerToSend)
		if err != nil {
			logging.Println(err, "Encode failed for struct: %#v", headerToSend)
		}

	}

}

// Handle the received combined filter and missing Txs from coordinator
func (shard *Shard) handleFilterCombined(filterData *FilterGob, conn net.Conn) {
	logging.Println("Recived filter", filterData)
	shard.intersectFilter <- filterData

}

// Handle request for a block shard by another shard
//func (shard *Shard) handleSendBlock(header *HeaderGob, conn net.Conn) {
//	logging.Println("Received request to send a block shard")
//
//	msgBlock := shard.SQLDB.FetchTXs(header.Header.BlockHash())
//	msgBlock.Header = *header.Header
//
//	// Create a gob of serialized msgBlock
//	msg := Message{
//		Cmd: "PRCBLOCK",
//		Data: RawBlockGob{
//			Block:  msgBlock,
//			Flags:  BFNone,
//			Height: header.Height,
//		},
//	}
//	//Actually write the GOB on the socket
//	enc := gob.NewEncoder(conn)
//	err := enc.Encode(msg)
//	if err != nil {
//		logging.Println(err, "Encode failed for struct: %#v", msg)
//	}
//
//}
//
// Receive a list of ip:port from coordinator, to which this shard will connect
func (shard *Shard) handleShardConnect(receivedShardAddresses *AddressesGob, conn net.Conn) {
	logging.Println("Received request to connect to shards")

	for _, address := range receivedShardAddresses.Addresses {
		logging.Println("Shard connecting to shard on " + address.String())
		connection, err := net.Dial("tcp", address.String())
		if err != nil {
			fmt.Println(err)
		}

		shardConn := NewShardConnection(connection, address.Port, 0) // TODO: change
		shard.RegisterShard(shardConn)
		go shard.ReceiveShard(shardConn)
	}
	// Send connection done to coordinator
	enc := gob.NewEncoder(conn)
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

	coordEnc := gob.NewEncoder(shard.Socket)

	doneMsg := Message{
		Cmd: "SHARDDONE",
	}

	// Process the transactions
	// Create a new block node for the block and add it to the in-memory
	block := btcutil.NewBlockShard(msgBlockShard)

	// Store the txs in database, this could be postponed until after validation
	//StoreBlockShard(shard.SQLDB, msgBlockShard)

	// shard does not need the parent node
	blockNode := NewBlockNode(&msgBlockShard.Header, nil)
	// Set node height to block height according to coordinator
	blockNode.height = receivedBlock.Height

	_, err := shard.ShardConnectBestChain(blockNode, block)
	if err != nil {
		logging.Println(err)
		shard.NotifyOfBadBlock()
		// return!
		return
	}

	logging.Println("Done processing block, sending SHARDDONE")

	// Send conformation to coordinator!
	err = coordEnc.Encode(doneMsg)
	if err != nil {
		logging.Println(err, "Encode failed for struct: %#v", doneMsg)
	}

}

func (shard *Shard) receive() {
	fmt.Println("Shard started recieving")

	shard.handleSmartMessages(shard.Socket)

}

func (shard *Shard) handleSmartMessages(conn net.Conn) {

	gob.Register(RawBlockGob{})
	gob.Register(HeaderGob{})
	gob.Register(AddressesGob{})
	gob.Register(FilterGob{})
	gob.Register(MissingTxOutsGob{})
	gob.Register(DHTGob{})

	for {
		logging.Println("Waiting to decode on", conn)
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
		case "SHARDCON":
			addresses := msg.Data.(AddressesGob)
			shard.handleShardConnect(&addresses, conn)
		// Get DHT from coordintor and connect to all
		case "SHARDDHT":
			DHTGob := msg.Data.(DHTGob)
			shard.handleConnectToShards(DHTGob.Index, DHTGob.DHTTable)
		// handle an instruction from coordinator to request a block
		case "REQBLOCK":
			header := msg.Data.(HeaderGob)
			shard.handleRequestBlock(&header, conn)
		// handle a request for block shard coming from another shard
		//case "SNDBLOCK":
		//	header := msg.Data.(HeaderGob)
		//	shard.handleSendBlock(&header, conn)

		// Messages related to processing a block
		case "PRCBLOCK":
			block := msg.Data.(RawBlockGob)
			shard.handleProcessBlock(&block, conn)
		// Receive a combined bloom filter from coordinator
		case "BLOOMCOM":
			filter := msg.Data.(FilterGob)
			shard.handleFilterCombined(&filter, conn)
		// Get a message from coordinator that block is bad, drop process
		// of current block
		case "BADBLOCK":
			shard.handleBadBlock(conn)
		case "MISSOUTS":
			missingTxOutsGob := msg.Data.(MissingTxOutsGob)
			shard.handleMissingTxOuts(conn, missingTxOutsGob.MissingTxOuts)
		// handle request for missing TxOuts from all shards
		case "REQOUTS":
			logging.Println("Got command REQOUTS")
			missingTxOutsGob := msg.Data.(MissingTxOutsGob)
			shard.handleRequestedOuts(conn, missingTxOutsGob.MissingEmptyOuts)
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
		case connection := <-shard.registerShard:
			shard.interShards[connection] = true
			fmt.Println("Added new shard to shard!")
		case connection := <-shard.unregisterShard:
			if _, ok := shard.interShards[connection]; ok {
				delete(shard.interShards, connection)
				fmt.Println("A connection has terminated!")
			}
		}
	}
}

// RegisterShard Recoreds a connection to another shard
func (shard *Shard) RegisterShard(s *Shard) {
	shard.registerShard <- s
}

// ReceiveShard is respoinsible for receiving messages from other shards
func (shard *Shard) ReceiveShard(s *Shard) {
	logging.Println("Started receiving on conn", s.Socket)
	shard.handleSmartMessages(s.Socket)
}

// SendEmptyBloomFilter sends an empty bloomfilter to the coordintor. This
// is an indication that the shard has no transactions to process, and
// is the same as SHARDDONE
func (shard *Shard) SendEmptyBloomFilter() {
	logging.Println("Sending Empty bloomfilter to coordinator")
	enc := gob.NewEncoder(shard.Socket)

	err := enc.Encode(
		Message{
			Cmd: "BLOOMFLT",
			Data: FilterGob{
				InputFilter: nil,
				TxFilter:    nil,
			},
		})
	if err != nil {
		logging.Println(err, "Encode failed for empty filter")
	}
}

// SendInputBloomFilter sends a bloom filter of transaction inputs to the
// coordinator and waits for either conformation for that no intersection is
// possible or request for additional information
// In addtion, a filter of all transaction hashes is sent
// TODO: change list to map[]struct{}
func (shard *Shard) SendInputBloomFilter(inputFilter *bloom.Filter, txFilter *bloom.Filter, missingTxOutsList *[]*wire.OutPoint) {
	logging.Println("Sending bloom filter of transactions to coordinator")
	enc := gob.NewEncoder(shard.Socket)

	// Print all missing inputs
	logging.Println("Sending missing Txs:")
	for _, input := range *missingTxOutsList {
		logging.Println(input)
	}

	err := enc.Encode(
		Message{
			Cmd: "BLOOMFLT",
			Data: FilterGob{
				InputFilter:   inputFilter.MsgFilterLoad(),
				TxFilter:      txFilter.MsgFilterLoad(),
				MissingTxOuts: *missingTxOutsList,
			},
		})
	if err != nil {
		logging.Println(err, "Encode failed for struct: %#v", inputFilter)
	}

	// Wait for intersectFilter from coordinator
	shard.ReceivedCombinedFilter = <-shard.intersectFilter
	logging.Println("Received combined filter")
	return
}

// SendMatchingAndMissingTxOuts sends a list of matching transaction inputs to the coordinator
// This list could be empty,
func (shard *Shard) SendMatchingAndMissingTxOuts(matchingTxOuts []*wire.OutPoint, missingTxOuts map[wire.OutPoint]*UtxoEntry) map[wire.OutPoint]*UtxoEntry {
	logging.Println("Sending bloom filter of transactions to coordinator")
	enc := gob.NewEncoder(shard.Socket)

	err := enc.Encode(
		Message{
			Cmd: "MATCHTXS",
			Data: MissingTxOutsGob{
				MatchingTxOuts: matchingTxOuts,
				MissingTxOuts:  missingTxOuts,
			},
		})
	if err != nil {
		logging.Println(err, "Encode failed for struct: %#v", matchingTxOuts)
	}

	receivedMissingTxOuts := <-shard.missingTxOuts
	logging.Println("Received list of missing TxOuts from coordinator")
	return receivedMissingTxOuts
}

// NotifyOfBadBlock sends a message to the coordinator that the block is illegal
// for some reason, the block will not be connected
func (shard *Shard) NotifyOfBadBlock() {
	logging.Println("Illegal block, send BADBLOCK to coordinator")
	enc := gob.NewEncoder(shard.Socket)

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
func (shard *Shard) handleBadBlock(conn net.Conn) {
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
func (shard *Shard) HandleRequestInfo(conn net.Conn, shardIndex int) {
	shard.Index = shardIndex
	logging.Println("Registered own index as", shard.Index)
	var addresses []*net.TCPAddr

	interAddr := net.TCPAddr{
		IP:   shard.IP,
		Port: shard.InterPort,
	}

	intraAddr := net.TCPAddr{
		IP:   shard.IP,
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

	//Actually write the GOB on the socket
	enc := gob.NewEncoder(conn)
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
	for i := 0; i < numShards-shard.Index-1; i++ {
		connection, _ := shard.ShardIntraListener.Accept()
		logging.Println("Received connection", connection)
		// The connected shard is expected to sleep before sending its index
		dec := gob.NewDecoder(connection)
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
		shardConn := NewShardConnection(connection, 0, shardIndex)
		logging.Println("Connected to shard", shardIndex, "on", shardConn.Socket)
		// Save mappings both ways for send and receive
		shard.ShardNumToShard[shardIndex] = shardConn
		shard.socketToShardNum[connection] = shardIndex
		go shard.ReceiveShard(shardConn)
	}
	// Send connection done to coordinator
	logging.Println("Received all shards connection")
	//for len(shard.ShardNumToShard) < shard.NumShards-1 {
	//	// TODO TODO TODO: Busy wait
	//	logging.Println("BusyWait")
	//}
	for _, con := range shard.ShardNumToShard {
		logging.Println("Should go receive on", con.Socket)
	}
	logging.Println("Done connecting to all shards")

	enc := gob.NewEncoder(shard.Socket)
	msg := Message{
		Cmd: "CONCTDONE",
	}
	err := enc.Encode(msg)
	if err != nil {
		logging.Println(err, "Encode failed for struct: %#v", msg)
	}
}

// handleConnectToShards receives a map of addresses, connects to all, and registers
// the connection in a local int->conn table
func (shard *Shard) handleConnectToShards(index int, dhtTable map[int]net.TCPAddr) {
	// Set you own index in the dht
	// if index != shard.Index {
	// 	logging.Panicln("Coordinator is talking to wrong shard")
	// }

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
		msg := Message{
			Cmd:  "SHARDNUM",
			Data: shard.Index,
		}
		err = enc.Encode(msg)
		if err != nil {
			logging.Println(err, "Encode failed for struct: %#v", msg)
		}
		shardConn := NewShardConnection(connection, 0, shardIndex)
		logging.Println("Connected to shard", shardIndex, "on", shardConn.Socket)
		// Save mappings both ways for send and receive
		shard.ShardNumToShard[shardIndex] = shardConn
		shard.socketToShardNum[connection] = shardIndex
		go shard.ReceiveShard(shardConn)
	}
}

// SendMissingTxOuts sends each shard a map of all the outputs the sending shard is missing
// It then awaits for a response from all shards (Response can be empty)
func (shard *Shard) SendMissingTxOuts(missingTxOuts map[int]map[wire.OutPoint]struct{}) {
	for shardNum, intraShard := range shard.ShardNumToShard {
		// Send on the open socket with the relevant shard
		logging.Println("Sending missing outs to", shardNum)
		//for txOut := range missingTxOuts[shardNum] {
		//	logging.Println("Missing", txOut)
		//}
		enc := gob.NewEncoder(intraShard.Socket)
		logging.Println("Sending info on", intraShard.Socket)

		msg := Message{
			Cmd: "REQOUTS",
			Data: MissingTxOutsGob{
				// This can be nil, but needs to be sent
				MissingEmptyOuts: missingTxOuts[shardNum],
			},
		}
		err := enc.Encode(msg)
		if err != nil {
			logging.Println("Error encoding addresses GOB data:", err)
			return
		}
	}
}

// AwaitMissingTxOuts waits for answers on missing TxOuts from all shards
// Once all answers are received, unlocks the waiting channel
func (shard *Shard) AwaitMissingTxOuts() {
	logging.Println("Waiting for requests")
	for i := 0; i < shard.NumShards-1; i++ {
		<-shard.receiveMissingRequest
	}
	// Unlock wait for all shards to send requests
	shard.receiveAllMissingRequests <- true
}

func (shard *Shard) handleRequestedOuts(conn net.Conn, missingOuts map[wire.OutPoint]struct{}) {
	logging.Println("Got request from shard", shard.socketToShardNum[conn])
	//for txOut := range missingOuts {
	//	logging.Println("Got missing out", txOut)
	//}
	// Unlock wait for each shard to send missing
	//shard.receiveMissingRequest <- true
}

// SendDeadToShards sends DEADBEEF
func (shard *Shard) SendDeadToShards() {
	for shardNum, shardConn := range shard.ShardNumToShard {
		logging.Println("Sending dead to", shardNum, "on", shardConn.Socket)
		enc := gob.NewEncoder(shardConn.Socket)

		err := enc.Encode(
			Message{
				Cmd: "DEADBEE",
			})
		if err != nil {
			logging.Panicln(err, "Encode failed for struct DEADBEE")
		}
	}
	logging.Println("Sending dead to coord", "on", shard.Socket)
	enc := gob.NewEncoder(shard.Socket)

	err := enc.Encode(
		Message{
			Cmd: "DEADBEE",
		})
	if err != nil {
		logging.Panicln(err, "Encode failed for struct DEADBEE")
	}
}
