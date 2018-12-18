// Copyright (c) 2013-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package main

import (
	_ "bytes"
	"flag"
	"fmt"
	_ "io"
	logging "log"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"encoding/gob"
	"encoding/json"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/blockchain/fullblocktests"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/database"
	"github.com/btcsuite/btcd/limits"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
)

// Config is used for to save values from json config file
type Config struct {
	Server struct {
		ServerLog             string `json:"server_log"`
		ServerShardsPort      string `json:"server_shards_port"`
		ServerShardsIP        string `json:"server_shards_ip"`
		ServerCoordsPort      string `json:"server_coords_port"`
		ServerTargetServer    string `json:"server_target_server"`
		ServerTargetShardPort string `json:"server_target_shard_port"`
		ServerDb              string `json:"server_db"`
	} `json:"server"`
	Shard struct {
		ShardLog       string `json:"shard_log"`
		ShardInterPort string `json:"shard_inter_port"`
		ShardIntraPort string `json:"shard_intra_port"`
		ShardInterIP   string `json:"shard_inter_ip"`
		ShardIntraIP   string `json:"shard_intra_ip"`
		ShardDb        string `json:"shard_db"`
	} `json:"shard"`
}

const (
	// testDbType is the database backend type to use for the tests.
	testDbType = "ffldb"

	// blockDataNet is the expected network in the test block data.
	blockDataNet = wire.MainNet
)

// isSupportedDbType returns whether or not the passed database type is
// currently supported.
func isSupportedDbType(dbType string) bool {
	supportedDrivers := database.SupportedDrivers()
	for _, driver := range supportedDrivers {
		if dbType == driver {
			return true
		}
	}

	return false
}

// LoadConfig loads the passed json file and returns a config struct
func LoadConfig(filename string) (Config, error) {
	var config Config
	fmt.Println("Config filename: ", filename)
	confFile, err := os.Open(filename)
	defer confFile.Close()
	if err != nil {
		return config, err
	}
	jsonParser := json.NewDecoder(confFile)
	err = jsonParser.Decode(&config)
	fmt.Println("config: ", config)
	return config, err
}

const (
	// blockDbNamePrefix is the prefix for the block database name.  The
	// database type is appended to this value to form the full block
	// database name.
	blockDbNamePrefix = "blocks"
)

var (
	cfg *config
)

// winServiceMain is only invoked on Windows.  It detects when btcd is running
// as a service and reacts accordingly.
var winServiceMain func() (bool, error)

// btcdMain is the real main function for btcd.  It is necessary to work around
// the fact that deferred functions do not run when os.Exit() is called.  The
// optional serverChan parameter is mainly used by the service code to be
// notified with the server once it is setup so it can gracefully stop it when
// requested from the service control manager.
// func btcdMain(serverChan chan<- *server) error {
// 	// Load configuration and parse command line.  This function also
// 	// initializes logging and configures it accordingly.
// 	tcfg, _, err := loadConfig()
// 	if err != nil {
// 		return err
// 	}
// 	cfg = tcfg
// 	defer func() {
// 		if logRotator != nil {
// 			logRotator.Close()
// 		}
// 	}()
//
// 	// Get a channel that will be closed when a shutdown signal has been
// 	// triggered either from an OS signal such as SIGINT (Ctrl+C) or from
// 	// another subsystem such as the RPC server.
// 	interrupt := interruptListener()
// 	defer btcdLog.Info("Shutdown complete")
//
// 	// Show version at startup.
// 	btcdLog.Infof("Version %s", version())
//
// 	// Enable http profiling server if requested.
// 	if cfg.Profile != "" {
// 		go func() {
// 			listenAddr := net.JoinHostPort("", cfg.Profile)
// 			btcdLog.Infof("Profile server listening on %s", listenAddr)
// 			profileRedirect := http.RedirectHandler("/debug/pprof",
// 				http.StatusSeeOther)
// 			http.Handle("/", profileRedirect)
// 			btcdLog.Errorf("%v", http.ListenAndServe(listenAddr, nil))
// 		}()
// 	}
//
// 	// Write cpu profile if requested.
// 	if cfg.CPUProfile != "" {
// 		f, err := os.Create(cfg.CPUProfile)
// 		if err != nil {
// 			btcdLog.Errorf("Unable to create cpu profile: %v", err)
// 			return err
// 		}
// 		pprof.StartCPUProfile(f)
// 		defer f.Close()
// 		defer pprof.StopCPUProfile()
// 	}
//
// 	// Perform upgrades to btcd as new versions require it.
// 	if err := doUpgrades(); err != nil {
// 		btcdLog.Errorf("%v", err)
// 		return err
// 	}
//
// 	// Return now if an interrupt signal was triggered.
// 	if interruptRequested(interrupt) {
// 		return nil
// 	}
//
// 	// Load the block database.
// 	db, err := loadBlockDB()
// 	if err != nil {
// 		btcdLog.Errorf("%v", err)
// 		return err
// 	}
// 	defer func() {
// 		// Ensure the database is sync'd and closed on shutdown.
// 		btcdLog.Infof("Gracefully shutting down the database...")
// 		db.Close()
// 	}()
//
// 	// Return now if an interrupt signal was triggered.
// 	if interruptRequested(interrupt) {
// 		return nil
// 	}
//
// 	// Drop indexes and exit if requested.
// 	//
// 	// NOTE: The order is important here because dropping the tx index also
// 	// drops the address index since it relies on it.
// 	if cfg.DropAddrIndex {
// 		if err := indexers.DropAddrIndex(db, interrupt); err != nil {
// 			btcdLog.Errorf("%v", err)
// 			return err
// 		}
//
// 		return nil
// 	}
// 	if cfg.DropTxIndex {
// 		if err := indexers.DropTxIndex(db, interrupt); err != nil {
// 			btcdLog.Errorf("%v", err)
// 			return err
// 		}
//
// 		return nil
// 	}
// 	if cfg.DropCfIndex {
// 		if err := indexers.DropCfIndex(db, interrupt); err != nil {
// 			btcdLog.Errorf("%v", err)
// 			return err
// 		}
//
// 		return nil
// 	}
//
// 	// Create server and start it.
// 	server, err := newServer(cfg.Listeners, db, activeNetParams.Params,
// 		interrupt)
// 	if err != nil {
// 		// TODO: this logging could do with some beautifying.
// 		btcdLog.Errorf("Unable to start server on %v: %v",
// 			cfg.Listeners, err)
// 		return err
// 	}
// 	defer func() {
// 		btcdLog.Infof("Gracefully shutting down the server...")
// 		server.Stop()
// 		server.WaitForShutdown()
// 		srvrLog.Infof("Server shutdown complete")
// 	}()
// 	server.Start()
// 	if serverChan != nil {
// 		serverChan <- server
// 	}
//
// 	// Wait until the interrupt signal is received from an OS signal or
// 	// shutdown is requested through one of the subsystems such as the RPC
// 	// server.
// 	<-interrupt
// 	return nil
// }

// removeRegressionDB removes the existing regression test database if running
// in regression test mode and it already exists.
func removeRegressionDB(dbPath string) error {
	// Don't do anything if not in regression test mode.
	if !cfg.RegressionTest {
		return nil
	}

	// Remove the old regression test database if it already exists.
	fi, err := os.Stat(dbPath)
	if err == nil {
		btcdLog.Infof("Removing regression test database from '%s'", dbPath)
		if fi.IsDir() {
			err := os.RemoveAll(dbPath)
			if err != nil {
				return err
			}
		} else {
			err := os.Remove(dbPath)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// dbPath returns the path to the block database given a database type.
func blockDbPath(dbType string) string {
	// The database name is based on the database type.
	dbName := blockDbNamePrefix + "_" + dbType
	if dbType == "sqlite" {
		dbName = dbName + ".db"
	}
	dbPath := filepath.Join(cfg.DataDir, dbName)
	return dbPath
}

// warnMultipleDBs shows a warning if multiple block database types are detected.
// This is not a situation most users want.  It is handy for development however
// to support multiple side-by-side databases.
func warnMultipleDBs() {
	// This is intentionally not using the known db types which depend
	// on the database types compiled into the binary since we want to
	// detect legacy db types as well.
	dbTypes := []string{"ffldb", "leveldb", "sqlite"}
	duplicateDbPaths := make([]string, 0, len(dbTypes)-1)
	for _, dbType := range dbTypes {
		if dbType == cfg.DbType {
			continue
		}

		// Store db path as a duplicate db if it exists.
		dbPath := blockDbPath(dbType)
		if fileExists(dbPath) {
			duplicateDbPaths = append(duplicateDbPaths, dbPath)
		}
	}

	// Warn if there are extra databases.
	if len(duplicateDbPaths) > 0 {
		selectedDbPath := blockDbPath(cfg.DbType)
		btcdLog.Warnf("WARNING: There are multiple block chain databases "+
			"using different database types.\nYou probably don't "+
			"want to waste disk space by having more than one.\n"+
			"Your current database is located at [%v].\nThe "+
			"additional database is located at %v", selectedDbPath,
			duplicateDbPaths)
	}
}

// loadBlockDB loads (or creates when needed) the block database taking into
// account the selected database backend and returns a handle to it.  It also
// contains additional logic such warning the user if there are multiple
// databases which consume space on the file system and ensuring the regression
// test database is clean when in regression test mode.
func loadBlockDB(dbName string) (database.DB, error) {
	// The memdb backend does not have a file path associated with it, so
	// handle it uniquely.  We also don't want to worry about the multiple
	// database type warnings when running with the memory database.
	//if cfg.DbType == "memdb" {
	//	btcdLog.Infof("Creating block database in memory.")
	//	db, err := database.Create(cfg.DbType)
	//	if err != nil {
	//		return nil, err
	//	}
	//	return db, nil
	//}

	//warnMultipleDBs()

	// The database name is based on the database type.
	// dbPath := blockDbPath(cfg.DbType)
	testDbRoot := "testdb"
	dbPath := filepath.Join(testDbRoot, dbName)

	// The regression test is special in that it needs a clean database for
	// each run, so remove it now if it already exists.
	// removeRegressionDB(dbPath)

	fmt.Printf("Loading block database from '%s\n'", dbPath)
	db, err := database.Open("ffldb", dbPath, activeNetParams.Net)
	if err != nil {
		// Return the error if it's not because the database doesn't
		// exist.
		if dbErr, ok := err.(database.Error); !ok || dbErr.ErrorCode !=
			database.ErrDbDoesNotExist {

			return nil, err
		}

		// Create the db if it does not exist.
		err = os.MkdirAll(cfg.DataDir, 0700)
		if err != nil {
			return nil, err
		}
		db, err = database.Create(cfg.DbType, dbPath, activeNetParams.Net)
		if err != nil {
			return nil, err
		}
	}

	fmt.Println("Block database loaded")
	return db, nil
}

func nomain() {
	// Use all processor cores.
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Block and transaction processing can cause bursty allocations.  This
	// limits the garbage collector from excessively overallocating during
	// bursts.  This value was arrived at with the help of profiling live
	// usage.
	debug.SetGCPercent(10)

	// Up some limits.
	if err := limits.SetLimits(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to set limits: %v\n", err)
		os.Exit(1)
	}
	// Call serviceMain on Windows to handle running as a service.  When
	// the return isService flag is true, exit now since we ran as a
	// service.  Otherwise, just fall through to normal operation.
	if runtime.GOOS == "windows" {
		isService, err := winServiceMain()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		if isService {
			os.Exit(0)
		}
	}

	// Work around defer not working after os.Exit()
	// if err := btcdMain(nil); err != nil {
	// 	os.Exit(1)
	// }
}

// chainSetup is used to create a new db and chain instance with the genesis
// block already inserted.  In addition to the new chain instance, it returns
// a teardown function the caller should invoke when done testing to clean up.
func chainSetup(dbName string, params *chaincfg.Params, config Config) (*blockchain.BlockChain, func(), error) {

	if !isSupportedDbType(testDbType) {
		return nil, nil, fmt.Errorf("unsupported db type %v", testDbType)
	}

	// Handle memory database specially since it doesn't need the disk
	// specific handling.
	var db database.DB
	var teardown func()
	// Create the root directory for test databases.
	testDbRoot := "testdb"
	if !fileExists(testDbRoot) {
		fmt.Println("Creating new testdb")
		if err := os.MkdirAll(testDbRoot, 0700); err != nil {
			err := fmt.Errorf("unable to create test db "+
				"root: %v", err)
			return nil, nil, err
		}
	}

	// Create a new database to store the accepted blocks into.
	dbPath := filepath.Join(testDbRoot, dbName)
	fmt.Println("DB set to path", dbPath)
	if !fileExists(dbPath) {
		// _ = os.RemoveAll(dbPath)
		fmt.Println("Creating database", dbPath)
		var err error
		db, err = database.Create(testDbType, dbPath, blockDataNet)
		if err != nil {
			return nil, nil, fmt.Errorf("error creating db: %v", err)
		}
		fmt.Println("DB created", db)
		teardown = func() {
			db.Close()
		}
	} else {
		// Load the block database.
		var err error
		db, err = loadBlockDB(dbName)
		if err != nil {
			btcdLog.Errorf("%v", err)
			return nil, nil, err
		}
		teardown = func() {
			// Ensure the database is sync'd and closed on shutdown.
			fmt.Println("Gracefully shutting down the database...")
			db.Close()
		}
	}
	fmt.Println("DB passed", db)

	// Setup a teardown function for cleaning up.  This function is
	// returned to the caller to be invoked when it is done testing.
	// teardown = func() {
	// 	db.Close()
	// 	os.RemoveAll(dbPath)
	// 	os.RemoveAll(testDbRoot)
	// }

	// Copy the chain params to ensure any modifications the tests do to
	// the chain parameters do not affect the global instance.
	paramsCopy := *params

	// Create the main chain instance.
	chain, err := blockchain.New(&blockchain.Config{
		DB:          db,
		ChainParams: &paramsCopy,
		Checkpoints: nil,
		TimeSource:  blockchain.NewMedianTime(),
		SigCache:    txscript.NewSigCache(1000),
	})
	if err != nil {
		err := fmt.Errorf("failed to create chain instance: %v", err)
		teardown()
		return nil, nil, err
	}
	return chain, teardown, nil
}

// Init function to run before the main process and handle command line arguments
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")

var flagMode *string
var flagConfig *string
var flagBoot *bool
var flagNumShards *int
var flagNumTxs *int

func init() {
	loadConfig()

	flagMode = flag.String("mode", "server", "start in shard or server mode")
	flagConfig = flag.String("conf", "config1.json", "Select config file to use")
	flagBoot = flag.Bool("bootstrap", false, "Toggle if this is the first node on the network")
	flagNumShards = flag.Int("n", 1, "Select number of shards")
	flagNumTxs = flag.Int("tx", 100, "Select number of Txs per block")
	flag.Parse()
}

// TestFullBlocks ensures all tests generated by the fullblocktests package
// have the expected result when processed via ProcessBlock.
func main() {
	// CPU profiling
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			logging.Panicln("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			logging.Panicln("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}
	// load btcd config

	numShards := *flagNumShards
	// load my config

	if strings.ToLower(*flagMode) == "server" {
		fmt.Print("Server mode\n")
		config, _ := LoadConfig(strings.ToLower(*flagConfig))

		// Setting up my logging system
		f, _ := os.OpenFile(config.Server.ServerLog, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		defer f.Close()
		logging.SetOutput(f)
		logging.SetFlags(logging.Lshortfile)
		logging.Println("This is a test log entry")

		// Create a new database and chain instance to run tests against.
		chain, teardownFunc, err := chainSetup(config.Server.ServerDb,
			&chaincfg.RegressionNetParams, config)
		if err != nil {
			logging.Printf("Failed to setup chain instance: %v", err)
			return
		}
		defer teardownFunc()

		// Start listener for shards
		fmt.Println("Starting server...")
		shardListener, error := net.Listen("tcp", ":"+config.Server.ServerShardsPort)
		if error != nil {
			fmt.Println(error)
		}
		// Listner for other coordinators (peers)
		coordListener, error := net.Listen("tcp", ":"+config.Server.ServerCoordsPort)
		if error != nil {
			fmt.Println(error)
		}

		coord := blockchain.NewCoordinator(shardListener, coordListener, chain, numShards)
		go coord.Start()

		// Wait for all the shards to get connected
		shardCount := 0
		for coord.GetNumShardes() < numShards {
			logging.Println("Num shards", coord.GetNumShardes())
			fmt.Println("New shard is connecting")
			connection, _ := coord.ShardListener.Accept()
			enc := gob.NewEncoder(connection)
			dec := gob.NewDecoder(connection)
			// NOTE: this should be either a constant, or sent to the coord
			// once connection is established
			shard := blockchain.NewShardConnection(connection, 0, shardCount, enc, dec)
			coord.RegisterShard(shard)
			// Wait for write to map to finish
			<-coord.ConnectectionAdded
			// Request shards ports and wait for response
			coord.RequestShardsInfo(connection, shardCount)
			shardCount++
			// Start receiving from shard
			// Will continue loop once a shard has connected
			go coord.ReceiveIntraShard(shard)
		}
		// Wait for all shards to send connect done
		for i := 0; i < numShards; i++ {
			<-coord.ConnectedOut
		}
		logging.Println("All shards finished connecting to each other")

		// Wait for connections from other coordinators
		go coord.ListenToCoordinators()

		if !(*flagBoot) {
			// connect to the already running server
			connection, err := net.Dial("tcp", config.Server.ServerTargetServer)
			if err != nil {

				fmt.Println(err)
			}
			enc := gob.NewEncoder(connection)
			dec := gob.NewDecoder(connection)
			// Register the connection with yourself
			// TODO: fix
			c := blockchain.NewCoordConnection(connection, enc, dec)
			coord.RegisterCoord(c)

			msg := blockchain.Message{
				Cmd: "GETSHARDS",
			}
			err = enc.Encode(msg)
			if err != nil {
				logging.Println(err, "Encode failed for struct: %#v", msg)
			}
			// TODO Change to work with Message + Move to Coord
			var receivedShards blockchain.DHTGob

			err = dec.Decode(&receivedShards)
			if err != nil {
				logging.Println("Error decoding GOB data:", err)
				return
			}
			coord.NotifyShards(receivedShards.DHTTable)

			for i := 0; i < numShards; i++ {
				<-coord.ConnectedOut
			}
			logging.Println("All shards finished connecting")

			// Once setup is complete, listen for messages from connected coord
			go coord.ReceiveCoord(c)

			// Need to wait for shards to be ready
			coord.SendBlocksRequest()

		}

		// Sleep on this until process is killed
		<-coord.KeepAlive

		// Start in oracle mode
	} else if strings.ToLower(*flagMode) == "oracle" {

		// Setting up my logging system
		f, _ := os.OpenFile("otestlog.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		defer f.Close()
		logging.SetOutput(f)
		logging.SetFlags(logging.Lshortfile)

		// Connect to coordinator
		// TODO make this part of the config
		connection, err := net.Dial("tcp", "127.0.0.1:12346")
		enc := gob.NewEncoder(connection)
		dec := gob.NewDecoder(connection)
		coordConn := blockchain.NewCoordConnection(connection, enc, dec)
		if err != nil {
			fmt.Println(err)
		}

		logging.Println("Connection started", coordConn.Socket)

		numTxs := *flagNumTxs

		tests, err := fullblocktests.SimpleGenerate(false, numTxs)
		if err != nil {
			fmt.Printf("failed to generate tests: %v", err)
		}

		gob.Register(blockchain.HeaderGob{})
		gob.Register(blockchain.RawBlockGob{})
		gob.Register(blockchain.AddressesGob{})
		gob.Register(blockchain.DHTGob{})

		// testAcceptedBlock attempts to process the block in the provided test
		// instance and ensures that it was accepted according to the flags
		// specified in the test.
		testAcceptedBlock := func(item fullblocktests.AcceptedBlock) {
			blockHeight := item.Height
			block := item.Block

			serializedSize := block.SerializeSizeStripped()
			logging.Printf("serialized block is: %d B", serializedSize)

			logging.Printf("Testing block %s (hash %s, height %d)",
				item.Name, block.BlockHash(), blockHeight)

			// Send block to coordinator
			gob.Register(blockchain.RawBlockGob{})

			coordEnc := coordConn.Enc

			headerBlock := wire.NewMsgBlockShard(&block.Header)
			for idx, tx := range block.Transactions {
				// spew.Dump(tx)
				newTx := wire.NewTxIndexFromTx(tx, int32(idx))
				// txHash := newTx.TxHash()
				// logging.Println("txHash", txHash)
				headerBlock.AddTransaction(newTx)
			}

			// Generate a header gob to send to coordinator
			msg := blockchain.Message{
				Cmd: "PROCBLOCK",
				Data: blockchain.RawBlockGob{
					Block:  headerBlock,
					Flags:  blockchain.BFNone,
					Height: blockHeight,
				},
			}

			err = coordEnc.Encode(msg)
			if err != nil {
				logging.Println(err, "Encode failed for struct: %#v", msg)
			}

			logging.Println("Waiting for conformation on block")
			for {
				dec := coordConn.Dec
				var msg blockchain.Message
				err := dec.Decode(&msg)
				if err != nil {
					logging.Println("Error decoding GOB data:", err)
					return
				}
				cmd := msg.Cmd

				logging.Println("Recived command", cmd)
				switch cmd {
				case "BLOCKDONE":
					logging.Println("Received BLOCKDONE")
					break // Quit the switch case
				case "BADBLOCK":
					logging.Println("Received BADBLOCK")
					break // Quit the switch case
				default:
					logging.Println("Command '", cmd, "' is not registered.")
				}
				break // Quit the for loop
			}

			// Ensure the main chain and orphan flags match the values
			// specified in the test.
			//if isMainChain != item.IsMainChain {
			//	t.Fatalf("block %q (hash %s, height %d) unexpected main "+
			//		"chain flag -- got %v, want %v", item.Name,
			//		block.Hash(), blockHeight, isMainChain,
			//		item.IsMainChain)
			//}
			//if isOrphan != item.IsOrphan {
			//	t.Fatalf("block %q (hash %s, height %d) unexpected "+
			//		"orphan flag -- got %v, want %v", item.Name,
			//		block.Hash(), blockHeight, isOrphan,
			//		item.IsOrphan)
			//}
		}
		fmt.Println("Started testing blocks")
		startTime := time.Now()
		for testNum, test := range tests {
			logging.Println("Test Num:", testNum)
			for itemNum, item := range test {
				logging.Println("Item Num:", testNum)
				switch item := item.(type) {
				case fullblocktests.AcceptedBlock:
					testAcceptedBlock(item)
				//case fullblocktests.RejectedBlock:
				//	testRejectedBlock(item)
				//case fullblocktests.RejectedNonCanonicalBlock:
				//	testRejectedNonCanonicalBlock(item)
				//case fullblocktests.OrphanOrRejectedBlock:
				//	testOrphanOrRejectedBlock(item)
				//case fullblocktests.ExpectedTip:
				//	testExpectedTip(item)
				default:
					fmt.Printf("test #%d, item #%d is not one of "+
						"the supported test instance types -- "+
						"got type: %T", testNum, itemNum, item)
				}
			}
		}
		endTime := time.Since(startTime)
		fmt.Println("Run took ", endTime)
		fmt.Println(endTime)

		// Start a shard
	} else if strings.ToLower(*flagMode) == "shard" {
		config, _ := LoadConfig(strings.ToLower(*flagConfig))

		f, err := os.OpenFile(config.Shard.ShardLog, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			fmt.Printf("Failed to create Shard log file: %v", err)
		}
		defer f.Close()

		logging.SetOutput(f)
		logging.SetFlags(logging.Lshortfile)
		logging.Println("This is a test log entry")

		fmt.Print("Shard mode\n")

		// Create a new database and chain instance to run tests against.
		chain, teardownFunc, err := chainSetup(config.Shard.ShardDb,
			&chaincfg.RegressionNetParams, config)
		if err != nil {
			logging.Printf("Failed to setup chain instance: %v", err)
			return
		}
		defer teardownFunc()

		fmt.Println("Starting shard...")
		connection, err := net.Dial("tcp", config.Server.ServerShardsIP+":"+config.Server.ServerShardsPort)
		if err != nil {
			fmt.Println(err)
		}

		// Listner for other shards to connect
		shardInterListener, error := net.Listen("tcp", ":"+config.Shard.ShardInterPort)
		if error != nil {
			fmt.Println(error)
		}

		// Listner for other shards to connect
		shardIntraListener, error := net.Listen("tcp", ":"+config.Shard.ShardIntraPort)
		if error != nil {
			fmt.Println(error)
		}
		logging.Println("Connection started", connection)
		interShardPort, err := strconv.Atoi(config.Shard.ShardInterPort)
		intraShardPort, err := strconv.Atoi(config.Shard.ShardIntraPort)
		logging.Println("Shard inter port", interShardPort)
		logging.Println("Shard intra port", intraShardPort)
		if err != nil {
			fmt.Println(err)
		}
		dec := gob.NewDecoder(connection)
		enc := gob.NewEncoder(connection)
		coordConn := blockchain.NewCoordConnection(connection, enc, dec)
		s := blockchain.NewShard(shardInterListener, shardIntraListener, coordConn, chain, net.ParseIP(config.Shard.ShardInterIP), net.ParseIP(config.Shard.ShardIntraIP), interShardPort, intraShardPort)
		// Before the shard is started, it must receive its index from coordinator
		var msg blockchain.Message
		err = dec.Decode(&msg)
		if err != nil {
			logging.Panicln("Error decoding GOB data:", err)
		}
		if msg.Cmd != "REQINFO" {
			logging.Panicln("Error receiving index from coord", err)
		}
		s.Index = msg.Data.(int)

		// Now the shard can start and reply with its info
		go s.StartShard()

		// Reply to the coordinators request for information
		s.HandleRequestInfo(s.Index)
		// Go wait for other shards to connect to you
		s.AwaitShards(numShards)

		for idx, con := range s.ShardNumToShard {
			logging.Println("Conn to", idx, "on", con.Socket)
		}

		fmt.Println("Waiting for shards to connect")
		for {
			//for i := 0; i < 1; i++ {
			connection, _ := s.ShardInterListener.Accept()
			enc := gob.NewEncoder(connection)
			dec := gob.NewDecoder(connection)
			shardConn := blockchain.NewShardConnection(connection, 0, 0, enc, dec)
			s.RegisterShard(shardConn)
			<-s.ConnectionAdded
			fmt.Println("Sleep on connection")
			go s.ReceiveInterShard(shardConn)
		}
		//<-s.Finish
	} else if strings.ToLower(*flagMode) == "test" {
	} else if strings.ToLower(*flagMode) == "full" {

		config, _ := LoadConfig(strings.ToLower(*flagConfig))

		// Setting up my logging system
		f, _ := os.OpenFile("otestlog.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		defer f.Close()
		logging.SetOutput(f)
		logging.SetFlags(logging.Lshortfile)

		// Connect to coordinator
		// TODO make this part of the config
		connection, err := net.Dial("tcp", "127.0.0.1:12346")
		enc := gob.NewEncoder(connection)
		dec := gob.NewDecoder(connection)
		coordConn := blockchain.NewCoordConnection(connection, enc, dec)
		if err != nil {
			fmt.Println(err)
		}

		logging.Println("Connection started", coordConn.Socket)

		gob.Register(blockchain.HeaderGob{})
		gob.Register(blockchain.RawBlockGob{})
		gob.Register(blockchain.AddressesGob{})
		gob.Register(blockchain.DHTGob{})
		// fix git
		chain, teardownFunc, err := chainSetup(config.Server.ServerDb,
			&chaincfg.RegressionNetParams, config)
		if err != nil {
			logging.Printf("Failed to setup chain instance: %v", err)
			return
		}
		defer teardownFunc()

		blockHash, err := chain.BlockHashByHeight(int32(5))
		if err != nil {
			logging.Println("Unable to fetch hash of block ", err)
		}
		fmt.Println("Block hash is ", blockHash)
		fmt.Println("Best chain length", chain.BestChainLength())
		start := time.Now()
		for i := 1; i < 10000; i++ {
			blockHash, err := chain.BlockHashByHeight(int32(i))
			if err != nil {
				logging.Println("Unable to fetch hash of block ", i)
			}
			block, _ := chain.BlockByHash(blockHash)

			blockToSend := wire.NewMsgBlockShard(block.Header())
			for idx, tx := range block.Transactions() {
				// spew.Dump(tx)
				newTx := wire.NewTxIndexFromTx(tx.MsgTx(), int32(idx))
				// txHash := newTx.TxHash()
				// logging.Println("txHash", txHash)
				blockToSend.AddTransaction(newTx)
			}

			// Generate a header gob to send to coordinator
			msg := blockchain.Message{
				Cmd: "PROCBLOCK",
				Data: blockchain.RawBlockGob{
					Block:  blockToSend,
					Flags:  blockchain.BFNone,
					Height: int32(i),
				},
			}

			err = enc.Encode(msg)
			if err != nil {
				logging.Println(err, "Encode failed for struct: %#v", msg)
			}

			logging.Println("Waiting for conformation on block")
			for {
				dec := coordConn.Dec
				var msg blockchain.Message
				err := dec.Decode(&msg)
				if err != nil {
					logging.Println("Error decoding GOB data:", err)
					return
				}
				cmd := msg.Cmd

				logging.Println("Recived command", cmd)
				switch cmd {
				case "BLOCKDONE":
					logging.Println("Received BLOCKDONE")
					break // Quit the switch case
				case "BADBLOCK":
					logging.Println("Received BADBLOCK")
					break // Quit the switch case
				default:
					logging.Println("Command '", cmd, "' is not registered.")
				}
				break // Quit the for loop
			}
		}
		elapsed := time.Since(start)
		fmt.Printf("chain sync took %v \n", elapsed)
	}
}
