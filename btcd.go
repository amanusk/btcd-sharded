// Copyright (c) 2013-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	reallog "log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strings"

	"encoding/gob"
	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/blockchain/fullblocktests"
	"github.com/btcsuite/btcd/blockchain/indexers"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/database"
	"github.com/btcsuite/btcd/limits"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
)

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

var numShards int = 1

// btcdMain is the real main function for btcd.  It is necessary to work around
// the fact that deferred functions do not run when os.Exit() is called.  The
// optional serverChan parameter is mainly used by the service code to be
// notified with the server once it is setup so it can gracefully stop it when
// requested from the service control manager.
func btcdMain(serverChan chan<- *server) error {
	// Load configuration and parse command line.  This function also
	// initializes logging and configures it accordingly.
	tcfg, _, err := loadConfig()
	if err != nil {
		return err
	}
	cfg = tcfg
	defer func() {
		if logRotator != nil {
			logRotator.Close()
		}
	}()

	// Get a channel that will be closed when a shutdown signal has been
	// triggered either from an OS signal such as SIGINT (Ctrl+C) or from
	// another subsystem such as the RPC server.
	interrupt := interruptListener()
	defer btcdLog.Info("Shutdown complete")

	// Show version at startup.
	btcdLog.Infof("Version %s", version())

	// Enable http profiling server if requested.
	if cfg.Profile != "" {
		go func() {
			listenAddr := net.JoinHostPort("", cfg.Profile)
			btcdLog.Infof("Profile server listening on %s", listenAddr)
			profileRedirect := http.RedirectHandler("/debug/pprof",
				http.StatusSeeOther)
			http.Handle("/", profileRedirect)
			btcdLog.Errorf("%v", http.ListenAndServe(listenAddr, nil))
		}()
	}

	// Write cpu profile if requested.
	if cfg.CPUProfile != "" {
		f, err := os.Create(cfg.CPUProfile)
		if err != nil {
			btcdLog.Errorf("Unable to create cpu profile: %v", err)
			return err
		}
		pprof.StartCPUProfile(f)
		defer f.Close()
		defer pprof.StopCPUProfile()
	}

	// Perform upgrades to btcd as new versions require it.
	if err := doUpgrades(); err != nil {
		btcdLog.Errorf("%v", err)
		return err
	}

	// Return now if an interrupt signal was triggered.
	if interruptRequested(interrupt) {
		return nil
	}

	// Load the block database.
	db, err := loadBlockDB()
	if err != nil {
		btcdLog.Errorf("%v", err)
		return err
	}
	defer func() {
		// Ensure the database is sync'd and closed on shutdown.
		btcdLog.Infof("Gracefully shutting down the database...")
		db.Close()
	}()

	// Return now if an interrupt signal was triggered.
	if interruptRequested(interrupt) {
		return nil
	}

	// Drop indexes and exit if requested.
	//
	// NOTE: The order is important here because dropping the tx index also
	// drops the address index since it relies on it.
	if cfg.DropAddrIndex {
		if err := indexers.DropAddrIndex(db, interrupt); err != nil {
			btcdLog.Errorf("%v", err)
			return err
		}

		return nil
	}
	if cfg.DropTxIndex {
		if err := indexers.DropTxIndex(db, interrupt); err != nil {
			btcdLog.Errorf("%v", err)
			return err
		}

		return nil
	}
	// Create server and start it.
	server, err := newServer(cfg.Listeners, db, activeNetParams.Params,
		interrupt)
	if err != nil {
		// TODO: this logging could do with some beautifying.
		btcdLog.Errorf("Unable to start server on %v: %v",
			cfg.Listeners, err)
		return err
	}
	defer func() {
		btcdLog.Infof("Gracefully shutting down the server...")
		server.Stop()
		server.WaitForShutdown()
		srvrLog.Infof("Server shutdown complete")
	}()
	server.Start()
	if serverChan != nil {
		serverChan <- server
	}

	// Wait until the interrupt signal is received from an OS signal or
	// shutdown is requested through one of the subsystems such as the RPC
	// server.
	<-interrupt
	return nil
}

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

// warnMultipeDBs shows a warning if multiple block database types are detected.
// This is not a situation most users want.  It is handy for development however
// to support multiple side-by-side databases.
func warnMultipeDBs() {
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
func loadBlockDB() (database.DB, error) {
	// The memdb backend does not have a file path associated with it, so
	// handle it uniquely.  We also don't want to worry about the multiple
	// database type warnings when running with the memory database.
	if cfg.DbType == "memdb" {
		btcdLog.Infof("Creating block database in memory.")
		db, err := database.Create(cfg.DbType)
		if err != nil {
			return nil, err
		}
		return db, nil
	}

	warnMultipeDBs()

	// The database name is based on the database type.
	dbPath := blockDbPath(cfg.DbType)

	// The regression test is special in that it needs a clean database for
	// each run, so remove it now if it already exists.
	removeRegressionDB(dbPath)

	btcdLog.Infof("Loading block database from '%s'", dbPath)
	db, err := database.Open(cfg.DbType, dbPath, activeNetParams.Net)
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

	btcdLog.Info("Block database loaded")
	return db, nil
}

func notmain() {
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
	if err := btcdMain(nil); err != nil {
		os.Exit(1)
	}
}

// chainSetup is used to create a new db and chain instance with the genesis
// block already inserted.  In addition to the new chain instance, it returns
// a teardown function the caller should invoke when done testing to clean up.
func chainSetup(dbName string, params *chaincfg.Params) (*blockchain.BlockChain, func(), error) {

	// Handle memory database specially since it doesn't need the disk
	// specific handling.
	var teardown func()
	sqlDB := blockchain.OpenDB()

	// Setup a teardown function for cleaning up.  This function is
	// returned to the caller to be invoked when it is done testing.
	teardown = func() {
		sqlDB.Close()
	}

	// Copy the chain params to ensure any modifications the tests do to
	// the chain parameters do not affect the global instance.
	paramsCopy := *params

	// Create the main chain instance.
	chain, err := blockchain.SqlNew(&blockchain.Config{
		SqlDB:       sqlDB,
		ChainParams: &paramsCopy,
		Checkpoints: nil,
		TimeSource:  blockchain.NewMedianTime(),
		SigCache:    txscript.NewSigCache(1000),
	})
	if err != nil {
		teardown()
		err := fmt.Errorf("failed to create chain instance: %v", err)
		return nil, nil, err
	}
	return chain, teardown, nil
}

var flagMode *string

func init() {
	flagMode = flag.String("mode", "server", "start in shard or server mode")
	flag.Parse()
}

// TestFullBlocks ensures all tests generated by the fullblocktests package
// have the expected result when processed via ProcessBlock.
func main() {

	loadConfig()

	if strings.ToLower(*flagMode) == "server" {
		fmt.Print("Server mode\n")

		// Setting up my logging system
		f, _ := os.OpenFile("testlog.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		defer f.Close()
		reallog.SetOutput(f)
		reallog.SetFlags(reallog.Lshortfile)
		reallog.Println("This is a test log entry")

		// Create a new database and chain instance to run tests against.
		chain, teardownFunc, err := chainSetup("fullblocktest",
			&chaincfg.RegressionNetParams)
		if err != nil {
			reallog.Printf("Failed to setup chain instance: %v", err)
			return
		}
		defer teardownFunc()

		// Start listener on port 12345 for coordinator
		fmt.Println("Starting server...")
		shardListener, error := net.Listen("tcp", ":12345")
		if error != nil {
			fmt.Println(error)
		}
		// Listner for other coordinators (peers)
		coordListener, error := net.Listen("tcp", ":12346")
		if error != nil {
			fmt.Println(error)
		}

		manager := blockchain.NewCoordinator(shardListener, coordListener, chain)
		go manager.Start()

		// Wait for all the shards to get connected
		for manager.GetNumShardes() < numShards {
			connection, _ := manager.ShardListener.Accept()
			shard := blockchain.NewShardConnection(connection)
			manager.RegisterShard(shard)
			go manager.ReceiveShard(shard)
			// Will continue loop once a shard has connected
			<-manager.Connected
		}

		// Wait for connections from other coordinators
		fmt.Println("Waiting for coordinators to connect")
		for {
			connection, _ := manager.CoordListener.Accept()
			coord := blockchain.NewCoordConnection(connection)
			manager.RegisterCoord(coord)
			go manager.ReceiveCoord(coord)
		}
		// Start in oracle mode
	} else if strings.ToLower(*flagMode) == "oracle" {

		// Setting up my logging system
		f, _ := os.OpenFile("otestlog.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		defer f.Close()
		reallog.SetOutput(f)
		reallog.SetFlags(reallog.Lshortfile)

		// Connect to coordinator
		coordConn, err := net.Dial("tcp", "localhost:12346")
		if err != nil {
			fmt.Println(err)
		}

		fmt.Println("Connection started", coordConn)

		tests, err := fullblocktests.SimpleGenerate(false)
		if err != nil {
			fmt.Printf("failed to generate tests: %v", err)
		}

		// GET shards from coordinator
		//TODO restore for full protocol
		coordConn.Write([]byte("GETSHARDS"))

		var receivedShards blockchain.AddressesGob

		dec := gob.NewDecoder(coordConn)
		err = dec.Decode(&receivedShards)
		if err != nil {
			reallog.Println("Error decoding GOB data:", err)
			return
		}
		//TODO Logic to connect to shards will be moved to coordinator
		shardDial := receivedShards.Addresses[0].IP.String() + ":12347"
		// NOTE: There should be an array of shards to communicate with
		shardConn, err := net.Dial("tcp", shardDial)
		if err != nil {
			fmt.Println(err)
		}

		// testAcceptedBlock attempts to process the block in the provided test
		// instance and ensures that it was accepted according to the flags
		// specified in the test.
		testAcceptedBlock := func(item fullblocktests.AcceptedBlock) {
			blockHeight := item.Height
			block := btcutil.NewBlock(item.Block)
			block.SetHeight(blockHeight)
			reallog.Printf("Testing block %s (hash %s, height %d)",
				item.Name, block.Hash(), blockHeight)

			// Send block to coordinator
			coordConn.Write([]byte("PROCBLOCK"))
			coordEnc := gob.NewEncoder(coordConn)
			// Generate a header gob to send to coordinator
			headerToSend := blockchain.HeaderGob{
				Header:       &block.MsgBlock().Header,
				Flags:        blockchain.BFNone,
				ActiveShards: numShards,
			}
			err = coordEnc.Encode(headerToSend)
			if err != nil {
				reallog.Println(err, "Encode failed for struct: %#v", headerToSend)
			}

			bShards := make([]*wire.MsgBlockShard, numShards)

			// Create a block shard to send to shards
			for idx, _ := range bShards {
				bShards[idx] = wire.NewMsgBlockShard(&block.MsgBlock().Header)
			}

			// Split transactions between blocks
			for idx, tx := range block.MsgBlock().Transactions {
				newTx := wire.NewTxIndexFromTx(tx, int32(idx))
				// NOTE: This should be a DHT
				bShards[idx%numShards].AddTransaction(newTx)
			}
			reallog.Println("Sending shards")
			reallog.Println(bShards[0])
			activeShards := numShards
			for i := 0; i < numShards; i++ {
				if len(bShards[i].Transactions) > 0 {
					var bb bytes.Buffer
					bShards[i].Serialize(&bb)

					// All data is sent in gobs
					blockToSend := blockchain.BlockGob{
						Block: bb.Bytes(),
					}
					shardConn.Write([]byte("BLOCKGOB"))

					//Actually write the GOB on the socket
					enc := gob.NewEncoder(shardConn)
					err = enc.Encode(blockToSend)
					if err != nil {
						reallog.Println(err, "Encode failed for struct: %#v", blockToSend)
					}
				} else {
					activeShards--
				}
			}
			//shardConn.Write([]byte("BLOCKDAD"))

			reallog.Println("Waiting for conformation on block")
			for {
				message := make([]byte, 9)
				n, err := coordConn.Read(message)
				switch {
				case err == io.EOF:
					reallog.Println("Reached EOF - close this connection.\n   ---")
					return
				}
				cmd := (string(message[:n]))
				reallog.Println("Recived command", cmd)
				switch cmd {
				case "BLOCKDONE":
					break // Quit the switch case
				default:
					reallog.Println("Command '", cmd, "' is not registered.")
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
		fmt.Printf("Started testing blocks")
		for testNum, test := range tests {
			for itemNum, item := range test {
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

		// Start a shard
	} else {
		f, err := os.OpenFile("stestlog.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			fmt.Printf("Failed to create Shard log file: %v", err)
		}
		defer f.Close()

		reallog.SetOutput(f)
		reallog.SetFlags(reallog.Lshortfile)
		reallog.Println("This is a test log entry")

		fmt.Print("Shard mode\n")
		index := blockchain.MyNewBlockIndex(&chaincfg.RegressionNetParams)
		sqlDB := blockchain.OpenDB()

		reallog.Printf("Index is", index)
		reallog.Printf("DB is", sqlDB)

		fmt.Println("Starting shard...")
		connection, err := net.Dial("tcp", "localhost:12345")
		if err != nil {
			fmt.Println(err)
		}

		// Listner for other shards to connect
		shardListener, error := net.Listen("tcp", ":12347")
		if error != nil {
			fmt.Println(error)
		}

		fmt.Println("Connection started", connection)
		s := blockchain.NewShard(shardListener, connection, index, sqlDB)
		go s.StartShard()

		fmt.Println("Waiting for shards to connect")
		for {
			connection, _ := s.ShardListener.Accept()
			shardConn := blockchain.NewShardConnection(connection)
			s.RegisterShard(shardConn)
			go s.ReceiveShard(shardConn)
		}
	}
}
