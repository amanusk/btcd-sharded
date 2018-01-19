package blockchain_test

import (
	"bufio"
	_ "bytes"
	"fmt"
	reallog "log"
	_ "net"
	"os"
	_ "path/filepath"
	"strings"
	_ "sync"
	"testing"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg"
	_ "github.com/btcsuite/btcd/chaincfg/chainhash"
	_ "github.com/btcsuite/btcd/database/ffldb"
)

func TestClient(t *testing.T) {
	// Setting up my logging system
	f, err := os.OpenFile("ctestlog.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("Failed to create Client log file: %v", err)
	}
	defer f.Close()

	reallog.SetOutput(f)
	reallog.SetFlags(reallog.Lshortfile)
	reallog.Println("This is a test log entry")

	fmt.Print("Client mode\n")
	index := blockchain.MyNewBlockIndex(&chaincfg.RegressionNetParams)
	sqlDB := blockchain.OpenDB()

	reallog.Printf("Index is", index)
	reallog.Printf("DB is", sqlDB)

	//fmt.Println("Starting client...")
	//connection, err := net.Dial("tcp", "localhost:12345")
	//if err != nil {
	//	fmt.Println(err)
	//}

	//fmt.Println("Connection started", connection)
	//c := blockchain.NewClient(connection, index, sqlDB)
	//c.StartClient()
	fmt.Println("Wait for user input")
	reader := bufio.NewReader(os.Stdin)
	message, _ := reader.ReadString('\n')
	message = strings.TrimRight(message, "\n")
	fmt.Println("Message")
	time.Sleep(200)

}
