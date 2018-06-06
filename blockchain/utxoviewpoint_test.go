// Copyright (c) 2013-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package blockchain

import (
	_ "bytes"
	//"fmt"
	_ "io"
	//reallog "log"
	_ "net/http/pprof"
	//_ "runtime"
	//_ "runtime/debug"
	_ "runtime/pprof"
	"testing"
	_ "time"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	_ "github.com/davecgh/go-spew/spew"
)

//func TestViewPoint(b *testing.B) {
func BenchmarkSqlViewPoint(b *testing.B) {

	//sqlDB := OpenDB("postgresql://amanusk@localhost:26257/blockchain?sslmode=disable")
	//view := UtxoViewpoint(sqlDB)
	//b.ResetTimer()

	//for i := 0; i < b.N; i++ {
	//tx := btcutil.NewTx(multiTx)

	//view := NewUtxoViewpoint()
	//view.AddTxOuts(tx, 1)
	//txHash := tx.Hash()
	//entry := view.LookupEntry(txHash)
	//if entry == nil {
	//	reallog.Println("Could not find tx ", txHash)
	//}

	//e := view.LookupEntry(txHash)
	//if e == nil {
	//	b.Error("Could not find tx in view")
	//}

	//entries := view.Entries()
	//_, ok := entries[*txHash]
	//if !ok {
	//	b.Error("Could not find tx in view")
	//}

	//hash, err := chainhash.NewHash([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	//if err != nil {
	//	fmt.Printf("Failed to create hash", err)
	//}
	//reallog.Println("Hash ", hash)
	//_, ok = entries[*hash]
	//if ok {
	//	b.Error("Found non existing tx")
	//}

	//}
	//sqlDB.Close()
}

// multiTx is a MsgTx with an input and output and used in various tests.
var multiTx = &wire.MsgTx{
	Version: 1,
	TxIn: []*wire.TxIn{
		{
			PreviousOutPoint: wire.OutPoint{
				Hash:  chainhash.Hash{},
				Index: 0xffffffff,
			},
			SignatureScript: []byte{
				0x04, 0x31, 0xdc, 0x00, 0x1b, 0x01, 0x62,
			},
			Sequence: 0xffffffff,
		},
	},
	TxOut: []*wire.TxOut{
		{
			Value: 0x12a05f200,
			PkScript: []byte{
				0x41, // OP_DATA_65
				0x04, 0xd6, 0x4b, 0xdf, 0xd0, 0x9e, 0xb1, 0xc5,
				0xfe, 0x29, 0x5a, 0xbd, 0xeb, 0x1d, 0xca, 0x42,
				0x81, 0xbe, 0x98, 0x8e, 0x2d, 0xa0, 0xb6, 0xc1,
				0xc6, 0xa5, 0x9d, 0xc2, 0x26, 0xc2, 0x86, 0x24,
				0xe1, 0x81, 0x75, 0xe8, 0x51, 0xc9, 0x6b, 0x97,
				0x3d, 0x81, 0xb0, 0x1c, 0xc3, 0x1f, 0x04, 0x78,
				0x34, 0xbc, 0x06, 0xd6, 0xd6, 0xed, 0xf6, 0x20,
				0xd1, 0x84, 0x24, 0x1a, 0x6a, 0xed, 0x8b, 0x63,
				0xa6, // 65-byte signature
				0xac, // OP_CHECKSIG
			},
		},
		{
			Value: 0x5f5e100,
			PkScript: []byte{
				0x41, // OP_DATA_65
				0x04, 0xd6, 0x4b, 0xdf, 0xd0, 0x9e, 0xb1, 0xc5,
				0xfe, 0x29, 0x5a, 0xbd, 0xeb, 0x1d, 0xca, 0x42,
				0x81, 0xbe, 0x98, 0x8e, 0x2d, 0xa0, 0xb6, 0xc1,
				0xc6, 0xa5, 0x9d, 0xc2, 0x26, 0xc2, 0x86, 0x24,
				0xe1, 0x81, 0x75, 0xe8, 0x51, 0xc9, 0x6b, 0x97,
				0x3d, 0x81, 0xb0, 0x1c, 0xc3, 0x1f, 0x04, 0x78,
				0x34, 0xbc, 0x06, 0xd6, 0xd6, 0xed, 0xf6, 0x20,
				0xd1, 0x84, 0x24, 0x1a, 0x6a, 0xed, 0x8b, 0x63,
				0xa6, // 65-byte signature
				0xac, // OP_CHECKSIG
			},
		},
	},
	LockTime: 0,
}
