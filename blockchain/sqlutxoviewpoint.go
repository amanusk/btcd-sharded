// Copyright (c) 2015-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package blockchain

//import (
//"fmt"
//reallog "log"

//"github.com/btcsuite/btcd/chaincfg/chainhash"
//"github.com/btcsuite/btcd/database"
//"github.com/btcsuite/btcd/txscript"
//"github.com/btcsuite/btcd/wire"
//"github.com/btcsuite/btcutil"
//)

//// -----------------------------------------------------------//
//// -----------------------SQLUtxoViewpoint--------------------//

//// SQLUtxoViewpoint represents a view into the set of unspent transaction outputs
//// from a specific point of view in the chain.  For example, it could be for
//// the end of the main chain, some point in the history of the main chain, or
//// down a side chain.
////
//// The unspent outputs are needed by other transactions for things such as
//// script validation and double spend prevention.
//type SQLUtxoViewpoint struct {
//db       *SQLBlockDB
//bestHash chainhash.Hash
//}

//// Force UtxoViewpoint to implement the interface
//// var _ UtxoView = (*SQLUtxoViewpoint)(nil)

//// NewSQLUtxoViewpoint returns a new empty unspent transaction output view.
//func NewSQLUtxoViewpoint(indb *SQLBlockDB) *SQLUtxoViewpoint {

//newview := SQLUtxoViewpoint{
//db: indb,
//}
//newview.db.NewUtoxView()
//return &newview

//}

//// BestHash returns the hash of the best block in the chain the view currently
//// respresents.
//func (view *SQLUtxoViewpoint) BestHash() *chainhash.Hash {
//return &view.bestHash
//}

//// SetBestHash sets the hash of the best block in the chain the view currently
//// respresents.
//func (view *SQLUtxoViewpoint) SetBestHash(hash *chainhash.Hash) {
//view.bestHash = *hash
//}

//// PrintToLog Prints all the information in the UtxoView to the log
//func (view *SQLUtxoViewpoint) PrintToLog() {
////TODO
//}

//// LookupEntry returns information about a given transaction according to the
//// current state of the view.  It will return nil if the passed transaction
//// hash does not exist in the view or is otherwise not available such as when
//// it has been disconnected during a reorg.
//func (view *SQLUtxoViewpoint) LookupEntry(txHash *chainhash.Hash) *UtxoEntry {
//entry, err := view.db.FetchUtxoEntry(txHash)
//if err != nil {
//return nil
//}
//return entry
//}

//// AddTxOuts adds all outputs in the passed transaction which are not provably
//// unspendable to the view.  When the view already has entries for any of the
//// outputs, they are simply marked unspent.  All fields will be updated for
//// existing entries since it's possible it has changed during a reorg.
//func (view *SQLUtxoViewpoint) AddTxOuts(tx *btcutil.Tx, blockHeight int32) {
//// When there are not already any utxos associated with the transaction,
//// add a new entry for it to the view.
//entry := view.LookupEntry(tx.Hash())
//if entry == nil {
//entry = newUtxoEntry(tx.MsgTx().Version, IsCoinBase(tx),
//blockHeight)
//view.db.StoreUtxoEntry(*tx.Hash(), tx.MsgTx().Version, IsCoinBase(tx), blockHeight)
//} else {
//// update blockHeight
////entry.blockHeight = blockHeight
//view.db.UpdateUtxoEntryBlockHeight(*tx.Hash(), blockHeight)

//}
//view.db.UpdateUtxoEntryModified(*tx.Hash())

//// Loop all of the transaction outputs and add those which are not
//// provably unspendable.
//for txOutIdx, txOut := range tx.MsgTx().TxOut {
//if txscript.IsUnspendable(txOut.PkScript) {
//continue
//}

//// Update existing entries.  All fields are updated because it's
//// possible (although extremely unlikely) that the existing
//// entry is being replaced by a different transaction with the
//// same hash.  This is allowed so long as the previous
//// transaction is fully spent.

//// If transaction output does not exist, it is created

//if ok := view.db.UpdateUtxoOuptput(*tx.Hash(), int32(txOutIdx), int64(txOut.Value), txOut.PkScript); ok {
//continue
//}
//}
//}

//// ConnectTransaction updates the view by adding all new utxos created by the
//// passed transaction and marking all utxos that the transactions spend as
//// spent.  In addition, when the 'stxos' argument is not nil, it will be updated
//// to append an entry for each spent txout.  An error will be returned if the
//// view does not contain the required utxos.
//func (view *SQLUtxoViewpoint) ConnectTransaction(tx *btcutil.Tx, blockHeight int32, stxos *[]spentTxOut) error {
//// Coinbase transactions don't have any inputs to spend.
//if IsCoinBase(tx) {
//// Add the transaction's outputs as available utxos.
//view.AddTxOuts(tx, blockHeight)
//return nil
//}

//// Spend the referenced utxos by marking them spent in the view and,
//// if a slice was provided for the spent txout details, append an entry
//// to it.
//for _, txIn := range tx.MsgTx().TxIn {
////TODO: Consider pulling out all Txs in a single query
////TODO: Consider if to pull all the txouts in LookupEntry or only metadata
//originIndex := txIn.PreviousOutPoint.Index
//entry := view.LookupEntry(&txIn.PreviousOutPoint.Hash)

//// Ensure the referenced utxo exists in the view.  This should
//// never happen unless there is a bug is introduced in the code.
//if entry == nil {
//return AssertError(fmt.Sprintf("view missing input %v",
//txIn.PreviousOutPoint))
//}
//// Note: This should be done as a transaction
////entry.SpendOutput(originIndex)
//view.db.UpdateOutputSpent(txIn.PreviousOutPoint.Hash, originIndex)

//// Don't create the stxo details if not requested.
//if stxos == nil {
//continue
//}

//// Populate the stxo details using the utxo entry.  When the
//// transaction is fully spent, set the additional stxo fields
//// accordingly since those details will no longer be available
//// in the utxo set.

////utxoOutput := view.db.FetchUtxoOutput(txIn.PreviousOutPoint.Hash, originIndex)

//var stxo = spentTxOut{
//compressed: false,
//version:    entry.Version(),
//amount:     entry.AmountByIndex(originIndex),
//pkScript:   entry.PkScriptByIndex(originIndex),
//}
//if entry.IsFullySpent() {
//stxo.height = entry.BlockHeight()
//stxo.isCoinBase = entry.IsCoinBase()
//}

//// Append the entry to the provided spent txouts slice.
//*stxos = append(*stxos, stxo)
//}

//// Add the transaction's outputs as available utxos.
//view.AddTxOuts(tx, blockHeight)
//return nil
//}

//// ConnectTransactions updates the view by adding all new utxos created by all
//// of the transactions in the passed block, marking all utxos the transactions
//// spend as spent, and setting the best hash for the view to the passed block.
//// In addition, when the 'stxos' argument is not nil, it will be updated to
//// append an entry for each spent txout.
//func (view *SQLUtxoViewpoint) ConnectTransactions(block btcutil.Block, stxos *[]spentTxOut) error {
//// NOTE: debug information for stxos
//reallog.Print("stxos Before", stxos)
//for _, tx := range block.Transactions() {
//err := view.ConnectTransaction(tx, block.Height(), stxos)

//reallog.Print("Connected tx", tx)
//if err != nil {
//reallog.Print("Error connecting", tx)
//return err
//}
//}
//reallog.Print("stxos After", stxos)

//// Update the best hash for view to include this block since all of its
//// transactions have been connected.
//view.SetBestHash(block.Hash())
//return nil
//}

//// DisconnectTransactions updates the view by removing all of the transactions
//// created by the passed block, restoring all utxos the transactions spent by
//// using the provided spent txo information, and setting the best hash for the
//// view to the block before the passed block.
//func (view *SQLUtxoViewpoint) DisconnectTransactions(block btcutil.Block, stxos []spentTxOut) error {
//// TODO TODO TODO
//return nil
//}

//// Entries returns a map of all the entries, fetched from the DB
//func (view *SQLUtxoViewpoint) Entries() map[chainhash.Hash]*UtxoEntry {
//return view.db.FetchAllUtxoEntries()
//}

//// Commit prunes all entries marked modified that are now fully spent and marks
//// all entries as unmodified.
//// TODO: Move all utxos to UtxoSet and drop table of ViewPoint
//func (view *SQLUtxoViewpoint) Commit() {
////for txHash, entry := range view.entries {
////	if entry == nil || (entry.modified && entry.IsFullySpent()) {
////		delete(view.entries, txHash)
////		continue
////	}

////	entry.modified = false
////}
//return
//}

//// FetchUtxosMain currently place holder
//func (view *SQLUtxoViewpoint) FetchUtxosMain(db database.DB, txSet map[chainhash.Hash]struct{}) error {
//return nil
//}

//// SQLFetchUtxosMain fetches unspent transaction output data about the provided
//// set of transactions from the point of view of the end of the main chain at
//// the time of the call.
////
//// Upon completion of this function, the view will contain an entry for each
//// requested transaction.  Fully spent transactions, or those which otherwise
//// don't exist, will result in a nil entry in the view.
//func (view *SQLUtxoViewpoint) SQLFetchUtxosMain(db *SQLBlockDB, txSet map[chainhash.Hash]struct{}) error {
//// Nothing to do if there are no requested hashes.
//if len(txSet) == 0 {
//return nil
//}

//reallog.Println("Fetching transaction Inputs")
//// Load the unspent transaction output information for the requested set
//// of transactions from the point of view of the end of the main chain.
////
//// NOTE: Missing entries are not considered an error here and instead
//// will result in nil entries in the view.  This is intentionally done
//// since other code uses the presence of an entry in the store as a way
//// to optimize spend and unspend updates to apply only to the specific
//// utxos that the caller needs access to.
//for hash := range txSet {
//hashCopy := hash
//reallog.Println("Going to db for ", hash)
//entry, err := db.SQLDbFetchUtxoEntry(&hashCopy)
//if err != nil {
//return err
//}
//view.db.StoreUtxoEntry(hashCopy, entry.version, entry.isCoinBase, entry.blockHeight)
//// TODO: Do this as a single query!! // TODO
//// TODO: Consider saving all Utxos as entries and not encoding them first
//for txOutIdx, txOutput := range entry.sparseOutputs {
//view.db.StoreUtxoOutput(hashCopy, int32(txOutIdx), txOutput.spent, txOutput.amount, txOutput.pkScript)
//}
//}
//return nil
//}

//// FetchUtxos Place holder
//func (view *SQLUtxoViewpoint) FetchUtxos(db database.DB, txSet map[chainhash.Hash]struct{}) error {
//return nil
//}

//// FetchInputUtxos Place holder
//func (view *SQLUtxoViewpoint) FetchInputUtxos(db database.DB, block btcutil.Block) error {
//return nil
//}

//// SQLFetchInputUtxos loads utxo details about the input transactions referenced
//// by the transactions in the given block into the view from the database as
//// needed.  In particular, referenced entries that are earlier in the block are
//// added to the view and entries that are already in the view are not modified.
//func (view *SQLUtxoViewpoint) SQLFetchInputUtxos(db *SQLBlockDB, block btcutil.Block) error {
//// Build a map of in-flight transactions because some of the inputs in
//// this block could be referencing other transactions earlier in this
//// block which are not yet in the chain.
//// TODO: needs to be updated to use the actual index in the block
//txInFlight := map[chainhash.Hash]int{}
//transactions := block.Transactions()
//for _, tx := range transactions {
//txInFlight[*tx.Hash()] = tx.Index()
//}

//// NOTE: This is where we might need to create a DB transaction
//// Loop through all of the transaction inputs (except for the coinbase
//// which has no inputs) collecting them into sets of what is needed and
//// what is already known (in-flight).
//txNeededSet := make(map[chainhash.Hash]struct{})
//for i, tx := range transactions[0:] {
//for _, txIn := range tx.MsgTx().TxIn {
//// It is acceptable for a transaction input to reference
//// the output of another transaction in this block only
//// if the referenced transaction comes before the
//// current one in this block.  Add the outputs of the
//// referenced transaction as available utxos when this
//// is the case.  Otherwise, the utxo details are still
//// needed.
////
//// NOTE: The >= is correct here because i is one less
//// than the actual position of the transaction within
//// the block due to skipping the coinbase.
//originHash := &txIn.PreviousOutPoint.Hash
//if inFlightIndex, ok := txInFlight[*originHash]; ok &&
//i >= inFlightIndex {

//originTx := transactions[inFlightIndex]
//// Good point to synchronize
//view.AddTxOuts(originTx, block.Height())
//continue
//}

//// Don't request entries that are already in the view
//// from the database.
////if _, ok := view.entries[*originHash]; ok {
////	continue
////}

//txNeededSet[*originHash] = struct{}{}
//}
//}

//// Request the input utxos from the database.
//return view.SQLFetchUtxosMain(db, txNeededSet)
//}
///

// ---------------- UTXO view methods --------------//
////

// NewUtoxView initialized an new view, with an SQL table as the database
//func (db *SQLBlockDB) NewUtoxView() {
//// TODO: Consider drop tables if exist

//// Create utxos table
//_, err := db.db.Exec(
//"CREATE TABLE IF NOT EXISTS viewpoint (txhash BYTES PRIMARY KEY," +
//"modified BOOL, " +
//"version INT, " +
//"isCoinBase BOOL, " +
//"blockHeight INT)")
//if err != nil {
//reallog.Fatal(err)
//}
//// Create UtxoEntry table
//_, err = db.db.Exec(
//"CREATE TABLE IF NOT EXISTS utxooutputs (txhash BYTES," +
//"txOutIdx INT, " +
//"spent BOOL, " +
//"compressed BOOL, " +
//"amount INT, " +
//"pkscript BYTES, " +
//"PRIMARY KEY (txhash, txOutIdx), " +
//"CONSTRAINT fk_txhash FOREIGN KEY (txhash) REFERENCES viewpoint" +
//") INTERLEAVE IN PARENT viewpoint (txhash)")
//if err != nil {
//reallog.Fatal(err)
//}
//reallog.Printf("Created utxo view")

//}

//// StoreUtxoOutput adds an output entry to the TxOuts database.
//func (db *SQLBlockDB) StoreUtxoOutput(txHash chainhash.Hash, txOutIdx int32, spent bool, amount int64, pkScript []byte) {
//_, err := db.db.Exec("INSERT INTO utxooutputs (txhash, txOutIdx, spent, compressed, amount, pkScript)"+
//"VALUES ($1, $2, $5, FALSE,$3, $4);", txHash[:], txOutIdx, amount, pkScript, spent)
//reallog.Print("Inserting output to utxo ", txHash[:])
//if err != nil {
//// Should be checked or something, left for debug
//reallog.Print("SQL Insert Err:", err)
//} else {
//reallog.Print("Save tx ", txHash[:])
//}
//}

//// StoreUtxoEntry stores a UTXO in a database
//// This is different from StoreUTXO as here the data is not serialized
//func (db *SQLBlockDB) StoreUtxoEntry(txHash chainhash.Hash, version int32, isCoinBase bool, blockHeight int32) {
//reallog.Print("txHash ", txHash)
//_, err := db.db.Exec("INSERT INTO viewpoint (txhash, version, iscoinbase, blockheight)"+
//"VALUES ($1, $2, $3, $4);", txHash[:], version, isCoinBase, blockHeight)
//reallog.Print("Inserting utxo ", txHash[:])
//if err != nil {
//reallog.Print("SQL Insert Err:", err)
//} else {
//reallog.Print("Save tx ", txHash)
//}
//}
