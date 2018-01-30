# Notes from bitcoin edge lectures

Nodes

## p2p network
### Types of nodes
* Fully validating nodes
    * Validates all the transactions
    * Maintains a collection of unspent outputs
    * The most secure and private
The full nodes keeps the blockchain and the UTXO set.
Only the UTXO set is actually needed to enforce the consensus.

* Pruned nodes
    * Discarding blocks if they are all spent
    * Keeps couple of dates back
    * The cannot send you old blocks to sync
    * The are a full nodes, they are saving some space on disk

* SPV nodes
    * Only download block headers,
    * Can validate proof of work
    * Can verify that a transaction in included 
    * Cannot verify a transaction has not appeared in a blockchain
    * Use bloom filter to keep some privacy (When asking about transactions)
    * A full nodes can support up to 8 SPV nodes

### Messasge format
* Message format
    * header 24 bytes
        * 4 bytes magic (Just some indication of a bitcoin message)
        * command: 12 bytes. ADDR, INV, BLOCK (ASCII of these commands)
        * Payload 4bytes: How large is the payload
        * Checksum: Double sha256 of payload
    * Payload: Up to 32 MB

### Types of messages
* Control messages
    VERACK : messages used to establish a connection in the p2p network
    ADDR: Send information about other nodes in the network (to build the network)

* INV: inventory
    * Used to announce new transactions
    * Contain txid
    * A receiving node that wants announced inventory sends GETDATA

Receving node | transmitting node

<- INV <-
-> GETHEADERS ->
-> GETDATA ->
<- HEADERS <-
<- BLOCK <-

Compact blocks:
* Reduce the bandwidth,
* Relies on fact that peer has already seen most transactions
* You can send less information, 



### Bad nodes
* Bad nodes get 10 points for each unconnected block they send
* After a 100 the get banned

### Mempool
You receive transactions using the INV messages.
All nodes will have their own mempool, and they will be different from each other. 
Miners use the mempool to choose transactions
* Double spends are not allowed
* Transaction chains are allowed, you can use transactions not yet in a block

#### How is the mempool managed and does not explode 
* There is a limit on number of days
* After that, transactions with smaller fees start to be evicted
* Fee filter: I am no longer interested in transactions with less than X sat/byte
* Replace by fee, you can send the same TX with a higher fee
* The mempool is not consensus critical

* Transactions are seen twice, when sent by user, and when accepted into block
    Signature validation is the most costly part.
    Saving caches of validates transactions, saves the need to validate them when receiving the block
    We will still have to validate other things about the transaction (such as lock time). 
    But we can save on validating the signature again
    Signature/scirpt caching requries a mempool


