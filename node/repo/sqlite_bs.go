package repo

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"runtime"
	"sync"

	_ "github.com/mattn/go-sqlite3"

	"github.com/filecoin-project/lotus/lib/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multibase"
)

type chainBS struct {
	db   *sql.DB
	dbMu sync.RWMutex // SQLite concurrency is surprisingly hard: https://github.com/mattn/go-sqlite3/issues/632

	stmtCache sync.Map
}

func keyFromCid(c cid.Cid) (k string) {
	// CB: we can not store complete CIDs as keys - code expects being able
	// to match on multihash alone, likely both over raw *and* CBOR :( :( :(
	//return c.Encode(multibase.MustNewEncoder(multibase.Base64))

	k, _ = multibase.Encode(multibase.Base64, c.Hash())
	return
}

func (fsr *fsLockedRepo) ChainBlockstore() (blockstore.Blockstore, error) {

	fsr.chainBsOnce.Do(func() {

		cbs := &chainBS{}

		if fsr.chainBsErr = os.MkdirAll(fsr.join(fsDatastore), 0755); fsr.chainBsErr != nil {
			return
		}

		dbFile := "file:" + fsr.join(fsDatastore, "chain.sqlite")
		pragmas := []string{
			fmt.Sprintf("PRAGMA busy_timeout = %d", 10*1000),
			fmt.Sprintf("PRAGMA threads = %d", (runtime.NumCPU()/4)+1),
			"PRAGMA synchronous = OFF",
			"PRAGMA auto_vacuum = NONE",
			"PRAGMA automatic_index = OFF",
			"PRAGMA journal_mode = OFF",
			"PRAGMA read_uncommitted = ON",
		}

		cbs.db, fsr.chainBsErr = sql.Open("sqlite3", dbFile+"?mode=rwc")
		if fsr.chainBsErr != nil {
			return
		}

		for _, sql := range pragmas {
			if _, fsr.chainBsErr = cbs.db.Exec(sql); fsr.chainBsErr != nil {
				return
			}
		}

		for _, ddl := range []string{
			"CREATE TABLE IF NOT EXISTS blocks(" +
				"multiHash TEXT NOT NULL PRIMARY KEY," +
				"initialCodecID INTEGER NOT NULL," +
				"content BLOB NOT NULL" +
				") WITHOUT ROWID",
		} {
			if _, fsr.chainBsErr = cbs.db.Exec(ddl); fsr.chainBsErr != nil {
				return
			}
		}

		// we do not currently use the Identity codec, but just in case...
		fsr.chainBs = blockstore.WrapIDStore(cbs)
	})

	if fsr.chainBsErr != nil {
		return nil, fsr.chainBsErr
	}

	return fsr.chainBs, nil
}

func (cbs *chainBS) Has(c cid.Cid) (has bool, err error) {

	cbs.dbMu.RLock()
	defer cbs.dbMu.RUnlock()

	err = cbs.prepSQL("SELECT EXISTS( SELECT 42 FROM blocks WHERE multiHash = ? )").QueryRow(
		keyFromCid(c),
	).Scan(&has)
	return
}

func (cbs *chainBS) Get(c cid.Cid) (blocks.Block, error) {

	cbs.dbMu.RLock()
	defer cbs.dbMu.RUnlock()

	var data []byte
	err := cbs.prepSQL("SELECT content FROM blocks WHERE multiHash = ?").QueryRow(
		keyFromCid(c),
	).Scan(&data)

	switch err {

	case sql.ErrNoRows:
		return nil, blockstore.ErrNotFound

	case nil:
		return blocks.NewBlockWithCid(data, c)

	default:
		return nil, err

	}
}

func (cbs *chainBS) GetSize(c cid.Cid) (size int, err error) {

	cbs.dbMu.RLock()
	defer cbs.dbMu.RUnlock()

	err = cbs.prepSQL("SELECT LENGTH(content) FROM blocks WHERE multiHash = ?").QueryRow(
		keyFromCid(c),
	).Scan(&size)

	if err == sql.ErrNoRows {
		// https://github.com/ipfs/go-ipfs-blockstore/blob/v1.0.1/blockstore.go#L183-L185
		return -1, blockstore.ErrNotFound
	}

	return
}

// Put puts a given block to the underlying datastore
func (cbs *chainBS) Put(b blocks.Block) (err error) {

	cbs.dbMu.Lock()
	defer cbs.dbMu.Unlock()

	_, err = cbs.prepSQL("INSERT OR IGNORE INTO blocks( multiHash, initialCodecID, content ) VALUES( ?, ?, ? )").Exec(
		keyFromCid(b.Cid()),
		b.Cid().Prefix().Codec,
		b.RawData(),
	)

	return
}

// Transactions are both overhead *AND* difficult with SQLite
func (cbs *chainBS) PutMany(blocks []blocks.Block) error {
	for _, b := range blocks {
		if err := cbs.Put(b); err != nil {
			return err
		}
	}
	return nil
}

// BEGIN UNIMPLEMENTED

func (cbs *chainBS) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {

	panic("AllKeysChan not implemented")

	// cbs.dbMu.RLock()

	// q, err := cbs.prepSQL("SELECT multiHash, initialCodecID FROM blocks").QueryContext(ctx)

	// retChan := make(chan cid.Cid)

	// if err == sql.ErrNoRows {
	// 	cbs.dbMu.RUnlock()
	// 	close(retChan)
	// 	return retChan, nil
	// } else if err != nil {
	// 	cbs.dbMu.RUnlock()
	// 	return nil, err
	// }

	// go func() {
	// 	defer q.Close()
	// 	defer cbs.dbMu.RUnlock()
	// 	defer close(retChan)

	// 	for q.Next() {
	// 		select {

	// 		case <-ctx.Done():
	// 			return

	// 		default:
	// 			var mhEnc string
	// 			var initCodec uint64

	// 			if err := q.Scan(&mhEnc, &initCodec); err == nil {
	// 				if _, mh, err := multibase.Decode(mhEnc); err == nil {
	// 					// CB: It seems we return varying stuff here, depending on store /o\
	// 					// https://github.com/ipfs/go-ipfs-blockstore/blob/v1.0.1/blockstore.go#L229
	// 					retChan <- cid.NewCidV1(initCodec, mh)
	// 					continue
	// 				}
	// 			}

	// 			// if we got that far: we errorred above
	// 			return
	// 		}
	// 	}
	// }()

	// return retChan, nil
}

// HashOnRead specifies if every read block should be
// rehashed to make sure it matches its CID.
func (cbs *chainBS) HashOnRead(enabled bool) {
	log.Warn("HashOnRead toggle not implemented, ignoring")
}

func (cbs *chainBS) DeleteBlock(cid.Cid) error {
	panic("DeleteBlock not permitted")
}

// END UNIMPLEMENTED

func (cbs *chainBS) prepSQL(s string) *sql.Stmt {

	if stmt, exists := cbs.stmtCache.Load(s); exists {
		return stmt.(*sql.Stmt)
	}

	newStmt, err := cbs.db.Prepare(s)
	if err != nil {
		log.Panicf("Failed to prepare SQL statement '%s': %s", s, err)
	}

	cbs.stmtCache.Store(s, newStmt)
	return newStmt
}
