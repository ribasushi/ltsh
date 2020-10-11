package sqlblockstore

import (
	"context"

	logging "github.com/ipfs/go-log/v2"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/filecoin-project/lotus/lib/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multibase"
)

var errNoRows = pgx.ErrNoRows

var dbpool *pgxpool.Pool

var log = logging.Logger("sql")

func DB() *pgxpool.Pool {
	if dbpool == nil {

		connStr := `postgres:///mainnet_mini?user=lotuser&host=/var/run/postgresql/`

		var err error
		dbpool, err = pgxpool.Connect(context.Background(), connStr)
		if err != nil {
			log.Fatalf("failed to connect to %s: %s", connStr, err)
		}

		for _, ddl := range []string{
			"CREATE TABLE IF NOT EXISTS blocks(" +
				"multiHash TEXT NOT NULL PRIMARY KEY," +
				"initialCodecID INTEGER NOT NULL," +
				"content BYTEA NOT NULL" +
				")",
			"CREATE TABLE IF NOT EXISTS heads(" +
				"seq SERIAL NOT NULL PRIMARY KEY," +
				"ts TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL ," +
				"height BIGINT NOT NULL," +
				"blockCids TEXT NOT NULL" +
				")",
			"CREATE INDEX IF NOT EXISTS height_idx ON heads ( height )",
		} {
			if _, err = dbpool.Exec(context.Background(), ddl); err != nil {
				log.Fatalf("On-connect DDL execution failed: %s", err)
			}
		}
	}

	return dbpool
}

type SqlBlockstore struct {
	db *pgxpool.Pool
}

func NewBlockStore() (blockstore.Blockstore, error) {

	sbs := &SqlBlockstore{
		db: DB(),
	}

	// we do not currently use the Identity codec, but just in case...
	return blockstore.WrapIDStore(sbs), nil
}

func keyFromCid(c cid.Cid) (k string) {
	// CB: we can not store complete CIDs as keys - code expects being able
	// to match on multihash alone, likely both over raw *and* CBOR :( :( :(
	//return c.Encode(multibase.MustNewEncoder(multibase.Base64))

	k, _ = multibase.Encode(multibase.Base64, c.Hash())
	return
}

func (sbs *SqlBlockstore) Has(c cid.Cid) (has bool, err error) {
	err = sbs.db.QueryRow(
		context.Background(),
		"SELECT EXISTS( SELECT 42 FROM blocks WHERE multiHash = $1 )",
		keyFromCid(c),
	).Scan(&has)
	return
}

func (sbs *SqlBlockstore) Get(c cid.Cid) (blocks.Block, error) {

	var data []byte
	err := sbs.db.QueryRow(
		context.Background(),
		"SELECT content FROM blocks WHERE multiHash = $1",
		keyFromCid(c),
	).Scan(&data)

	switch err {

	case errNoRows:
		return nil, blockstore.ErrNotFound

	case nil:
		return blocks.NewBlockWithCid(data, c)

	default:
		return nil, err

	}
}

func (sbs *SqlBlockstore) GetSize(c cid.Cid) (size int, err error) {

	err = sbs.db.QueryRow(
		context.Background(),
		"SELECT LENGTH(content) FROM blocks WHERE multiHash = $1",
		keyFromCid(c),
	).Scan(&size)

	if err == errNoRows {
		// https://github.com/ipfs/go-ipfs-blockstore/blob/v1.0.1/blockstore.go#L183-L185
		return -1, blockstore.ErrNotFound
	}

	return
}

func (sbs *SqlBlockstore) Put(b blocks.Block) (err error) {

	_, err = sbs.db.Exec(
		context.Background(),
		"INSERT INTO blocks( multiHash, initialCodecID, content ) VALUES( $1, $2, $3 ) ON CONFLICT (multiHash) DO NOTHING",
		keyFromCid(b.Cid()),
		b.Cid().Prefix().Codec,
		b.RawData(),
	)

	return
}

func (sbs *SqlBlockstore) PutMany(blks []blocks.Block) error {
	tx, err := sbs.db.BeginTx(context.Background(), pgx.TxOptions{IsoLevel: pgx.ReadUncommitted})
	if err != nil {
		return err
	}
	for _, b := range blks {
		if err := sbs.Put(b); err != nil {
			return err
		}
	}
	return tx.Commit(context.Background())
}

// BEGIN UNIMPLEMENTED

func (sbs *SqlBlockstore) AllKeysChan(context.Context) (<-chan cid.Cid, error) {
	panic("AllKeysChan not implemented")
}

func (sbs *SqlBlockstore) HashOnRead(enabled bool) {
	log.Warn("HashOnRead toggle not implemented, ignoring")
}

func (sbs *SqlBlockstore) DeleteBlock(cid.Cid) error {
	panic("DeleteBlock not permitted")
}

// END UNIMPLEMENTED
