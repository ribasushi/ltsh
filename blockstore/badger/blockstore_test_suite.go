// stm: #unit
package badgerbs

import (
	"context"
	"io"
	"reflect"
	"strings"
	"testing"

	u "github.com/ipfs/boxo/util"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/lotus/blockstore"
)

// TODO: move this to go-ipfs-blockstore.
type Suite struct {
	NewBlockstore  func(tb testing.TB) (bs blockstore.BasicBlockstore, path string)
	OpenBlockstore func(tb testing.TB, path string) (bs blockstore.BasicBlockstore, err error)
}

func (s *Suite) RunTests(t *testing.T, prefix string) {
	v := reflect.TypeOf(s)
	f := func(t *testing.T) {
		for i := 0; i < v.NumMethod(); i++ {
			if m := v.Method(i); strings.HasPrefix(m.Name, "Test") {
				f := m.Func.Interface().(func(*Suite, *testing.T))
				t.Run(m.Name, func(t *testing.T) {
					f(s, t)
				})
			}
		}
	}

	if prefix == "" {
		f(t)
	} else {
		t.Run(prefix, f)
	}
}

func (s *Suite) TestGetWhenKeyNotPresent(t *testing.T) {
	//stm: @SPLITSTORE_BADGER_OPEN_001, @SPLITSTORE_BADGER_CLOSE_001
	//stm: @SPLITSTORE_BADGER_GET_001, @SPLITSTORE_BADGER_POOLED_STORAGE_KEY_001
	ctx := context.Background()
	bs, _ := s.NewBlockstore(t)
	if c, ok := bs.(io.Closer); ok {
		defer func() { require.NoError(t, c.Close()) }()
	}

	c := cid.NewCidV0(u.Hash([]byte("stuff")))
	bl, err := bs.Get(ctx, c)
	require.Nil(t, bl)
	require.Equal(t, ipld.ErrNotFound{Cid: c}, err)
}

func (s *Suite) TestGetWhenKeyIsNil(t *testing.T) {
	//stm: @SPLITSTORE_BADGER_OPEN_001, @SPLITSTORE_BADGER_CLOSE_001
	//stm: @SPLITSTORE_BADGER_GET_001
	ctx := context.Background()
	bs, _ := s.NewBlockstore(t)
	if c, ok := bs.(io.Closer); ok {
		defer func() { require.NoError(t, c.Close()) }()
	}

	_, err := bs.Get(ctx, cid.Undef)
	require.Equal(t, ipld.ErrNotFound{Cid: cid.Undef}, err)
}

func (s *Suite) TestPutThenGetBlock(t *testing.T) {
	//stm: @SPLITSTORE_BADGER_OPEN_001, @SPLITSTORE_BADGER_CLOSE_001
	//stm: @SPLITSTORE_BADGER_PUT_001, @SPLITSTORE_BADGER_POOLED_STORAGE_KEY_001
	//stm: @SPLITSTORE_BADGER_GET_001
	ctx := context.Background()
	bs, _ := s.NewBlockstore(t)
	if c, ok := bs.(io.Closer); ok {
		defer func() { require.NoError(t, c.Close()) }()
	}

	orig := blocks.NewBlock([]byte("some data"))

	err := bs.Put(ctx, orig)
	require.NoError(t, err)

	fetched, err := bs.Get(ctx, orig.Cid())
	require.NoError(t, err)
	require.Equal(t, orig.RawData(), fetched.RawData())
}

func (s *Suite) TestHas(t *testing.T) {
	//stm: @SPLITSTORE_BADGER_OPEN_001, @SPLITSTORE_BADGER_CLOSE_001
	//stm: @SPLITSTORE_BADGER_HAS_001, @SPLITSTORE_BADGER_POOLED_STORAGE_KEY_001
	ctx := context.Background()
	bs, _ := s.NewBlockstore(t)
	if c, ok := bs.(io.Closer); ok {
		defer func() { require.NoError(t, c.Close()) }()
	}

	orig := blocks.NewBlock([]byte("some data"))

	err := bs.Put(ctx, orig)
	require.NoError(t, err)

	ok, err := bs.Has(ctx, orig.Cid())
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = bs.Has(ctx, blocks.NewBlock([]byte("another thing")).Cid())
	require.NoError(t, err)
	require.False(t, ok)
}

func (s *Suite) TestCidv0v1(t *testing.T) {
	//stm: @SPLITSTORE_BADGER_OPEN_001, @SPLITSTORE_BADGER_CLOSE_001
	//stm: @SPLITSTORE_BADGER_PUT_001, @SPLITSTORE_BADGER_POOLED_STORAGE_KEY_001
	//stm: @SPLITSTORE_BADGER_GET_001
	ctx := context.Background()
	bs, _ := s.NewBlockstore(t)
	if c, ok := bs.(io.Closer); ok {
		defer func() { require.NoError(t, c.Close()) }()
	}

	orig := blocks.NewBlock([]byte("some data"))

	err := bs.Put(ctx, orig)
	require.NoError(t, err)

	fetched, err := bs.Get(ctx, cid.NewCidV1(cid.DagProtobuf, orig.Cid().Hash()))
	require.NoError(t, err)
	require.Equal(t, orig.RawData(), fetched.RawData())
}

func (s *Suite) TestDoubleClose(t *testing.T) {
	//stm: @SPLITSTORE_BADGER_OPEN_001, @SPLITSTORE_BADGER_CLOSE_001
	bs, _ := s.NewBlockstore(t)
	c, ok := bs.(io.Closer)
	if !ok {
		t.SkipNow()
	}
	require.NoError(t, c.Close())
	require.NoError(t, c.Close())
}

func (s *Suite) TestReopenPutGet(t *testing.T) {
	//stm: @SPLITSTORE_BADGER_OPEN_001, @SPLITSTORE_BADGER_CLOSE_001
	//stm: @SPLITSTORE_BADGER_PUT_001, @SPLITSTORE_BADGER_POOLED_STORAGE_KEY_001
	//stm: @SPLITSTORE_BADGER_GET_001
	ctx := context.Background()
	bs, path := s.NewBlockstore(t)
	c, ok := bs.(io.Closer)
	if !ok {
		t.SkipNow()
	}

	orig := blocks.NewBlock([]byte("some data"))
	err := bs.Put(ctx, orig)
	require.NoError(t, err)

	err = c.Close()
	require.NoError(t, err)

	bs, err = s.OpenBlockstore(t, path)
	require.NoError(t, err)

	fetched, err := bs.Get(ctx, orig.Cid())
	require.NoError(t, err)
	require.Equal(t, orig.RawData(), fetched.RawData())

	err = bs.(io.Closer).Close()
	require.NoError(t, err)
}

func (s *Suite) TestPutMany(t *testing.T) {
	//stm: @SPLITSTORE_BADGER_OPEN_001, @SPLITSTORE_BADGER_CLOSE_001
	//stm: @SPLITSTORE_BADGER_HAS_001, @SPLITSTORE_BADGER_POOLED_STORAGE_KEY_001
	//stm: @SPLITSTORE_BADGER_GET_001, @SPLITSTORE_BADGER_PUT_MANY_001
	//stm: @SPLITSTORE_BADGER_ALL_KEYS_CHAN_001
	ctx := context.Background()
	bs, _ := s.NewBlockstore(t)
	if c, ok := bs.(io.Closer); ok {
		defer func() { require.NoError(t, c.Close()) }()
	}

	blks := []blocks.Block{
		blocks.NewBlock([]byte("foo1")),
		blocks.NewBlock([]byte("foo2")),
		blocks.NewBlock([]byte("foo3")),
	}
	err := bs.PutMany(ctx, blks)
	require.NoError(t, err)

	for _, blk := range blks {
		fetched, err := bs.Get(ctx, blk.Cid())
		require.NoError(t, err)
		require.Equal(t, blk.RawData(), fetched.RawData())

		ok, err := bs.Has(ctx, blk.Cid())
		require.NoError(t, err)
		require.True(t, ok)
	}
}
