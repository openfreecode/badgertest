package main

import (
	"flag"
	"os"

	"github.com/dgraph-io/badger/v4"
	"github.com/duke-git/lancet/v2/random"
	"github.com/duke-git/lancet/v2/slice"
)

// bg gen -dir /tmp/badgergo -count 1000000 -keysize 10 -valsize 100
// bg read -dir /tmp/badgergo -key hello
// bg write -dir /tmp/badgergo -key hello -value world

type Options struct {
	Dir     string
	Key     string
	Value   string
	Count   uint64
	KeySize uint
	ValSize uint
}

var (
	bgen, bread, bwrite bool
	opts                Options
)

func init() {
	flag.BoolVar(&bgen, "gen", false, "gen")
	flag.BoolVar(&bread, "read", false, "read")
	flag.BoolVar(&bwrite, "write", false, "write")

	opts = Options{}
	flag.StringVar(&opts.Dir, "dir", "/tmp/badgergo", "dir")
	flag.StringVar(&opts.Key, "key", "", "key")
	flag.StringVar(&opts.Value, "value", "", "value")
	flag.Uint64Var(&opts.Count, "count", 10000, "kv counts")
	flag.UintVar(&opts.KeySize, "keysize", 10, "key size")
	flag.UintVar(&opts.ValSize, "valuesize", 100, "value size")
}

func main() {
	flag.Parse()

	if slice.Count[bool]([]bool{bgen, bread, bwrite}, true) != 1 {
		println("usage:")
		println("    bt -gen/-read/-write [options]")
		os.Exit(1)
	}

	switch {
	case bgen:
		db := initdb(opts)
		defer db.Close()
		err := gen(db, opts)
		if err != nil {
			errLog(err.Error())
		}
	case bread:
		db := initdb(opts)
		defer db.Close()
		err := read(db, opts)
		if err != nil {
			errLog(err.Error())
		}
	case bwrite:
		db := initdb(opts)
		defer db.Close()
		err := write(db, opts)
		if err != nil {
			errLog(err.Error())
		}
	default:
		errLog("")
	}

}

func initdb(opts Options) *badger.DB {
	optsx := badger.DefaultOptions(opts.Dir)
	db, err := badger.Open(optsx)
	if err != nil {
		panic(err)
	}
	return db
}

func gen(db *badger.DB, opts Options) error {

	txn := db.NewTransaction(true)
	defer txn.Discard()

	for i := uint64(0); i < opts.Count; i++ {
		k := randBytes(opts.KeySize)
		v := randBytes(opts.ValSize)
		if err := txn.Set(k, v); err == badger.ErrTxnTooBig {
			if err := txn.Commit(); err != nil {
				return err
			}
			txn = db.NewTransaction(true)
			if err = txn.Set([]byte(k), []byte(v)); err != nil {
				return err
			}
		}
	}

	return txn.Commit()
}

func read(db *badger.DB, opts Options) error {
	return db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(opts.Key))
		if err != nil {
			return err
		}
		item.Value(func(val []byte) error {
			println(string(val))
			return nil
		})
		return nil
	})
}

func write(db *badger.DB, opts Options) error {
	return db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(opts.Key), []byte(opts.Value))
	})
}

func randBytes(sz uint) []byte {
	return random.RandBytes(int(sz))
}

func errLog(err string) {
	println(err)
	os.Exit(1)
}
