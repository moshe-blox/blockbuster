package main

import (
	"context"
	"encoding/binary"
	"log"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/attestantio/go-eth2-client/spec"
	"github.com/attestantio/go-eth2-client/spec/altair"
	"github.com/attestantio/go-eth2-client/spec/bellatrix"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/dgraph-io/badger/v3"
	"github.com/klauspost/compress/snappy"
)

const (
	gcInterval = 30 * time.Minute
)

var (
	keyBoundaries = []byte{0}
	keySlot       = []byte{1}
)

type Store struct {
	db     *badger.DB
	ctx    context.Context
	cancel func()
}

func OpenStore(dir, network string) (*Store, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	opt := badger.DefaultOptions(filepath.Join(dir, network))
	opt.Logger = nil
	db, err := badger.Open(opt)
	if err != nil {
		return nil, err
	}

	s := &Store{
		db: db,
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())

	// Garbage collection.
	go s.gc()

	return s, nil
}

func (s *Store) gc() {
	ticker := time.NewTicker(gcInterval)
	defer ticker.Stop()
	for {
		start := time.Now()
		err := s.db.RunValueLogGC(0.7)
		if err != nil {
			log.Printf("Error running value log GC: %v", err)
		}
		log.Printf("BadgerDB GC took %v", time.Since(start))
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Store) Filled(slot phase0.Slot) (bool, error) {
	var exists bool
	err := s.db.View(func(txn *badger.Txn) error {
		var slotBytes [8]byte
		binary.BigEndian.PutUint64(slotBytes[:], uint64(slot))
		_, err := txn.Get(append(keySlot, slotBytes[:]...))
		if err == nil {
			exists = true
		} else if err != badger.ErrKeyNotFound {
			return err
		}
		return nil
	})
	return exists, err
}

func (s *Store) Count() (slots, blocks int, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(keySlot); it.ValidForPrefix(keySlot); it.Next() {
			slots++

			err := it.Item().Value(func(val []byte) error {
				version := spec.DataVersion(binary.BigEndian.Uint64(val[:8]))
				if version != spec.DataVersion(math.MaxInt) {
					blocks++
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	return
}

type BlockWithRoot struct {
	BlockRoot phase0.Root
	*spec.VersionedSignedBeaconBlock
}

func (s *Store) Block(slot phase0.Slot) (*BlockWithRoot, error) {
	block := &BlockWithRoot{VersionedSignedBeaconBlock: &spec.VersionedSignedBeaconBlock{}}
	err := s.db.View(func(txn *badger.Txn) error {
		// 1) Read slot from key.
		var slotBytes [8]byte
		binary.BigEndian.PutUint64(slotBytes[:], uint64(slot))
		item, err := txn.Get(append(keySlot, slotBytes[:]...))
		if err != nil {
			return err
		}

		// 2) Copy the value.
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		// 2.1) Read version.
		block.Version = spec.DataVersion(binary.BigEndian.Uint64(val[:8]))
		if block.Version == spec.DataVersion(math.MaxInt) {
			// No block for this slot.
			block = nil
			return nil
		}

		// 2.2) Read root.
		copy(block.BlockRoot[:], val[8:40])

		// 2.3) Read block.
		blockBytes, err := snappy.Decode(nil, val[40:])
		if err != nil {
			return err
		}
		switch block.Version {
		case spec.DataVersionPhase0:
			block.Phase0 = &phase0.SignedBeaconBlock{}
			if err := block.Phase0.UnmarshalSSZ(blockBytes); err != nil {
				return err
			}
		case spec.DataVersionAltair:
			block.Altair = &altair.SignedBeaconBlock{}
			if err := block.Altair.UnmarshalSSZ(blockBytes); err != nil {
				return err
			}
		case spec.DataVersionBellatrix:
			block.Bellatrix = &bellatrix.SignedBeaconBlock{}
			if err := block.Bellatrix.UnmarshalSSZ(blockBytes); err != nil {
				return err
			}
		}
		return nil
	})
	return block, err
}

func (s *Store) SetBlock(slot phase0.Slot, block *BlockWithRoot) error {
	return s.db.Update(func(txn *badger.Txn) error {
		var slotBytes [8]byte
		binary.BigEndian.PutUint64(slotBytes[:], uint64(slot))

		var versionBytes [8]byte
		if block == nil {
			binary.BigEndian.PutUint64(versionBytes[:], math.MaxInt)
		} else {
			binary.BigEndian.PutUint64(versionBytes[:], uint64(block.Version))
		}

		var root phase0.Root
		if block != nil {
			root = block.BlockRoot
		}

		var blockBytes []byte
		if block != nil {
			var (
				b   []byte
				err error
			)
			switch block.Version {
			case spec.DataVersionPhase0:
				b, err = block.Phase0.MarshalSSZ()
			case spec.DataVersionAltair:
				b, err = block.Altair.MarshalSSZ()
			case spec.DataVersionBellatrix:
				b, err = block.Bellatrix.MarshalSSZ()
			}
			if err != nil {
				return err
			}
			blockBytes = snappy.Encode(nil, b)
		}

		value := make([]byte, 0, len(versionBytes)+len(root)+len(blockBytes))
		value = append(value, versionBytes[:]...)
		value = append(value, root[:]...)
		value = append(value, blockBytes[:]...)

		return txn.Set(
			append(keySlot, slotBytes[:]...),
			value,
		)
	})
}

// Purge removes all slots within the given range (inclusive).
func (s *Store) Purge(from, to phase0.Slot) (deleted int, err error) {
	err = s.db.Update(func(txn *badger.Txn) error {
		var fromBytes [8]byte
		binary.BigEndian.PutUint64(fromBytes[:], uint64(from))

		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		fromKey := append(keySlot, fromBytes[:]...)
		for it.Seek(fromKey); it.ValidForPrefix(keySlot); it.Next() {
			key := it.Item().KeyCopy(nil)
			slot := phase0.Slot(binary.BigEndian.Uint64(key[len(keySlot):]))
			if slot > to {
				break
			}
			if err := txn.Delete(key); err != nil {
				return err
			}
			deleted++
		}
		return nil
	})
	return
}

func (s *Store) Close() error {
	s.cancel()
	return s.db.Close()
}
