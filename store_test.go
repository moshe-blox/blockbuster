package main

import (
	"math/rand"
	"testing"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/require"
)

func TestPurge(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	require.NoError(t, err)

	store := &Store{db: db}
	defer store.Close()

	// Set 5 slots.
	const filledSlots = 5
	for _, i := range rand.Perm(filledSlots) {
		err = store.SetBlock(phase0.Slot(i), nil)
		require.NoError(t, err)
	}

	// Purge 3 slots.
	deleted, err := store.Purge(1, 3)
	require.NoError(t, err)
	require.Equal(t, 3, deleted)

	// Check that 2 slots remain.
	for i := 0; i < filledSlots; i++ {
		_, err := store.Block(phase0.Slot(i))
		if i < 1 || i > 3 {
			require.NoError(t, err)
		} else {
			require.ErrorIs(t, err, badger.ErrKeyNotFound)
		}
	}
}
