/*
Copyright SecureKey Technologies Inc. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

// Package newstorage contains common tests for newstorage implementation.
//
package newstorage

import (
	"errors"
	"testing"

	"github.com/hyperledger/aries-framework-go/pkg/storage"

	"github.com/google/uuid"
	"github.com/hyperledger/aries-framework-go/pkg/newstorage"
	"github.com/stretchr/testify/require"
)

// TestAll tests all storage methods.
func TestAll(t *testing.T, provider newstorage.Provider) {
	t.Helper()

	t.Run("Store put and get", func(t *testing.T) {
		TestPutGet(t, provider)
	})

	t.Run("Multi store put and get", func(t *testing.T) {
		TestMultiStorePutGet(t, provider)
	})

	t.Run("Delete", func(t *testing.T) {
		TestDelete(t, provider)
	})
}

// TestPutGet tests Put and Get methods.
func TestPutGet(t *testing.T, provider newstorage.Provider) {
	t.Helper()

	store, err := provider.OpenStore(randomKey())
	require.NoError(t, err)

	const key = "did:example:123"

	data := []byte("value")

	err = store.Put(key, data)
	require.NoError(t, err)

	doc, err := store.Get(key)
	require.NoError(t, err)
	require.NotEmpty(t, doc)
	require.Equal(t, data, doc)

	// test update
	data = []byte(`{"key1":"value1"}`)
	err = store.Put(key, data)
	require.NoError(t, err)

	doc, err = store.Get(key)
	require.NoError(t, err)
	require.NotEmpty(t, doc)
	require.Equal(t, data, doc)

	// test update
	update := []byte(`{"_key1":"value1"}`)
	err = store.Put(key, update)
	require.NoError(t, err)

	doc, err = store.Get(key)
	require.NoError(t, err)
	require.NotEmpty(t, doc)
	require.Equal(t, update, doc)

	did2 := "did:example:789"
	_, err = store.Get(did2)
	require.True(t, errors.Is(err, storage.ErrDataNotFound))

	// nil key
	_, err = store.Get("")
	require.Error(t, err)

	// nil value
	err = store.Put(key, nil)
	require.Error(t, err)

	// nil key
	err = store.Put("", data)
	require.Error(t, err)
}

// TestMultiStorePutGet tests Put and Get methods.
func TestMultiStorePutGet(t *testing.T, provider newstorage.Provider) {
	t.Helper()

	const commonKey = "did:example:1"

	data := []byte("value1")
	// create store 1 & store 2
	store1name := randomKey()
	store1, err := provider.OpenStore(store1name)
	require.NoError(t, err)

	store2, err := provider.OpenStore(randomKey())
	require.NoError(t, err)

	// put in store 1
	err = store1.Put(commonKey, data)
	require.NoError(t, err)

	// get in store 1 - found
	doc, err := store1.Get(commonKey)
	require.NoError(t, err)
	require.NotEmpty(t, doc)
	require.Equal(t, data, doc)

	// get in store 2 - not found
	doc, err = store2.Get(commonKey)
	require.Error(t, err)
	require.Equal(t, err, storage.ErrDataNotFound)
	require.Empty(t, doc)

	// put in store 2
	err = store2.Put(commonKey, data)
	require.NoError(t, err)

	// get in store 2 - found
	doc, err = store2.Get(commonKey)
	require.NoError(t, err)
	require.NotEmpty(t, doc)
	require.Equal(t, data, doc)

	// create new store 3 with same name as store1
	store3, err := provider.OpenStore(store1name)
	require.NoError(t, err)

	// get in store 3 - found
	doc, err = store3.Get(commonKey)
	require.NoError(t, err)
	require.NotEmpty(t, doc)
	require.Equal(t, data, doc)
}

// TestDelete tests Delete method.
func TestDelete(t *testing.T, provider newstorage.Provider) {
	t.Helper()

	const commonKey = "did:example:1234"

	data := []byte("value1")

	// create store 1 & store 2
	store, err := provider.OpenStore(randomKey())
	require.NoError(t, err)

	// put in store 1
	err = store.Put(commonKey, data)
	require.NoError(t, err)

	// get in store 1 - found
	doc, err := store.Get(commonKey)
	require.NoError(t, err)
	require.NotEmpty(t, doc)
	require.Equal(t, data, doc)

	// now try Delete with an empty key - should fail
	err = store.Delete("")
	require.EqualError(t, err, "key is mandatory")

	err = store.Delete("k1")
	require.NoError(t, err)

	// finally test Delete an existing key
	err = store.Delete(commonKey)
	require.NoError(t, err)

	doc, err = store.Get(commonKey)
	require.EqualError(t, err, storage.ErrDataNotFound.Error())
	require.Empty(t, doc)
}

func randomKey() string {
	// prefix `key` is needed for couchdb due to error e.g Name: '7c80bdcd-b0e3-405a-bb82-fae75f9f2470'.
	// Only lowercase characters (a-z), digits (0-9), and any of the characters _, $, (, ), +, -, and / are allowed.
	// Must begin with a letter.
	return "key" + uuid.New().String()
}
