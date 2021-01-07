/*
Copyright SecureKey Technologies Inc. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

// Package couchdb implements a storage interface for Aries (aries-framework-go).
package newcouchdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff"

	// The CouchDB driver.
	_ "github.com/go-kivik/couchdb"
	"github.com/go-kivik/kivik"
	"github.com/hyperledger/aries-framework-go/pkg/newstorage"
)

const (
	blankHostErrMsg    = "hostURL for new CouchDB provider can't be blank"
	couchDBNotFoundErr = "Not Found:"
	couchDBUsersTable  = "_users"
	getBulkKeyNotFound = "no value found for key %s: %w"
	designDocumentName = "AriesStorageDesignDocument"
)

// Provider represents a CouchDB implementation of the storage.Provider interface.
type Provider struct {
	log           logger
	hostURL       string
	couchDBClient *kivik.Client
	dbPrefix      string
}

type logger interface {
	Warnf(msg string, args ...interface{})
}

var errGetBulkKeysStringSliceNil = errors.New("keys string slice cannot be nil")

// Option configures the couchdb provider.
type Option func(opts *Provider)

// WithDBPrefix option is for adding prefix to db name.
func WithDBPrefix(dbPrefix string) Option {
	return func(opts *Provider) {
		opts.dbPrefix = dbPrefix
	}
}

// PingCouchDB performs a readiness check on the CouchDB url.
func PingCouchDB(url string) error {
	if url == "" {
		return errors.New(blankHostErrMsg)
	}

	client, err := kivik.New("couch", url)
	if err != nil {
		return err
	}

	exists, err := client.DBExists(context.Background(), couchDBUsersTable)
	if err != nil {
		return fmt.Errorf("failed to probe couchdb for '%s' DB at %s: %w", couchDBUsersTable, url, err)
	}

	if !exists {
		return fmt.Errorf(
			"'%s' DB does not yet exist - CouchDB might not be fully initialized", couchDBUsersTable)
	}

	return nil
}

// NewProvider instantiates Provider.
// Certain stores like couchdb cannot accept key IDs with '_' prefix, to avoid getting errors with such values, key ID
// need to be base58 encoded for these stores. In order to do so, the store must be wrapped (using base58 or
// prefix wrapper).
func NewProvider(hostURL string, opts ...Option) (*Provider, error) {
	err := PingCouchDB(hostURL)
	if err != nil {
		return nil, fmt.Errorf("failed to ping couchDB: %w", err)
	}

	client, err := kivik.New("couch", hostURL)
	if err != nil {
		return nil, err
	}

	p := &Provider{hostURL: hostURL, couchDBClient: client}

	for _, opt := range opts {
		opt(p)
	}

	return p, nil
}

// TODO DEREK deal with Kivik issue - maybe URL encoding?

// OpenStore opens an existing store with the given name and returns it.
func (p *Provider) OpenStore(name string) (newstorage.Store, error) {
	if p.dbPrefix != "" {
		name = p.dbPrefix + "_" + name
	}

	err := p.couchDBClient.CreateDB(context.Background(), name)
	if err != nil {
		if err.Error() != "Precondition Failed: The database could not be created, the file already exists." {
			return nil, fmt.Errorf("failed to create db: %w", err)
		}
	}

	db := p.couchDBClient.DB(context.Background(), name)
	if db.Err() != nil {
		return nil, db.Err()
	}

	return &Store{log: p.log, db: db}, nil
}

// If duplicate tags are provided, then CouchDB simply ignores them.
func (p *Provider) SetStoreConfig(name string, config newstorage.StoreConfiguration) error {
	// TODO DEREK: possible to pass in nil stuff here?
	if config.TagNames == nil {
		return errors.New("store configuration tag names cannot be nil")
	}

	if p.dbPrefix != "" {
		name = p.dbPrefix + "_" + name
	}

	for _, tagName := range config.TagNames {
		if tagName == "payload" {
			return errors.New(`tag name cannot be "payload" as it is a reserved keyword`)
		}
	}

	db := p.couchDBClient.DB(context.Background(), name)
	err := db.Err()
	if err != nil {
		return db.Err()
	}

	existingIndexes, err := db.GetIndexes(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get existingIndexes in CouchDB: %w", err)
	}

	tagNamesAlreadyConfigured := make(map[string]struct{})

	for _, existingIndex := range existingIndexes {
		if existingIndex.Name != "_all_docs" { // _all_docs is the CouchDB default index on the document ID

			existingTagName := strings.TrimSuffix(existingIndex.Name, "_index")

			var isInNewConfig bool
			for _, tagName := range config.TagNames {
				if existingTagName == tagName {
					isInNewConfig = true
					tagNamesAlreadyConfigured[tagName] = struct{}{}
					break
				}
			}

			// if the new store configuration doesn't have the existing index (tag) defined, then we will delete it
			if !isInNewConfig {
				_ = db.DeleteIndex(context.Background(), designDocumentName, existingIndex.Name)
			}
		}
	}

	for _, tag := range config.TagNames {
		_, indexAlreadyCreated := tagNamesAlreadyConfigured[tag]

		if !indexAlreadyCreated {
			err := db.CreateIndex(context.Background(), "AriesStorageDesignDoc", tag+"_index",
				`{"fields": ["`+tag+`"]}`)
			if err != nil {
				return fmt.Errorf("failed to create index in CouchDB: %w", err)
			}
		}
	}

	return nil
}

func (p *Provider) GetStoreConfig(name string) (newstorage.StoreConfiguration, error) {
	if p.dbPrefix != "" {
		name = p.dbPrefix + "_" + name
	}

	db := p.couchDBClient.DB(context.Background(), name)
	err := db.Err()
	if err != nil {
		return newstorage.StoreConfiguration{}, db.Err()
	}

	indexes, err := db.GetIndexes(context.Background())
	if err != nil {
		return newstorage.StoreConfiguration{}, fmt.Errorf("failed to get indexes in CouchDB: %w", err)
	}

	var tags []string
	for _, index := range indexes {
		if index.Name != "_all_docs" { // _all_docs is the CouchDB default index on the document ID
			tags = append(tags, strings.TrimSuffix(index.Name, "_index"))
		}
	}

	return newstorage.StoreConfiguration{TagNames: tags}, nil
}

// Close closes the provider.
func (p *Provider) Close() error {
	err := p.couchDBClient.Close(context.Background())
	if err != nil {
		return fmt.Errorf("failed to close database via client: %w", err)
	}

	return nil
}

// Store represents a CouchDB-backed database.
type Store struct {
	log logger
	db  *kivik.DB
}

// Put stores the key + value pair along with the (optional) tags.
func (s *Store) Put(k string, v []byte, tags ...newstorage.Tag) error {
	if k == "" {
		return errors.New("key cannot be empty")
	}

	if v == nil {
		return errors.New("value cannot be nil")
	}

	valuesMapToMarshal := make(map[string]string)

	valuesMapToMarshal["payload"] = string(v)

	for _, tag := range tags {
		if tag.Name == "payload" {
			return errors.New(`tag name cannot be "payload" as it is a reserved keyword`)
		}

		valuesMapToMarshal[tag.Name] = tag.Value
	}

	valueToPut, _ := json.Marshal(valuesMapToMarshal)

	return s.put(k, valueToPut)
}

// Get retrieves the value in the store associated with the given key.
func (s *Store) Get(k string) ([]byte, error) {
	if k == "" {
		return nil, errors.New("key is mandatory")
	}

	rawDoc := make(map[string]interface{})

	row := s.db.Get(context.Background(), k)

	err := row.ScanDoc(&rawDoc)
	if err != nil {
		if strings.Contains(err.Error(), couchDBNotFoundErr) {
			return nil, newstorage.ErrDataNotFound
		}

		return nil, err
	}

	storedValue, err := s.getStoredValueFromRawDoc(rawDoc)
	if err != nil {
		return nil, fmt.Errorf("failed to get payload from raw document: %w", err)
	}

	return storedValue, nil
}

func (s *Store) GetTags(k string) ([]newstorage.Tag, error) {
	if k == "" {
		return nil, errors.New("key is mandatory")
	}

	rawDoc := make(map[string]interface{})

	row := s.db.Get(context.Background(), k)

	err := row.ScanDoc(&rawDoc)
	if err != nil {
		if strings.Contains(err.Error(), couchDBNotFoundErr) {
			return nil, newstorage.ErrDataNotFound
		}

		return nil, err
	}

	var tags []newstorage.Tag

	for key, value := range rawDoc {
		// Any key that isn't one of the reserved keywords below must be a tag.
		if key != "_id" && key != "_rev" && key != "payload" {
			valueString, ok := value.(string)
			if !ok {
				return nil, errors.New("failed to assert tag value as string")
			}
			tags = append(tags, newstorage.Tag{
				Name:  key,
				Value: valueString,
			})
		}
	}

	return tags, nil
}

// TODO DEREK: Make it not all-or-nothing
func (s *Store) GetBulk(keys ...string) ([][]byte, error) {
	if keys == nil {
		return nil, errGetBulkKeysStringSliceNil
	}

	rawDocs, err := s.getRawDocs(keys)
	if err != nil {
		return nil, fmt.Errorf(getRawDocsFailureErrMsg, err)
	}

	values, err := s.getStoredValuesFromRawDocs(rawDocs, keys)
	if err != nil {
		return nil, fmt.Errorf(failureWhileGettingStoredValuesFromRawDocs, err)
	}

	return values, nil
}

// TODO Derek use options
// TODO Derek looks like use_index might not be needed unless it's a partial index. Keep anyway?
func (s *Store) Query(expression string, options ...newstorage.QueryOption) (newstorage.Iterator, error) {
	expressionSplit := strings.Split(expression, ":")

	switch len(expressionSplit) {
	case 1:
		expressionTagName := expressionSplit[0]
		findQuery := `{"selector":{"` + expressionTagName + `":{"$exists":true}},"use_index": ["` + designDocumentName +
			`","` + expressionTagName + `"]}`
		resultRows, err := s.db.Find(context.Background(), findQuery)
		if err != nil {
			return nil, err
		}

		return &couchDBResultsIterator{store: s, resultRows: resultRows}, nil
	case 2:
		expressionTagName := expressionSplit[0]
		expressionTagValue := expressionSplit[1]

		findQuery := `{"selector":{"` + expressionTagName + `":"` + expressionTagValue + `"}},"use_index": ["` +
			designDocumentName + `","` + expressionTagName + `"]}`
		resultRows, err := s.db.Find(context.Background(), findQuery)
		if err != nil {
			return nil, err
		}

		return &couchDBResultsIterator{store: s, resultRows: resultRows}, nil
	default:
		return &couchDBResultsIterator{},
			errors.New("invalid expression format. it must be in the following format: TagName:TagValue")
	}
}

// Delete will delete the record with key k.
func (s *Store) Delete(k string) error {
	if k == "" {
		return errors.New("key is mandatory")
	}

	revID, err := s.getRevID(k)
	if err != nil {
		return err
	}

	// no error if nothing to delete
	if revID == "" {
		return nil
	}

	_, err = s.db.Delete(context.TODO(), k, revID)
	if err != nil {
		return fmt.Errorf("failed to delete doc: %w", err)
	}

	return nil
}

func (s *Store) Batch(operations []newstorage.Operation) error {
	// TODO: deal with duplicate keys and also check what happens if there's a delete after a put. Does the same
	// (ignore the last) behaviour remain?
	// If CouchDB receives the same key multiple times, it will just keep the first change and disregard the rest.
	// We want the opposite behaviour - we need it to only keep the last change and disregard the earlier ones as if
	// they've been overwritten.
	// Note: couchDB will not have history of those duplicates.
	// operationsWithoutDuplicates := removeDuplicatesKeepingOnlyLast(operations)

	// valuesToPut := make([][]byte, len(keys))

	// DEREK Will need revID and stuff for deleted docs...
	keys := make([]string, len(operations))

	for i, operation := range operations {
		keys[i] = operation.Key
	}

	rawDocs, err := s.getRawDocs(keys)
	if err != nil {
		return fmt.Errorf(getRawDocsFailureErrMsg, err)
	}

	valuesToPut := make([]interface{}, len(rawDocs))

	for i, rawDoc := range rawDocs {
		if operations[i].Value == nil { // This operation is a delete
			rawDoc["_deleted"] = true
			valuesToPut[i] = rawDoc
		} else { // This operation is a put
			rawDoc["payload"] = operations[i].Value
		}
	}

	_, err = s.db.BulkDocs(context.Background(), valuesToPut)
	if err != nil {
		return fmt.Errorf(failureWhileDoingBulkDocsCall, err)
	}

	return nil
}

// CloseStore closes a previously opened store.
func (s *Store) Close() error {
	return s.db.Close(context.Background())
}

func (s *Store) put(k string, value []byte) error {
	const maxRetries = 3

	return backoff.Retry(func() error {
		revID, err := s.getRevID(k)
		if err != nil {
			return err
		}

		valueToPut := value

		if revID != "" {
			valueToPut = []byte(`{"_rev":"` + revID + `",` + string(valueToPut[1:]))
		}

		_, err = s.db.Put(context.Background(), k, valueToPut)
		if err != nil && strings.Contains(err.Error(), "Document update conflict") {
			return err
		}

		// if an error is not `Document update conflict` it will be marked as permanent.
		// It means that retry logic will not be applicable.
		return backoff.Permanent(err)
	}, backoff.WithMaxRetries(backoff.NewConstantBackOff(time.Millisecond), maxRetries))
}

func (s *Store) getRevID(k string) (string, error) {
	rawDoc := make(map[string]interface{})

	row := s.db.Get(context.Background(), k)

	err := row.ScanDoc(&rawDoc)
	if err != nil {
		if strings.Contains(err.Error(), couchDBNotFoundErr) {
			return "", nil
		}

		return "", err
	}

	return rawDoc["_rev"].(string), nil
}

func (s *Store) getRevIDs(operations []newstorage.Operation) ([]string, error) {
	keys := make([]string, len(operations))

	for i, operation := range operations {
		keys[i] = operation.Key
	}

	rawDocs, err := s.getRawDocs(keys)
	if err != nil {
		return nil, fmt.Errorf(getRawDocsFailureErrMsg, err)
	}

	revIDStrings := make([]string, len(rawDocs))

	for i, rawDoc := range rawDocs {
		if rawDoc == nil {
			continue
		}

		// If we're writing over what is currently a deleted document (from CouchDB's point of view),
		// then we must ensure we don't include a revision ID, otherwise CouchDB keeps the document in a "deleted"
		// state and it won't be retrievable.
		isDeleted, containsIsDeleted := rawDoc["_deleted"]
		if containsIsDeleted {
			isDeletedBool, ok := isDeleted.(bool)
			if !ok {
				return nil, errFailToAssertDeletedAsBool
			}

			if isDeletedBool {
				continue
			}
		}

		revID, containsRevID := rawDoc["_rev"]
		if !containsRevID {
			return nil, errMissingRevIDField
		}

		revIDString, ok := revID.(string)
		if !ok {
			return nil, errFailToAssertRevIDAsString
		}

		revIDStrings[i] = revIDString
	}

	return revIDStrings, nil
}

func (s *Store) getStoredValueFromRawDoc(rawDoc map[string]interface{}) ([]byte, error) {
	strippedJSON, err := json.Marshal(rawDoc["payload"]) // TODO DEREK Is this right?
	if err != nil {
		return nil, err
	}

	return strippedJSON, nil
}

func (s *Store) getStoredValuesFromRawDocs(rawDocs []map[string]interface{}, keys []string) ([][]byte, error) {
	storedValues := make([][]byte, len(keys))

	for i, rawDoc := range rawDocs {
		if rawDoc == nil {
			return nil, fmt.Errorf(getBulkKeyNotFound, keys[i], newstorage.ErrDataNotFound)
		}

		// CouchDB still returns a raw document if the key has been deleted, so if this is a "deleted" raw document
		// then we need to return the "value not found" error in order to maintain consistent behaviour with
		// other storage implementations.
		isDeleted, containsIsDeleted := rawDoc["_deleted"]
		if containsIsDeleted {
			isDeletedBool, ok := isDeleted.(bool)
			if !ok {
				return nil, errFailToAssertDeletedAsBool
			}

			if isDeletedBool {
				return nil, fmt.Errorf(getBulkKeyNotFound, keys[i], newstorage.ErrDataNotFound)
			}
		}

		storedValue, err := s.getStoredValueFromRawDoc(rawDoc)
		if err != nil {
			return nil, fmt.Errorf("failed to get payload from raw document: %w", err)
		}

		storedValues[i] = storedValue
	}

	return storedValues, nil
}

// getRawDocs returns the raw documents from CouchDB using a bulk REST call.
// If a document is not found, then the raw document will be nil. It is not considered an error.
func (s *Store) getRawDocs(keys []string) ([]map[string]interface{}, error) {
	rawDocs := make([]map[string]interface{}, len(keys))

	bulkGetReferences := make([]kivik.BulkGetReference, len(keys))

	for i, key := range keys {
		bulkGetReferences[i].ID = key
	}

	// TODO DEREK: See if it's possible to just grab the reference IDs directly instead of pulling down the entire
	// raw documents.
	rows, err := s.db.BulkGet(context.Background(), bulkGetReferences)
	if err != nil {
		return nil, err
	}

	ok := rows.Next()

	if !ok {
		return nil, errors.New("bulk get from CouchDB was unexpectedly empty")
	}

	for i := 0; i < len(rawDocs); i++ {
		err := rows.ScanDoc(&rawDocs[i])
		// In the getRawDoc method, Kivik actually returns a different error message if a document was deleted.
		// When doing a bulk get, instead Kivik doesn't return an error message, and we have to check the "_deleted"
		// field in the raw doc. This is done in the getRevIDs method.
		if err != nil && !strings.Contains(err.Error(), bulkGetDocNotFoundErrMsgFromKivik) {
			return nil, fmt.Errorf(failureWhileScanningResultRowsDoc, err)
		}

		ok := rows.Next()

		// ok is expected to be false on the last doc.
		if i < len(rawDocs)-1 {
			if !ok {
				return nil, errors.New("got fewer docs from CouchDB than expected")
			}
		} else {
			if ok {
				return nil, errors.New("got more docs from CouchDB than expected")
			}
		}
	}

	return rawDocs, nil
}

type couchDBResultsIterator struct {
	store      *Store
	resultRows *kivik.Rows
}

func (i *couchDBResultsIterator) Tags() ([]newstorage.Tag, error) {
	panic("implement me") // TODO DEREK
}

// Next moves the pointer to the next value in the iterator. It returns false if the iterator is exhausted.
// Note that the Kivik library automatically closes the kivik.Rows iterator if the iterator is exhausted.
func (i *couchDBResultsIterator) Next() (bool, error) {
	nextCallResult := i.resultRows.Next()

	// Kivik only guarantees that this value will be set after all the rows have been iterated through.
	warningMsg := i.resultRows.Warning()

	if warningMsg != "" {
		i.store.log.Warnf(warningMsg)
	}

	err := i.resultRows.Err()
	if err != nil {
		return nextCallResult, fmt.Errorf(failureDuringIterationOfResultRows, err)
	}

	return nextCallResult, nil
}

// Release releases associated resources. Release should always result in success
// and can be called multiple times without causing an error.
func (i *couchDBResultsIterator) Release() error {
	err := i.resultRows.Close()
	if err != nil {
		return fmt.Errorf(failureWhenClosingResultRows, err)
	}

	return nil
}

// Key returns the key of the current key-value pair.
// A nil error likely means that the key list is exhausted.
func (i *couchDBResultsIterator) Key() (string, error) {
	key := i.resultRows.Key()
	if key != "" {
		// The returned key is a raw JSON string. It needs to be unescaped:
		str, err := strconv.Unquote(key)
		if err != nil {
			return "", fmt.Errorf(failureWhileUnquotingKey, err)
		}

		return str, nil
	}

	return "", nil
}

// Value returns the value of the current key-value pair.
func (i *couchDBResultsIterator) Value() ([]byte, error) {
	rawDoc := make(map[string]interface{})

	err := i.resultRows.ScanDoc(&rawDoc)
	if err != nil {
		return nil, fmt.Errorf(failureWhileScanningResultRowsDoc, err)
	}

	value, err := i.store.getStoredValueFromRawDoc(rawDoc)
	if err != nil {
		return nil, fmt.Errorf(failureWhileGettingStoredValueFromRawDoc, err)
	}

	return value, nil
}
