package backend

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table"
	uuid "github.com/satori/go.uuid"
	"go.uber.org/zap"
	"time"
)

type tikvBackend struct {
	kvClient *tikv.RawKVClient
	tls *common.TLS
}

func NewTiKVBackend(pdAddr string, tls *common.TLS) (Backend, error) {
	client, err := tikv.NewRawKVClient([]string{pdAddr}, config.Security{})
	if err != nil {
		return MakeBackend(nil), err
	}
	return MakeBackend(&tikvBackend{
		kvClient:client,
		tls: tls,
	}), nil
}

func (be *tikvBackend) Close() {
	_ = be.kvClient.Close()
}

func (be *tikvBackend) MakeEmptyRows() Rows {
	return kvPairs(nil)
}

func (be *tikvBackend) RetryImportDelay() time.Duration {
	return 0
}

func (be *tikvBackend) MaxChunkSize() int {
	// 96 MB
	return 96 * 1024 * 1024
}

func (be *tikvBackend) ShouldPostProcess() bool {
	return true
}

func (be *tikvBackend) CheckRequirements() error {
	log.L().Info("skipping check requirements for tikv backend")
	return nil
}

func (be *tikvBackend) NewEncoder(tbl table.Table, options *SessionOptions) Encoder {
	return NewTableKVEncoder(tbl, options)
}

func (be *tikvBackend) OpenEngine(context.Context, uuid.UUID) error {
	return nil
}

func (be *tikvBackend) CloseEngine(context.Context, uuid.UUID) error {
	return nil
}

func (be *tikvBackend) CleanupEngine(context.Context, uuid.UUID) error {
	return nil
}

func (be *tikvBackend) ImportEngine(context.Context, uuid.UUID) error {
	return nil
}

func (be *tikvBackend) WriteRows(ctx context.Context, _ uuid.UUID, tableName string, columnNames []string, ts uint64, r Rows) error {
	start := time.Now()
	kvs := r.(kvPairs)
	if len(kvs) == 0 {
		return nil
	}

	keysDefault := make([][]byte, 0)
	valuesDefault := make([][]byte, 0)
	keysWrite := make([][]byte, 0)
	valuesWrite := make([][]byte, 0)
	for _, pair := range kvs {
		key := encodeKeyWithTs(pair.Key, ts)
		if isShortValue(pair.Val) {
			value := encodeValue(pair.Val, ts)
			keysWrite = append(keysWrite, key)
			valuesWrite = append(valuesWrite, value)
		} else {
			value := encodeValue([]byte{}, ts)
			keysWrite = append(keysWrite, key)
			valuesWrite = append(valuesWrite, value)
			keysDefault = append(keysDefault, key)
			valuesDefault = append(valuesDefault, pair.Val)
		}
	}

	for i, key := range keysWrite {
		log.L().Info("write kv", zap.Binary("key", key), zap.Binary("value", valuesWrite[i]))
	}

	totalBytes := calculateBytes(kvs)
	err := be.kvClient.BatchPutCf(keysWrite, valuesWrite, "write")
	if err != nil {
		return err
	}

	if len(keysDefault) > 0 {
		err = be.kvClient.BatchPutCf(keysDefault, valuesDefault, "default")
		if err != nil {
			return err
		}
	}

	log.L().Debug(fmt.Sprintf("write rows finish, row count: %d, bytes: %d, duration: %v", len(kvs), totalBytes, time.Now().Sub(start)))

	return err
}

func encodeKeyWithTs(key []byte, ts uint64) []byte {
	keyLen := len(key)
	index := 0
	res := make([]byte, 0, maxEncodedBytesSize(len(key)))
	padBytes := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	for ; index < keyLen; {
		remain := keyLen - index
		pad := 0
		if remain > 8 {
			res = append(res, key[index:index+8]...)
		} else {
			pad = 8 - remain
			res = append(res, key[index:]...)
			res = append(res, padBytes[:pad]...)
		}
		res = append(res, byte(255 - pad))
		index += 8
	}
	keyLen = len(res)
	res = append(res, byte(0), byte(0), byte(0), byte(0), byte(0), byte(0), byte(0), byte(0))
	binary.BigEndian.PutUint64(res[keyLen:], ^ts)
	return res
}

func maxEncodedBytesSize(n int) int {
	return (n / 8 + 1) * (8 + 1)
}

func encodeValue(value []byte, ts uint64) []byte {
	buf := make([]byte, 9, 1 + 10 + len(value) + 2)
	buf[0] = 'P'
	length := binary.PutUvarint(buf[1:], ts)
	buf = buf[:length+1]
	if len(value) > 0 {
		buf = append(buf, 'v', uint8(len(value)))
		buf = append(buf, value...)
	}
	return buf
}

func calculateBytes(kvs kvPairs) int {
	total := 0
	for _, kv := range kvs {
		total += len(kv.Key) + len(kv.Val)
	}
	return total
}

func isShortValue(value []byte) bool {
	return len(value) <= 255
}

func (be *tikvBackend) FetchRemoteTableModels(schemaName string) ([]*model.TableInfo, error) {
	var tables []*model.TableInfo
	err := be.tls.GetJSON("/schema/"+schemaName, &tables)
	if err != nil {
		return nil, errors.Annotatef(err, "cannot read schema '%s' from remote", schemaName)
	}
	return tables, nil
}