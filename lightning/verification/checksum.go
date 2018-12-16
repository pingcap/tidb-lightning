package verification

import (
	"hash/crc64"

	kvec "github.com/pingcap/tidb/util/kvencoder"
)

var ecmaTable = crc64.MakeTable(crc64.ECMA)

type KVChecksum struct {
	bytes    uint64
	kvs      uint64
	checksum uint64
}

func NewKVChecksum(checksum uint64) *KVChecksum {
	return &KVChecksum{
		checksum: checksum,
	}
}

func MakeKVChecksum(bytes uint64, kvs uint64, checksum uint64) KVChecksum {
	return KVChecksum{
		bytes:    bytes,
		kvs:      kvs,
		checksum: checksum,
	}
}

func (c *KVChecksum) Update(kvs []kvec.KvPair) {
	var (
		checksum uint64
		sum      uint64
		kvNum    int
		bytes    int
	)

	for _, pair := range kvs {
		sum = crc64.Update(0, ecmaTable, pair.Key)
		sum = crc64.Update(sum, ecmaTable, pair.Val)
		checksum ^= sum
		kvNum++
		bytes += (len(pair.Key) + len(pair.Val))
	}

	c.bytes += uint64(bytes)
	c.kvs += uint64(kvNum)
	c.checksum ^= checksum
}

func (c *KVChecksum) Add(other *KVChecksum) {
	c.bytes += other.bytes
	c.kvs += other.kvs
	c.checksum ^= other.checksum
}

func (c *KVChecksum) Sum() uint64 {
	return c.checksum
}

func (c *KVChecksum) SumSize() uint64 {
	return c.bytes
}

func (c *KVChecksum) SumKVS() uint64 {
	return c.kvs
}
