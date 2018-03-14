package kv_test

import (
	"fmt"
	"testing"

	"golang.org/x/net/context"

	. "github.com/pingcap/tidb-lightning/ingest/kv"
	. "github.com/pingcap/tidb/util/kvencoder"
)

const (
	uuid    string = "0123456789abcdef"
	backend string = "172.16.10.2:18309"
	pdAddr  string = "172.16.10.2:18101"
)

func TestWriteFlush(t *testing.T) {
	ctx := context.Background()

	c, _ := NewKVDeliverClient(ctx, uuid, backend)
	defer c.Close()

	kvs := make([]KvPair, 0, 0)
	for i := 0; i < 10; i++ {
		kvs = append(kvs, KvPair{
			Key: []byte(fmt.Sprintf("k-%d", i)),
			Val: []byte(fmt.Sprintf("v-%d", i)),
		})
	}

	if err := c.Put(kvs); err != nil {
		panic(err.Error())
	}

	if err := c.Cleanup(); err != nil {
		panic(err.Error())
	}

	fmt.Println("basic finish !")
}

func TestManager(t *testing.T) {
	p, _ := NewPipeKvDeliver(uuid, backend)
	defer p.Close()

	datas := make([][]KvPair, 0)
	for i := 0; i < 20; i++ {
		kvs := make([]KvPair, 0)
		for j := 0; j < 10; j++ {
			kvs = append(kvs, KvPair{
				Key: []byte(fmt.Sprintf("cqc-key-%d-%d", i, j)),
				Val: []byte(fmt.Sprintf("cqc-val-%d-%d", i, j)),
			})
		}
		datas = append(datas, kvs)
	}

	for _, kvs := range datas {
		p.Put(kvs)
	}

	p.Cleanup()
	// p.Flush()

	p.CloseAndWait()

	fmt.Println("manager finish !")
}
