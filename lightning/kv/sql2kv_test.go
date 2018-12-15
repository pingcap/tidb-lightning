package kv

import (
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb/util/kvencoder"
	"github.com/pingcap/tidb/util/logutil"
	"io/ioutil"
	"testing"
)

func BenchmarkTableKVEncoder_SQL2KV(b *testing.B) {
	logConfig := logutil.LogConfig{
		Level: "panic",
		File: logutil.FileLogConfig{
			Filename: "/dev/null",
		},
	}
	err := logutil.InitLogger(&logConfig)
	if err != nil {
		b.Fatalf("logutil.InitLogger: %v", err)
	}

	encoder, err := NewTableKVEncoder("schr", "s0", 1, 22, config.NewConfig().TiDB.SQLMode, kvenc.NewAllocator())
	if err != nil {
		b.Fatal()
	}
	creatTable, err := ioutil.ReadFile("testdata/schr.s0-schema.sql")
	if err != nil {
		b.Fatal()
	}

	err = encoder.encoder.ExecDDLSQL(string(creatTable))
	if err != nil {
		b.Fatalf("ExecDDLSQL: %v", err)
	}

	sqlContent, err := ioutil.ReadFile("testdata/schr.s0.1.sql")
	if err != nil {
		b.Fatal()
	}

	sql := string(sqlContent)
	kvs, effectedRows, err := encoder.SQL2KV(sql)
	if err != nil {
		b.Fatalf("encoder.SQL2KV: %v", err)
	}
	var bytes int
	for _, kv := range kvs {
		bytes += len(kv.Key)
		bytes += len(kv.Val)
	}
	b.Logf("Kvs: %d, effectedRows: %d, bytes: %d", len(kvs), effectedRows, bytes)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, err := encoder.SQL2KV(sql)
		if err != nil {
			b.Fatalf("SQL2KV: %v", err)
		}
	}
}
