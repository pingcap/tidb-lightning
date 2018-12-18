package kv

import (
	"fmt"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb/util/kvencoder"
	"github.com/pingcap/tidb/util/logutil"
	"io/ioutil"
	"testing"
)

func benchmarkTable(b *testing.B, table string, column int) {
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

	encoder, err := NewTableKVEncoder("schr", table, 1, column, config.NewConfig().TiDB.SQLMode, kvenc.NewAllocator())
	if err != nil {
		b.Fatal()
	}
	creatTable, err := ioutil.ReadFile(fmt.Sprintf("testdata/schr.%s-schema.sql", table))
	if err != nil {
		b.Fatal()
	}

	err = encoder.encoder.ExecDDLSQL(string(creatTable))
	if err != nil {
		b.Fatalf("ExecDDLSQL: %v", err)
	}

	sqlContent, err := ioutil.ReadFile(fmt.Sprintf("testdata/schr.%s.1.sql", table))
	if err != nil {
		b.Fatal()
	}

	sql := string(sqlContent)
	kvs, affectRows, err := encoder.SQL2KV(sql)
	if err != nil {
		b.Fatalf("encoder.SQL2KV: %v", err)
	}
	var bytes int
	for _, kv := range kvs {
		bytes += len(kv.Key)
		bytes += len(kv.Val)
	}
	b.Logf("Table: %s, column: %d, kvs: %d, affectRows: %d, bytes: %d", table, column, len(kvs), affectRows, bytes)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, err := encoder.SQL2KV(sql)
		if err != nil {
			b.Fatalf("SQL2KV: %v", err)
		}
	}
}

func BenchmarkTableKVEncoder_SQL2KV(b *testing.B) {
	benchmarkTable(b, "s0", 22)
}

func BenchmarkTableKVEncoder_SQL2KV2(b *testing.B) {
	benchmarkTable(b, "s11", 55)
}
