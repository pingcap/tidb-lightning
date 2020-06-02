module github.com/pingcap/tidb-lightning

go 1.13

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/DATA-DOG/go-sqlmock v1.4.1
	github.com/cockroachdb/pebble v0.0.0-20200601233547-7956a7440a70
	github.com/coreos/go-semver v0.3.0
	github.com/dgraph-io/ristretto v0.0.2-0.20200115201040-8f368f2f2ab3 // indirect
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gogo/protobuf v1.3.1
	github.com/golang/mock v1.4.3
	github.com/joho/sqltocsv v0.0.0-20190824231449-5650f27fd5b6
	github.com/pingcap/br v0.0.0-20200413082646-b37ae8f0b494
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20190809092503-95897b64e011
	github.com/pingcap/failpoint v0.0.0-20200210140405-f8f9fb234798
	github.com/pingcap/kvproto v0.0.0-20200428101946-c3b73d03dde2
	github.com/pingcap/log v0.0.0-20200117041106-d28c14d3b1cd
	github.com/pingcap/parser v0.0.0-20200331080149-8dce7a46a199
	github.com/pingcap/pd/v4 v4.0.0-beta.1.0.20200305072537-61d9f9cc35d3
	github.com/pingcap/tidb v1.1.0-beta.0.20200402143739-827edde58535
	github.com/pingcap/tidb-tools v4.0.0-beta.1.0.20200306084441-875bd09aa3d5+incompatible
	github.com/prometheus/client_golang v1.5.1
	github.com/prometheus/client_model v0.2.0
	github.com/satori/go.uuid v1.2.0
	github.com/shurcooL/httpgzip v0.0.0-20190720172056-320755c1c1b0
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	go.uber.org/zap v1.14.1
	golang.org/x/sync v0.0.0-20200317015054-43a5402ce75a
	golang.org/x/text v0.3.2
	google.golang.org/grpc v1.25.1
	modernc.org/mathutil v1.0.0
)
