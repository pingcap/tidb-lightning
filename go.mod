module github.com/pingcap/tidb-lightning

go 1.12

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/DATA-DOG/go-sqlmock v1.3.3
	github.com/coreos/bbolt v1.3.3 // indirect
	github.com/coreos/etcd v3.3.13+incompatible // indirect
	github.com/coreos/go-semver v0.3.0
	github.com/go-sql-driver/mysql v1.4.1
	github.com/gogo/protobuf v1.3.1
	github.com/golang/lint v0.0.0-20180702182130-06c8688daad7 // indirect
	github.com/golang/mock v1.3.1
	github.com/golang/snappy v0.0.1 // indirect
	github.com/joho/sqltocsv v0.0.0-20190824231449-5650f27fd5b6
	github.com/myesui/uuid v1.0.0 // indirect
	github.com/pingcap/check v0.0.0-20191107115940-caf2b9e6ccf4
	github.com/pingcap/errors v0.11.4
	github.com/pingcap/failpoint v0.0.0-20190708053854-e7b1061e6e81
	github.com/pingcap/kvproto v0.0.0-20191202044712-32be31591b03
	github.com/pingcap/log v0.0.0-20191012051959-b742a5d432e9
	github.com/pingcap/parser v0.0.0-20191112053614-3b43b46331d5
	github.com/pingcap/tidb v0.0.0-20191115021711-b274eb2079dc
	github.com/pingcap/tidb-tools v3.0.7-0.20191202034632-451c58d281c7+incompatible
	github.com/prometheus/client_golang v1.1.0
	github.com/prometheus/client_model v0.0.0-20190812154241-14fe0d1b01d4
	github.com/satori/go.uuid v1.2.0
	github.com/shurcooL/httpgzip v0.0.0-20190720172056-320755c1c1b0
	go.uber.org/zap v1.12.0
	golang.org/x/sync v0.0.0-20190911185100-cd5d95a43a6e
	golang.org/x/text v0.3.2
	google.golang.org/grpc v1.25.1
	modernc.org/mathutil v1.0.0
)

// fix build failure on Go 1.13
