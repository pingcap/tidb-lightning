module github.com/pingcap/tidb-lightning

go 1.12

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/DATA-DOG/go-sqlmock v1.3.3
	github.com/coreos/go-semver v0.3.0
	github.com/go-sql-driver/mysql v1.4.1
	github.com/gogo/protobuf v1.3.1
	github.com/golang/mock v1.3.1
	github.com/joho/sqltocsv v0.0.0-20190824231449-5650f27fd5b6
	github.com/pingcap/check v0.0.0-20190102082844-67f458068fc8
	github.com/pingcap/errors v0.11.4
	github.com/pingcap/failpoint v0.0.0-20190708053854-e7b1061e6e81
	github.com/pingcap/kvproto v0.0.0-20191011042334-8ee4fd8fb4ca
	github.com/pingcap/log v0.0.0-20190307075452-bd41d9273596
	github.com/pingcap/parser v0.0.0-20190910041007-2a177b291004
	github.com/pingcap/tidb v3.0.4+incompatible
	github.com/pingcap/tidb-tools v3.0.4+incompatible
	github.com/prometheus/client_golang v1.1.0
	github.com/prometheus/client_model v0.0.0-20190129233127-fd36f4220a90
	github.com/satori/go.uuid v1.2.0
	github.com/shurcooL/httpgzip v0.0.0-20190720172056-320755c1c1b0
	go.uber.org/zap v1.10.0
	golang.org/x/sync v0.0.0-20190911185100-cd5d95a43a6e
	golang.org/x/text v0.3.2
	google.golang.org/grpc v1.21.4
	gopkg.in/stretchr/testify.v1 v1.4.0 // indirect
	modernc.org/mathutil v1.0.0
)

// fix build failure on Go 1.13
replace gopkg.in/stretchr/testify.v1 v1.4.0 => github.com/stretchr/testify v1.4.0
