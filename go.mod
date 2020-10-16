module github.com/pingcap/tidb-lightning

go 1.13

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/DATA-DOG/go-sqlmock v1.4.1
	github.com/carlmjohnson/flagext v0.0.11
	github.com/cockroachdb/pebble v0.0.0-20201007144542-b79d619f4761
	github.com/coreos/go-semver v0.3.0
	github.com/dgraph-io/ristretto v0.0.2-0.20200115201040-8f368f2f2ab3 // indirect
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gogo/protobuf v1.3.1
	github.com/golang/mock v1.4.4
	github.com/google/btree v1.0.0
	github.com/google/go-cmp v0.5.0 // indirect
	github.com/joho/sqltocsv v0.0.0-20190824231449-5650f27fd5b6
	github.com/juju/loggo v0.0.0-20180524022052-584905176618 // indirect
	github.com/onsi/ginkgo v1.13.0 // indirect
	github.com/pingcap/br v0.0.0-20201009140310-ed2b14378e3f
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20200917111840-a15ef68f753d
	github.com/pingcap/failpoint v0.0.0-20200702092429-9f69995143ce
	github.com/pingcap/kvproto v0.0.0-20200927025644-73dc27044686
	github.com/pingcap/log v0.0.0-20200828042413-fce0951f1463
	github.com/pingcap/parser v0.0.0-20200921041333-cd2542b7a8a2
	github.com/pingcap/tidb v1.1.0-beta.0.20200921082409-501466fb690d
	github.com/pingcap/tidb-tools v4.0.5-0.20200820092506-34ea90c93237+incompatible
	github.com/prometheus/client_golang v1.5.1
	github.com/prometheus/client_model v0.2.0
	github.com/satori/go.uuid v1.2.0
	github.com/shurcooL/httpgzip v0.0.0-20190720172056-320755c1c1b0
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/tikv/pd v1.1.0-beta.0.20200910042021-254d1345be09
	github.com/xitongsys/parquet-go v1.5.4-0.20201010004835-f51647f24120
	github.com/xitongsys/parquet-go-source v0.0.0-20200817004010-026bad9b25d0
	go.uber.org/zap v1.16.0
	golang.org/x/net v0.0.0-20200904194848-62affa334b73
	golang.org/x/sync v0.0.0-20200625203802-6e8e738ad208
	golang.org/x/text v0.3.3
	google.golang.org/grpc v1.27.1
	gopkg.in/mgo.v2 v2.0.0-20180705113604-9856a29383ce // indirect
	modernc.org/mathutil v1.0.0
)

replace github.com/cockroachdb/pebble => github.com/glorv/pebble v0.0.0-20201016054324-9e202fb8089f
