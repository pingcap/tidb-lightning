module github.com/pingcap/tidb-lightning

go 1.13

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/DATA-DOG/go-sqlmock v1.4.1
	github.com/carlmjohnson/flagext v0.0.11
	github.com/cockroachdb/pebble v0.0.0-20200617141519-3b241b76ed3b
	github.com/coreos/go-semver v0.3.0
	github.com/dgraph-io/ristretto v0.0.2-0.20200115201040-8f368f2f2ab3 // indirect
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gogo/protobuf v1.3.1
	github.com/golang/mock v1.4.3
	github.com/joho/sqltocsv v0.0.0-20190824231449-5650f27fd5b6
	github.com/juju/loggo v0.0.0-20180524022052-584905176618 // indirect
	github.com/onsi/ginkgo v1.13.0 // indirect
	github.com/pingcap/br v0.0.0-20200617120402-56e151ad8b67
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20190809092503-95897b64e011
	github.com/pingcap/failpoint v0.0.0-20200603062251-b230c36c413c
	github.com/pingcap/kvproto v0.0.0-20200706115936-1e0910aabe6c
	github.com/pingcap/log v0.0.0-20200511115504-543df19646ad
	github.com/pingcap/parser v0.0.0-20200623082809-b74301ac298b
	github.com/pingcap/pd/v4 v4.0.0-rc.2.0.20200714122454-1a64f969cb3c
	github.com/pingcap/tidb v1.1.0-beta.0.20200721005019-f5c6e59f0daf
	github.com/pingcap/tidb-tools v4.0.1+incompatible
	github.com/pingcap/tipb v0.0.0-20200615034523-dcfcea0b5965
	github.com/prometheus/client_golang v1.5.1
	github.com/prometheus/client_model v0.2.0
	github.com/satori/go.uuid v1.2.0
	github.com/shurcooL/httpgzip v0.0.0-20190720172056-320755c1c1b0
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5
	go.uber.org/zap v1.15.0
	golang.org/x/net v0.0.0-20200602114024-627f9648deb9
	golang.org/x/sync v0.0.0-20200317015054-43a5402ce75a
	golang.org/x/sys v0.0.0-20200615200032-f1bc736245b1 // indirect
	golang.org/x/text v0.3.3
	google.golang.org/grpc v1.26.0
	gopkg.in/mgo.v2 v2.0.0-20180705113604-9856a29383ce // indirect
	modernc.org/mathutil v1.0.0
)

replace github.com/pingcap/br => github.com/glorv/br v0.0.0-20200729030121-568b53aea3d0
