module github.com/pingcap/tidb-lightning

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/apache/thrift v0.0.0-20161221203622-b2a4d4ae21c7 // indirect
	github.com/coreos/go-semver v0.2.0
	github.com/cznic/golex v0.0.0-20160422121650-da5a7153a510 // indirect
	github.com/cznic/mathutil v0.0.0-20181021201202-eba54fb065b7
	github.com/cznic/parser v0.0.0-20160622100904-31edd927e5b1
	github.com/cznic/sortutil v0.0.0-20150617083342-4c7342852e65
	github.com/cznic/strutil v0.0.0-20150430124730-1eb03e3cc9d3
	github.com/cznic/y v0.0.0-20160420101755-9fdf92d4aac0
	github.com/go-sql-driver/mysql v1.4.0
	github.com/gogo/protobuf v1.1.1
	github.com/joho/sqltocsv v0.0.0-20180904231936-b24deec2b806
	github.com/pingcap/check v0.0.0-20171206051426-1c287c953996
	github.com/pingcap/gofail v0.0.0-20181121072748-c3f835e5a7d8
	github.com/pingcap/kvproto v0.0.0-20181105061835-1b5d69cd1d26
	github.com/pingcap/parser v0.0.0-20181113072426-4a9a1b13b591
	github.com/pingcap/tidb v0.0.0-20181120082053-012cb6da9443
	github.com/pingcap/tidb-enterprise-tools v1.0.1-0.20181116033341-5832f7307d74
	github.com/pkg/errors v0.8.0
	github.com/prometheus/client_golang v0.9.0
	github.com/prometheus/client_model v0.0.0-20180712105110-5c3871d89910
	github.com/satori/go.uuid v1.2.0
	github.com/siddontang/go v0.0.0-20170517070808-cb568a3e5cc0 // indirect
	github.com/sirupsen/logrus v1.2.0
	golang.org/x/text v0.3.0
	google.golang.org/appengine v1.1.1-0.20180731164958-4216e58b9158 // indirect
	google.golang.org/grpc v1.16.0
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

replace github.com/pkg/errors v0.8.0 => github.com/pingcap/errors v0.10.1
