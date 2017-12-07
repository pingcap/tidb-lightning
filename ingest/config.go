package ingest

type DataSource struct {
	Type string
	URL  string
}

type DBStore struct {
	Host     string
	Port     int
	User     string
	Psw      string
	Database string
}

type Config struct {
	Source        DataSource
	PdAddr        string
	KvDeliverAddr string
	ProgressStore DBStore
}
