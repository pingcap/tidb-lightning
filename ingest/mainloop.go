package ingest

import (
	"github.com/ngaut/log"

	_ "github.com/pingcap/tidb/util/kvencoder"
	_ "golang.org/x/net/context"
)

func RunMainLoop() {
	cfg := &Config{
		Source: DataSource{
			Type: "mydumper",
			URL:  "/Users/silentsai/mys/mydumper-datas",
		},

		PdAddr:        "172.16.10.2:10101",
		KvDeliverAddr: "172.16.10.2:10309",

		ProgressStore: DBStore{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Psw:      "",
			Database: "tidb_ingest",
		},
	}

	mdl := NewMyDumpLoader(cfg)
	dbMeta := mdl.GetTree()

	restore := NewRestoreControlloer(dbMeta, cfg)
	defer restore.Close()
	restore.wg.Wait()

	log.Info("Mainloop end !")
	return
}
