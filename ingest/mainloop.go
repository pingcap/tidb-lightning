package ingest

import (
	"sync"

	"github.com/ngaut/log"
	"golang.org/x/net/context"

	_ "github.com/pingcap/tidb/util/kvencoder"
	_ "golang.org/x/net/context"
)

type mainloop struct {
	cfg      *Config
	ctx      context.Context
	shutdown context.CancelFunc

	wg sync.WaitGroup
}

func Mainloop(cfg *Config) *mainloop {
	ctx, shutdown := context.WithCancel(context.Background())

	return &mainloop{
		cfg:      cfg,
		ctx:      ctx,
		shutdown: shutdown,
	}
}

func (m *mainloop) Run() {
	m.wg.Add(1)
	go m.run()
	m.wg.Wait()
}

func (m *mainloop) run() {
	dbMeta := NewMyDumpLoader(m.cfg).GetTree()
	restore := NewRestoreControlloer(dbMeta, m.cfg)
	defer func() {
		restore.Close()
		m.wg.Done()
	}()

	restore.Run(m.ctx)
	return
}

func (m *mainloop) Stop() {
	m.shutdown()
	m.wg.Wait()
}

///////////////////////////////////////////////////////////////

func RunMainLoop() {
	cfg := GetCQCConfig()
	mdl := NewMyDumpLoader(cfg)
	dbMeta := mdl.GetTree()

	restore := NewRestoreControlloer(dbMeta, cfg)
	defer restore.Close()
	restore.wg.Wait()

	log.Info("Mainloop end !")
	return
}

/*
	cfg := &Config{
		Source: DataSource{
			Type: "mydumper",
			URL:  "/home/pingcap/cenqichao/big-sysbench-datas",
		},

		PdAddr:        "172.16.10.2:10101",
		KvDeliverAddr: "172.16.10.2:10309",

		ProgressStore: DBStore{
			Host:     "172.16.10.2",
			Port:     10201,
			User:     "root",
			Psw:      "",
			Database: "tidb_ingest",
		},
	}
*/
