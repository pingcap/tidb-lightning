package ingest

import (
	"runtime"
	"sync"

	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	"github.com/pingcap/tidb-lightning/ingest/config"
	"github.com/pingcap/tidb-lightning/ingest/mydump"
	"github.com/pingcap/tidb-lightning/ingest/restore"
)

type mainloop struct {
	cfg      *config.Config
	ctx      context.Context
	shutdown context.CancelFunc

	wg sync.WaitGroup
}

func Mainloop(cfg *config.Config) *mainloop {
	ctx, shutdown := context.WithCancel(context.Background())

	return &mainloop{
		cfg:      cfg,
		ctx:      ctx,
		shutdown: shutdown,
	}
}

func (m *mainloop) Run() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.run()
	}()
	m.wg.Wait()
}

func (m *mainloop) run() {
	mdl, err := mydump.NewMyDumpLoader(m.cfg)
	if err != nil {
		log.Errorf("failed to load mydumper source : %s", err.Error())
		return
	}

	dbMeta := mdl.GetDatabase()
	procedure := restore.NewRestoreControlloer(dbMeta, m.cfg)
	defer procedure.Close()

	procedure.Run(m.ctx)
	return
}

func (m *mainloop) Stop() {
	m.shutdown()
	m.wg.Wait()
}
