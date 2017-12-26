package ingest

import (
	"runtime"
	"sync"

	"github.com/ngaut/log"
	"golang.org/x/net/context"

	"github.com/pingcap/tidb-lightning/ingest/config"
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
	go m.run()
	m.wg.Wait()
}

func (m *mainloop) run() {
	log.Info("mainloop running ~")
	return
}

func (m *mainloop) Stop() {
	m.shutdown()
	m.wg.Wait()
}
