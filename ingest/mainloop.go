package ingest

import (
	"runtime"
	"sync"

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
	runtime.GOMAXPROCS(runtime.NumCPU())

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
