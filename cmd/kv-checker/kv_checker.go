package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"

	"github.com/google/uuid"

	"github.com/pingcap/tidb-lightning/lightning/backend"
	"github.com/pingcap/tidb-lightning/lightning/log"
)

func main() {
	if len(os.Args) <= 1 {
		fmt.Printf("No input file.\r\n\t[Usage] ./check input-file-path")
		os.Exit(-1)
		return
	}

	log.InitLogger(&log.Config{
		Level: "info",
		File:  "check.log",
	}, "info")

	path := os.Args[1]
	concurrency := runtime.NumCPU()
	lb, err := backend.NewLocalBackend(context.Background(), nil, "", 96<<20, path, concurrency,
		32, true, nil, 1000000)
	if err != nil {
		fmt.Printf("open backend failed: %v\n", err)
		os.Exit(-1)
	}

	files, err := ioutil.ReadDir(path)
	if err != nil {
		fmt.Print("read dir '%s' failed: %v\n", path, err)
		os.Exit(-1)
	}

	for _, f := range files {
		if f.IsDir() {
			engineUUID, err := uuid.Parse(f.Name())
			if err != nil {
				fmt.Printf("[WARN] invalid engine name '%s', skipped.", f.Name())
				continue
			}
			closedEngine, err := lb.UnsafeCloseEngineWithUUID(context.Background(), "test", engineUUID)
			if err != nil {
				fmt.Printf("failed to close engine '%s'.", engineUUID)
				os.Exit(-1)
			}

			err = closedEngine.Import(context.Background())
			if err != nil {
				fmt.Printf("[Error] import engine '%s' failed, err: %v\n", engineUUID, err)
			}
		}
	}

}
