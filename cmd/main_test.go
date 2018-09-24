package main

// Reference: https://dzone.com/articles/measuring-integration-test-coverage-rate-in-pouchc

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func writePid(pidFile string) error {
	fd, err := unix.Open(pidFile, unix.O_CREAT|unix.O_WRONLY|unix.O_TRUNC|unix.O_CLOEXEC, 0666)
	if err != nil {
		return err
	}
	defer unix.Close(fd)

	err = unix.Flock(fd, unix.LOCK_EX)
	if err != nil {
		return err
	}
	defer unix.Flock(fd, unix.LOCK_UN)

	myPid := fmt.Sprint(os.Getpid())
	unix.Write(fd, []byte(myPid))

	return nil
}

func TestRunMain(t *testing.T) {
	var args []string
	for _, arg := range os.Args {
		switch {
		case arg == "DEVEL":
		case strings.HasPrefix(arg, "-test."):
		default:
			args = append(args, arg)
		}
	}

	if pidFile := os.Getenv("PIDFILE"); len(pidFile) > 0 {
		if err := writePid(pidFile); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to update $PIDFILE(='%s'): %v\n", pidFile, err)
			os.Exit(2)
		}
	}

	waitCh := make(chan struct{}, 1)

	os.Args = args
	go func() {
		main()
		close(waitCh)
	}()

	<-waitCh
}
