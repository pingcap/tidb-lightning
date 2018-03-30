### Makefile for tidb-lightning

GOPATH ?= $(shell go env GOPATH)

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
  $(error Please set the environment variable GOPATH before running `make`)
endif

LIGHTNING_BIN := bin/tidb-lightning

TIDBDIR := $(GOPATH)/src/github.com/pingcap/tidb
path_to_add := $(addsuffix /bin,$(subst :,/bin:,$(GOPATH)))
export PATH := $(path_to_add):$(PATH)

GO        := go
GOBUILD   := CGO_ENABLED=0 $(GO) build $(BUILD_FLAG)
GOTEST    := CGO_ENABLED=1 $(GO) test -p 3

ARCH      := "`uname -s`"
LINUX     := "Linux"
MAC       := "Darwin"

.PHONY: all build parser clean parserlib lightning

default: clean lightning checksuccess

build:
	$(GOBUILD)

clean:
	$(GO) clean -i ./...
	rm -f $(LIGHTNING_BIN)

checksuccess:
	@if [ -f $(LIGHTNING_BIN) ]; \
	then \
		echo "Lightning build successfully :-) !" ; \
	fi

goyacc:
	$(GOBUILD) -o $(TIDBDIR)/bin/goyacc $(TIDBDIR)/parser/goyacc/main.go

parser: goyacc
	$(TIDBDIR)/bin/goyacc -o /dev/null $(TIDBDIR)/parser/parser.y
	$(TIDBDIR)/bin/goyacc -o $(TIDBDIR)/parser/parser.go $(TIDBDIR)/parser/parser.y 2>&1 | egrep "(shift|reduce)/reduce" | awk '{print} END {if (NR > 0) {print "Find conflict in parser.y. Please check y.output for more information."; exit 1;}}'
	rm -f y.output

	@if [ $(ARCH) = $(LINUX) ]; \
	then \
		sed -i -e 's|//line.*||' -e 's/yyEofCode/yyEOFCode/' $(TIDBDIR)/parser/parser.go; \
	elif [ $(ARCH) = $(MAC) ]; \
	then \
		/usr/bin/sed -i "" 's|//line.*||' $(TIDBDIR)/parser/parser.go; \
		/usr/bin/sed -i "" 's/yyEofCode/yyEOFCode/' $(TIDBDIR)/parser/parser.go; \
	fi

	@awk 'BEGIN{print "// Code generated by goyacc"} {print $0}' $(TIDBDIR)/parser/parser.go > tmp_parser.go && mv tmp_parser.go $(TIDBDIR)/parser/parser.go;

parserlib: parser/parser.go

parser/parser.go: $(TIDBDIR)/parser/parser.y
	make parser

RACE_FLAG =
ifeq ("$(WITH_RACE)", "1")
	RACE_FLAG = -race
	GOBUILD   = GOPATH=$(TIDBDIR)/_vendor:$(GOPATH) CGO_ENABLED=1 $(GO) build
endif

lightning: parserlib
	@if [ -d $(TIDBDIR)/vendor/golang.org/x/net/trace ]; then mv $(TIDBDIR)/vendor/golang.org/x/net/trace $(TIDBDIR)/vendor/golang.org/x/net/_trace; fi
	-$(GOBUILD) $(RACE_FLAG) -o $(LIGHTNING_BIN) cmd/main.go
	@if [ -d $(TIDBDIR)/vendor/golang.org/x/net/_trace ]; then mv $(TIDBDIR)/vendor/golang.org/x/net/_trace $(TIDBDIR)/vendor/golang.org/x/net/trace; fi 
