package base

import (
	"fmt"
	"io"

	"github.com/ngaut/log"
)

const (
	TypeCSV      = "csv"
	TypeMydumper = "mydumper"
)

// Payload has either SQL or Params and they are mutually exclusive
type Payload struct {
	SQL    string
	Params []interface{}
}

type TableRegion struct {
	ID int

	DB    string
	Table string
	File  string

	Offset     int64
	Size       int64
	SourceType string
	Rows       int64
}

func (reg *TableRegion) String() string {
	return fmt.Sprintf("file:%s id:%d db:%s table:%s offset:%d size:%d rows:%d ",
		reg.File, reg.ID, reg.DB, reg.Table, reg.Offset, reg.Size, reg.Rows)
}

type RegionSlice []*TableRegion

func (rs RegionSlice) Len() int {
	return len(rs)
}
func (rs RegionSlice) Swap(i, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}
func (rs RegionSlice) Less(i, j int) bool {
	if rs[i].File == rs[j].File {
		return rs[i].Offset < rs[j].Offset
	}
	return rs[i].File < rs[j].File
}

func CurrOffset(seeker io.Seeker) int64 {
	off, err := seeker.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Errorf("get file offset failed (%s) : %v", err)
		return -1
	}
	return off
}
