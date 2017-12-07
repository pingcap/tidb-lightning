package ingest

import (
	"fmt"
	_ "path/filepath"

	"github.com/juju/errors"
	"github.com/ngaut/log"
)

const (
	StagePending string = "pending"
	StageLoaded         = "loaded"
	StageFlushed        = "flushed"
)

/*
	type Mission struct {
		ID int64
	}

	type KvDeliverSession struct {
		// missionID
		Server string // kv deliver backend address ~
		UUID   string
		DB     string
		Table  string
	}
*/

/************************************************
* 	Table's progress
*************************************************/

type TableProgress struct {
	DB    string
	Table string

	FilesPrgoress map[string]*TableFileProgress // map[name] = file_progress

	dbms *ProgressDBMS
}

func NewTableProgress(database string, table string, partitions map[string]int64) *TableProgress {
	tpg := &TableProgress{
		DB:    database,
		Table: table,
	}

	return tpg.init(partitions)
}

func (tpg *TableProgress) init(partitions map[string]int64) *TableProgress {
	tpg.FilesPrgoress = make(map[string]*TableFileProgress)
	for name, scope := range partitions {
		tpg.FilesPrgoress[name] =
			NewTableFileProgress(tpg.DB, tpg.Table, name, scope)
	}

	return tpg
}

func (tpg *TableProgress) Calc() map[string]float64 {
	var tolSize int64
	for _, pg := range tpg.FilesPrgoress {
		tolSize += pg.scope
	}

	summary := make(map[string]float64)
	for _, pg := range tpg.FilesPrgoress {
		scale := float64(pg.scope) / float64(tolSize)
		subs := pg.Calc()
		for stage, progress := range subs {
			summary[stage] += progress * scale
		}
	}

	return summary
}

func (tpg *TableProgress) SetComplete() {
	for _, fpg := range tpg.FilesPrgoress {
		fpg.SetComplete()
	}
}

func (tpg *TableProgress) showSummaryProgress() {
	stagesProg := tpg.Calc()
	precent := func(v float64) string {
		return fmt.Sprintf("%.2f %%", v*float64(100))
	}

	fmt.Println("stages ========>")
	fmt.Printf("- flushed : %s\n", precent(stagesProg[StageFlushed]))
	fmt.Printf("- loaded  : %s\n", precent(stagesProg[StageLoaded]))
	fmt.Printf("- pending : %s\n", precent(stagesProg[StagePending]))
}

/************************************************
*	Table's file progress
*************************************************/

/*
	File region progress (eg.) :


				|___________________________File____________________________|
	offset =>	0				 256		  300						  1024

	row id =>	0				 10			   15						   xxx

				|				  |			   |							|
	region-1 :	|---- Flushed ----|			   |							|
				|				  |			   |							|
	region-2 :	|				  |-- Loaded --|							|
				|							   |							|
	region-3 :	|							   |--------- Pending ----------|

*/

type TableFileProgress struct {
	DB    string
	Table string
	Name  string

	scope   int64
	regions map[string]*TableRegion // Map[stage] = region_progress

	dbms *ProgressDBMS
}

func NewTableFileProgress(database string, table string, name string, scope int64) *TableFileProgress {
	fpg := &TableFileProgress{
		DB:    database,
		Table: table,
		Name:  name,
		scope: scope,
	}
	fpg.initRegions()
	return fpg
}

func (fpg *TableFileProgress) initRegions() {
	stages := []string{StagePending, StageLoaded, StageFlushed}
	fpg.regions = make(map[string]*TableRegion)
	for _, stage := range stages {
		// TODO : replace name by file_name rather than file path ~
		fpg.regions[stage] = NewTableRegion(fpg.DB, fpg.Table, fpg.Name)
	}

	// ps : whole range of file be in pending stage
	pendingRegion, _ := fpg.regions[StagePending]
	pendingRegion.MaxRowID = 0
	pendingRegion.StartOffset = 0
	pendingRegion.EndOffset = fpg.scope

	return
}

func (fpg *TableFileProgress) addRegion(region *TableRegion, stage string) {
	fpg.regions[stage] = region

	var scope int64 = 0
	for _, rg := range fpg.regions {
		if rg.EndOffset > scope {
			scope = rg.EndOffset
		}
	}

	fpg.scope = scope
}

func (fpg *TableFileProgress) SetComplete() {
	_, maxRowID := fpg.Locate(StageLoaded)
	fpg.Update(StageFlushed, fpg.scope, maxRowID)
}

func (fpg *TableFileProgress) Update(stage string, offset int64, maxRowID int64) error {
	updateRegion, ok := fpg.regions[stage]
	if !ok {
		return errors.Errorf("progress stage (%s) invalid !", stage)
	}

	if offset < 0 || offset > fpg.scope {
		return errors.Errorf("scope overflow (%d > %d or %d < 0)", offset, fpg.scope, offset)
	}

	if offset < updateRegion.EndOffset {
		return errors.Errorf("operation not allowed ! allow forward offset only (%d < %d) ",
			offset, updateRegion.EndOffset)
	}

	if offset == updateRegion.EndOffset {
		updateRegion.MaxRowID = maxRowID
		return nil
	}

	for st, region := range fpg.regions {
		if st == stage {
			continue
		}

		// ps : adjust region scope, overlap not allowed across region by region
		if offset > region.StartOffset {
			if region.EndOffset <= updateRegion.StartOffset {
				continue
			}
			region.StartOffset = offset
			if offset >= region.EndOffset {
				region.EndOffset = offset
			}
		}
	}
	updateRegion.EndOffset = offset
	updateRegion.MaxRowID = maxRowID

	if fpg.dbms != nil {
		fpg.dbms.updateFileProgress(fpg)
	}

	return nil
}

func (fpg *TableFileProgress) Locate(stage string) (int64, int64) {
	region, ok := fpg.regions[stage]
	if !ok {
		log.Warnf("[%s] region with stage = %s not found !", fpg.Name, stage)
		return -1, -1
	}

	return region.EndOffset, region.MaxRowID
}

func (fpg *TableFileProgress) dump() {
	// PS : only for debug
	lr := fpg.regions[StageLoaded]
	fr := fpg.regions[StageFlushed]
	pr := fpg.regions[StagePending]

	fmt.Printf("regions scope ========> [%s | %s | %s]\n", fr.DB, fr.Table, fr.Name)
	fmt.Printf("- flushed : [%d , %d]\n", fr.StartOffset, fr.EndOffset)
	fmt.Printf("- loaded  : [%d , %d]\n", lr.StartOffset, lr.EndOffset)
	fmt.Printf("- pending : [%d , %d]\n", pr.StartOffset, pr.EndOffset)
}

func (fpg *TableFileProgress) Finished() bool {
	flushedRegion, ok := fpg.regions[StageFlushed]
	if !ok {
		log.Errorf("flushed stage region not found !")
		return false
	}

	return flushedRegion.Length() >= fpg.scope
}

func (fpg *TableFileProgress) Calc() map[string]float64 {
	stagesProgress := make(map[string]float64)
	for stage, region := range fpg.regions {
		stagesProgress[stage] = float64(region.Length()) / float64(fpg.scope)
	}
	return stagesProgress
}

/************************************************
*	Region of table's file
*************************************************/

type TableRegion struct {
	DB          string
	Table       string
	Name        string // file name
	StartOffset int64
	EndOffset   int64
	MaxRowID    int64
}

func NewTableRegion(database string, table string, name string) *TableRegion {
	return &TableRegion{
		DB:          database,
		Table:       table,
		Name:        name,
		StartOffset: 0,
		EndOffset:   0, // ps : open interval -- actual region range exclude this posistion
		MaxRowID:    -1,
	}
}

func (tr *TableRegion) Length() int64 {
	return tr.EndOffset - tr.StartOffset
}

func (tr *TableRegion) Cover(pos int64) bool {
	return pos >= tr.StartOffset && pos < tr.EndOffset
}

func (tr *TableRegion) Expand(endOffset int64, maxRowID int64) {
	tr.EndOffset = endOffset
	tr.MaxRowID = maxRowID
}
