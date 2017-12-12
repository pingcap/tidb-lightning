package ingest

import (
	"database/sql"
	"fmt"
	"testing"
)

func assert(a float64, b float64) {
	if (a > b && a-b > 0.000001) || (a < b && b-a > 0.000001) {
		panic(fmt.Sprintf("failed : %.3f vs %.3f", a, b))
	}
}

func assertInt(a int, b int) {
	assertInt64(int64(a), int64(b))
}

func assertInt64(a int64, b int64) {
	if a != b {
		panic(fmt.Sprintf("failed : %d <> %d", a, b))
	}
}

func clearDBStore(db *sql.DB, database string) {
	db.Exec("DROP database " + database)
}

func TestUpdateDBStore(t *testing.T) {
	database := "NBA"
	tables := []string{"A", "B"}

	db := ConnectDB("localhost", 3306, "root", "")
	dbms := NewProgressDBMS(db, database)
	defer clearDBStore(db, database)

	// init
	{
		aParts := map[string]int64{
			"A-1": 100,
			"A-2": 200,
		}
		aProgress := NewTableProgress(database, "A", aParts)

		aProgress.FilesPrgoress["A-1"].Update(StageLoaded, 100, 2)
		aProgress.FilesPrgoress["A-1"].Update(StageFlushed, 50, 1)
		aProgress.FilesPrgoress["A-2"].Update(StageLoaded, 100, 3)

		bParts := map[string]int64{
			"B-1": 100,
			"B-2": 200,
			"B-3": 300,
		}
		bProgress := NewTableProgress(database, "B", bParts)

		bProgress.FilesPrgoress["B-1"].Update(StageLoaded, 100, 1)
		bProgress.FilesPrgoress["B-1"].Update(StageFlushed, 100, 1)
		bProgress.FilesPrgoress["B-2"].Update(StageLoaded, 150, 2)

		dbms.Setup(map[string]*TableProgress{
			"A": aProgress,
			"B": bProgress})
	}

	{
		// recover
		tablesProgress := dbms.LoadAllProgress(database, tables)

		// A
		aProgress := tablesProgress["A"]
		assertInt64(
			aProgress.FilesPrgoress["A-1"].regions[StageLoaded].Length(), 50)
		assertInt64(
			aProgress.FilesPrgoress["A-1"].regions[StageFlushed].Length(), 50)
		assertInt64(
			aProgress.FilesPrgoress["A-1"].regions[StagePending].Length(), 0)

		assertInt64(
			aProgress.FilesPrgoress["A-2"].regions[StageLoaded].Length(), 100)
		assertInt64(
			aProgress.FilesPrgoress["A-2"].regions[StageFlushed].Length(), 0)
		assertInt64(
			aProgress.FilesPrgoress["A-2"].regions[StagePending].Length(), 100)

		// B
		bProgress := tablesProgress["B"]
		b1Off, b1RowID := bProgress.FilesPrgoress["B-1"].Locate(StageLoaded)
		assertInt64(b1Off, 100)
		assertInt64(b1RowID, 1)

		b1Off, b1RowID = bProgress.FilesPrgoress["B-1"].Locate(StageFlushed)
		assertInt64(b1Off, 100)
		assertInt64(b1RowID, 1)

		b2Off, b2RowID := bProgress.FilesPrgoress["B-2"].Locate(StageLoaded)
		assertInt64(b2Off, 150)
		assertInt64(b2RowID, 2)

		b3Off, b3RowID := bProgress.FilesPrgoress["B-3"].Locate(StageLoaded)
		assertInt64(b3Off, 0)
		assertInt64(b3RowID, -1)

		// update
		aProgress.FilesPrgoress["A-1"].Update(StageFlushed, 100, 2)
		aProgress.FilesPrgoress["A-2"].Update(StageLoaded, 200, 4)
		aProgress.FilesPrgoress["A-2"].Update(StageFlushed, 100, 3)

		bProgress.FilesPrgoress["B-3"].Update(StageLoaded, 300, 99)
		bProgress.FilesPrgoress["B-3"].Update(StageFlushed, 300, 99)
	}

	// recover again
	{
		tablesProgress := dbms.LoadAllProgress(database, tables)
		aProgress, bProgress := tablesProgress["A"], tablesProgress["B"]

		// A
		a1Off, a1RowID := aProgress.FilesPrgoress["A-1"].Locate(StageFlushed)
		assertInt64(a1Off, 100)
		assertInt64(a1RowID, 2)

		a2Off, a2RowID := aProgress.FilesPrgoress["A-2"].Locate(StageLoaded)
		assertInt64(a2Off, 200)
		assertInt64(a2RowID, 4)

		a2Off, a2RowID = aProgress.FilesPrgoress["A-2"].Locate(StageFlushed)
		assertInt64(a2Off, 100)
		assertInt64(a2RowID, 3)

		// B
		b1Off, b1RowID := bProgress.FilesPrgoress["B-1"].Locate(StageLoaded)
		assertInt64(b1Off, 100)
		assertInt64(b1RowID, 1)

		b2Off, b2RowID := bProgress.FilesPrgoress["B-2"].Locate(StageLoaded)
		assertInt64(b2Off, 150)
		assertInt64(b2RowID, 2)

		b3Off, b3RowID := bProgress.FilesPrgoress["B-3"].Locate(StageLoaded)
		assertInt64(b3Off, 300)
		assertInt64(b3RowID, 99)
	}
	return
}

func TestBaseStore(t *testing.T) {
	database := "NBA"
	tables := []string{"kobe", "LBJ"}

	db := ConnectDB("localhost", 3306, "root", "")
	dbms := NewProgressDBMS(db, database)
	defer clearDBStore(db, database)

	//
	tpgs := dbms.LoadAllProgress(database, tables)
	assertInt(len(tpgs), 0)

	//
	kbPartitions := map[string]int64{
		"kb-01": 200,
		"kb-02": 800,
	}
	kbProgress := NewTableProgress("NBA", "kobe", kbPartitions)
	kbProgress.FilesPrgoress["kb-01"].Update(StageLoaded, 200, 2)
	kbProgress.FilesPrgoress["kb-01"].Update(StageFlushed, 100, 1)
	kbProgress.FilesPrgoress["kb-02"].Update(StageLoaded, 400, 44)
	kbProgress.FilesPrgoress["kb-02"].Update(StageFlushed, 200, 22)

	lbjPartitions := map[string]int64{
		"lbj-0x066":   20,
		"lbj-0x08888": 80,
	}
	lbjProgress := NewTableProgress("NBA", "LBJ", lbjPartitions)
	lbjProgress.FilesPrgoress["lbj-0x066"].Update(StageLoaded, 20, 202)
	lbjProgress.FilesPrgoress["lbj-0x066"].Update(StageFlushed, 20, 202)
	lbjProgress.FilesPrgoress["lbj-0x08888"].Update(StageLoaded, 40, 404)
	lbjProgress.FilesPrgoress["lbj-0x08888"].Update(StageFlushed, 40, 404)
	// lbjProgress.showSummaryProgress()

	dbms.Setup(map[string]*TableProgress{
		"kobe": kbProgress,
		"LBJ":  lbjProgress})

	// tpgs := dbms.loadAllProgress(database, tables)
	// for _, tpg := range tpgs {
	// 	tpg.showSummaryProgress()
	// }
}

func TestTableProgress(t *testing.T) {
	tablePartitions := map[string]int64{
		"A": 200,
		"B": 800,
	}

	tblProgress := NewTableProgress("NBA", "players", tablePartitions)

	aprog := tblProgress.FilesPrgoress["A"]
	aprog.Update(StageLoaded, 200, 2)
	aprog.Update(StageFlushed, 100, 1)

	bprog := tblProgress.FilesPrgoress["B"]
	bprog.Update(StageLoaded, 400, 8)

	// tblProgress.showSummaryProgress()
	stages := tblProgress.Calc()
	assert(stages[StageFlushed], 0.1)
	assert(stages[StageLoaded], 0.5)
	assert(stages[StagePending], 0.4)
}

func TestFileProgress(t *testing.T) {
	fileProgress := NewTableFileProgress("NBA", "players", "west-player-file", 100)

	// 0 : 20 : 80
	fileProgress.Update(StageLoaded, 10, 1)
	fileProgress.Update(StageLoaded, 20, 2)

	regionsRatio := fileProgress.Calc()
	assert(regionsRatio[StageFlushed], 0.0)
	assert(regionsRatio[StageLoaded], 0.2)
	assert(regionsRatio[StagePending], 0.8)

	// 10 : 40 : 50
	fileProgress.Update(StageLoaded, 50, 5)
	fileProgress.Update(StageFlushed, 10, 1)

	regionsRatio = fileProgress.Calc()
	assert(regionsRatio[StageFlushed], 0.1)
	assert(regionsRatio[StageLoaded], 0.4)
	assert(regionsRatio[StagePending], 0.5)

	// 60 : 0 : 40
	fileProgress.Update(StageFlushed, 60, 6)

	regionsRatio = fileProgress.Calc()
	assert(regionsRatio[StageFlushed], 0.6)
	assert(regionsRatio[StageLoaded], 0.0)
	assert(regionsRatio[StagePending], 0.4)

	// 90 : 10 : 0
	fileProgress.Update(StageLoaded, 100, 10)
	fileProgress.Update(StageFlushed, 90, 10)

	regionsRatio = fileProgress.Calc()
	assert(regionsRatio[StageFlushed], 0.9)
	assert(regionsRatio[StageLoaded], 0.1)
	assert(regionsRatio[StagePending], 0.0)

	if fileProgress.Finished() {
		panic("should not be finished !")
	}

	// 100 : 0 : 0
	fileProgress.Update(StageFlushed, 100, 10)

	regionsRatio = fileProgress.Calc()
	assert(regionsRatio[StageFlushed], 1.0)
	assert(regionsRatio[StageLoaded], 0.0)
	assert(regionsRatio[StagePending], 0.0)

	if !fileProgress.Finished() {
		panic("should be finished !")
	}

	return
}
