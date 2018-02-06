package restore

import (
	"fmt"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/store/tikv"

	"github.com/ngaut/log"
	goctx "golang.org/x/net/context"
)

func init() {
	domain.RunAutoAnalyze = false
	tidb.SetStatsLease(0)
}

func initKVStorage(pd string) (kv.Storage, *domain.Domain, error) {
	var (
		store kv.Storage
		dom   *domain.Domain
		err   error
	)
	defer func() {
		if err != nil {
			if store != nil {
				store.Close()
			}
			if dom != nil {
				dom.Close()
			}
		}
	}()

	store, err = tikv.Driver{}.Open("tikv://" + pd)
	if err != nil {
		return nil, nil, err
	}

	dom, err = tidb.BootstrapSession(store)
	if err != nil {
		return nil, nil, err
	}

	return store, dom, nil
}

type TiDBManager struct {
	store kv.Storage
	dom   *domain.Domain
}

type TidbDBInfo struct {
	ID        int64
	Name      string
	Tables    map[string]*TidbTableInfo
	Available bool
}

type TidbTableInfo struct {
	ID        int64
	Name      string
	Columns   int
	Indices   int
	Available bool
}

func NewTiDBManager(pdAddr string) (*TiDBManager, error) {
	kvStore, dom, err := initKVStorage(pdAddr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	timgr := &TiDBManager{
		store: kvStore,
		dom:   dom,
	}

	return timgr, nil
}

func (timgr *TiDBManager) Close() {
	timgr.dom.Close()
	timgr.store.Close()
}

func (timgr *TiDBManager) InitSchema(database string, tablesSchema map[string]string) error {
	se, err := tidb.CreateSession(timgr.store)
	if err != nil {
		return errors.Trace(err)
	}
	defer se.Close()

	// TODO : all execute in one transaction ?

	ctx := goctx.Background()

	_, err = se.Execute(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", database))
	_, err = se.Execute(ctx, fmt.Sprintf("USE %s", database))
	if err != nil {
		return errors.Trace(err)
	}

	for _, sqlCreateTable := range tablesSchema {
		if _, err = se.Execute(ctx, sqlCreateTable); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (timgr *TiDBManager) LoadSchemaInfo(database string) *TidbDBInfo {
	se, err := tidb.CreateSession(timgr.store)
	if err != nil {
		log.Error(err.Error())
		return nil
	}
	defer se.Close()

	var dbInfo *TidbDBInfo
	dom := domain.GetDomain(se)
	schemas := dom.InfoSchema().AllSchemas() // ps : model/model.go

	for _, db := range schemas {
		if db.Name.String() != database {
			continue
		}

		dbInfo = &TidbDBInfo{
			ID:        db.ID,
			Name:      db.Name.String(),
			Tables:    make(map[string]*TidbTableInfo),
			Available: db.State == model.StatePublic,
		}

		for _, tbl := range db.Tables {
			tableName := tbl.Name.String()

			tableInfo := &TidbTableInfo{
				ID:        tbl.ID,
				Name:      tableName,
				Columns:   len(tbl.Columns),
				Indices:   len(tbl.Indices),
				Available: tbl.State == model.StatePublic,
			}
			dbInfo.Tables[tableName] = tableInfo

			if !tableInfo.Available {
				log.Warnf("Table [%s] not available : state = %v",
					tableName, tbl.State)
			}
		}
	}

	return dbInfo
}

func (timgr *TiDBManager) SyncSchema(database string) *TidbDBInfo {
	// TODO : change to timeout ~
	for i := 0; i < 100; i++ {
		done := true
		dbInfo := timgr.LoadSchemaInfo(database)
		for _, tblInfo := range dbInfo.Tables {
			if !tblInfo.Available {
				done = false
				break
			}
		}
		if !done {
			log.Warnf("Not all tables ready yet")
			time.Sleep(time.Second * 5)
			continue
		}
		break
	}

	return timgr.LoadSchemaInfo(database)
}
