// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package restore

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cznic/mathutil"
	"github.com/joho/sqltocsv"
	"github.com/pingcap/errors"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/mydump"
	verify "github.com/pingcap/tidb-lightning/lightning/verification"
)

type CheckpointStatus uint8

const (
	CheckpointStatusMaxInvalid      CheckpointStatus = 25
	CheckpointStatusLoaded          CheckpointStatus = 30
	CheckpointStatusAllWritten      CheckpointStatus = 60
	CheckpointStatusClosed          CheckpointStatus = 90
	CheckpointStatusImported        CheckpointStatus = 120
	CheckpointStatusAlteredAutoInc  CheckpointStatus = 150
	CheckpointStatusChecksumSkipped CheckpointStatus = 170
	CheckpointStatusChecksummed     CheckpointStatus = 180
	CheckpointStatusAnalyzeSkipped  CheckpointStatus = 200
	CheckpointStatusAnalyzed        CheckpointStatus = 210
)

const nodeID = 0

const (
	// the table names to store each kind of checkpoint in the checkpoint database
	// remember to increase the version number in case of incompatible change.
	checkpointTableNameTable  = "table_v4"
	checkpointTableNameEngine = "engine_v5"
	checkpointTableNameChunk  = "chunk_v4"
)

func (status CheckpointStatus) MetricName() string {
	switch status {
	case CheckpointStatusLoaded:
		return "pending"
	case CheckpointStatusAllWritten:
		return "written"
	case CheckpointStatusClosed:
		return "closed"
	case CheckpointStatusImported:
		return "imported"
	case CheckpointStatusAlteredAutoInc:
		return "altered_auto_inc"
	case CheckpointStatusChecksummed, CheckpointStatusChecksumSkipped:
		return "checksum"
	case CheckpointStatusAnalyzed, CheckpointStatusAnalyzeSkipped:
		return "analyzed"
	default:
		return "invalid"
	}
}

type ChunkCheckpointKey struct {
	Path   string
	Offset int64
}

func (key *ChunkCheckpointKey) String() string {
	return fmt.Sprintf("%s:%d", key.Path, key.Offset)
}

func (key *ChunkCheckpointKey) less(other *ChunkCheckpointKey) bool {
	switch {
	case key.Path < other.Path:
		return true
	case key.Path > other.Path:
		return false
	default:
		return key.Offset < other.Offset
	}
}

type ChunkCheckpoint struct {
	Key                ChunkCheckpointKey
	Columns            []byte
	ShouldIncludeRowID bool
	Chunk              mydump.Chunk
	Checksum           verify.KVChecksum
}

type EngineCheckpoint struct {
	Status CheckpointStatus
	Chunks []*ChunkCheckpoint // a sorted array
}

type TableCheckpoint struct {
	Status    CheckpointStatus
	AllocBase int64
	Engines   []*EngineCheckpoint
}

func (cp *TableCheckpoint) CountChunks() int {
	result := 0
	for _, engine := range cp.Engines {
		result += len(engine.Chunks)
	}
	return result
}

type chunkCheckpointDiff struct {
	pos      int64
	rowID    int64
	checksum verify.KVChecksum
}

type engineCheckpointDiff struct {
	hasStatus bool
	status    CheckpointStatus
	chunks    map[ChunkCheckpointKey]chunkCheckpointDiff
}

type TableCheckpointDiff struct {
	hasStatus bool
	hasRebase bool
	status    CheckpointStatus
	allocBase int64
	engines   map[int]engineCheckpointDiff
}

func NewTableCheckpointDiff() *TableCheckpointDiff {
	return &TableCheckpointDiff{
		engines: make(map[int]engineCheckpointDiff),
	}
}

func (cpd *TableCheckpointDiff) insertEngineCheckpointDiff(engineID int, newDiff engineCheckpointDiff) {
	if oldDiff, ok := cpd.engines[engineID]; ok {
		if newDiff.hasStatus {
			oldDiff.hasStatus = true
			oldDiff.status = newDiff.status
		}
		for key, chunkDiff := range newDiff.chunks {
			oldDiff.chunks[key] = chunkDiff
		}
		newDiff = oldDiff
	}
	cpd.engines[engineID] = newDiff
}

func (cpd *TableCheckpointDiff) String() string {
	return fmt.Sprintf(
		"{hasStatus:%v, hasRebase:%v, status:%d, allocBase:%d, engines:[%d]}",
		cpd.hasStatus, cpd.hasRebase, cpd.status, cpd.allocBase, len(cpd.engines),
	)
}

type TableCheckpointMerger interface {
	// MergeInto the table checkpoint diff from a status update or chunk update.
	// If there are multiple updates to the same table, only the last one will
	// take effect. Therefore, the caller must ensure events for the same table
	// are properly ordered by the global time (an old event must be merged
	// before the new one).
	MergeInto(cpd *TableCheckpointDiff)
}

type StatusCheckpointMerger struct {
	EngineID int // -1 == apply to whole table.
	Status   CheckpointStatus
}

func (merger *StatusCheckpointMerger) SetInvalid() {
	merger.Status /= 10
}

func (merger *StatusCheckpointMerger) MergeInto(cpd *TableCheckpointDiff) {
	if merger.EngineID == -1 || merger.Status <= CheckpointStatusMaxInvalid {
		cpd.status = merger.Status
		cpd.hasStatus = true
	}
	if merger.EngineID >= 0 {
		cpd.insertEngineCheckpointDiff(merger.EngineID, engineCheckpointDiff{
			hasStatus: true,
			status:    merger.Status,
			chunks:    make(map[ChunkCheckpointKey]chunkCheckpointDiff),
		})
	}
}

type ChunkCheckpointMerger struct {
	EngineID int
	Key      ChunkCheckpointKey
	Checksum verify.KVChecksum
	Pos      int64
	RowID    int64
}

func (merger *ChunkCheckpointMerger) MergeInto(cpd *TableCheckpointDiff) {
	cpd.insertEngineCheckpointDiff(merger.EngineID, engineCheckpointDiff{
		chunks: map[ChunkCheckpointKey]chunkCheckpointDiff{
			merger.Key: {
				pos:      merger.Pos,
				rowID:    merger.RowID,
				checksum: merger.Checksum,
			},
		},
	})
}

type RebaseCheckpointMerger struct {
	AllocBase int64
}

func (merger *RebaseCheckpointMerger) MergeInto(cpd *TableCheckpointDiff) {
	cpd.hasRebase = true
	cpd.allocBase = mathutil.MaxInt64(cpd.allocBase, merger.AllocBase)
}

type DestroyedTableCheckpoint struct {
	TableName    string
	EnginesCount int
}

type CheckpointsDB interface {
	Initialize(ctx context.Context, dbInfo map[string]*TidbDBInfo) error
	Get(ctx context.Context, tableName string) (*TableCheckpoint, error)
	Close() error
	InsertEngineCheckpoints(ctx context.Context, tableName string, checkpoints []*EngineCheckpoint) error
	Update(checkpointDiffs map[string]*TableCheckpointDiff)

	RemoveCheckpoint(ctx context.Context, tableName string) error
	IgnoreErrorCheckpoint(ctx context.Context, tableName string) error
	DestroyErrorCheckpoint(ctx context.Context, tableName string) ([]DestroyedTableCheckpoint, error)
	DumpTables(ctx context.Context, csv io.Writer) error
	DumpEngines(ctx context.Context, csv io.Writer) error
	DumpChunks(ctx context.Context, csv io.Writer) error
}

// NullCheckpointsDB is a checkpoints database with no checkpoints.
type NullCheckpointsDB struct{}

func NewNullCheckpointsDB() *NullCheckpointsDB {
	return &NullCheckpointsDB{}
}

func (*NullCheckpointsDB) Initialize(context.Context, map[string]*TidbDBInfo) error {
	return nil
}
func (*NullCheckpointsDB) Close() error {
	return nil
}

func (*NullCheckpointsDB) Get(_ context.Context, _ string) (*TableCheckpoint, error) {
	return &TableCheckpoint{
		Status: CheckpointStatusLoaded,
	}, nil
}

func (*NullCheckpointsDB) InsertEngineCheckpoints(_ context.Context, _ string, _ []*EngineCheckpoint) error {
	return nil
}

func (*NullCheckpointsDB) Update(map[string]*TableCheckpointDiff) {}

type MySQLCheckpointsDB struct {
	db      *sql.DB
	schema  string
	session uint64
}

func NewMySQLCheckpointsDB(ctx context.Context, db *sql.DB, schemaName string) (*MySQLCheckpointsDB, error) {
	var escapedSchemaName strings.Builder
	common.WriteMySQLIdentifier(&escapedSchemaName, schemaName)
	schema := escapedSchemaName.String()

	err := common.ExecWithRetry(ctx, db, "(create checkpoints database)", fmt.Sprintf(`
		CREATE DATABASE IF NOT EXISTS %s;
	`, schema))
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = common.ExecWithRetry(ctx, db, "(create table checkpoints table)", fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			node_id int unsigned NOT NULL,
			session bigint unsigned NOT NULL,
			table_name varchar(261) NOT NULL PRIMARY KEY,
			hash binary(32) NOT NULL,
			status tinyint unsigned DEFAULT 30,
			alloc_base bigint NOT NULL DEFAULT 0,
			create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			INDEX(node_id, session)
		);
	`, schema, checkpointTableNameTable))
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = common.ExecWithRetry(ctx, db, "(create engine checkpoints table)", fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			table_name varchar(261) NOT NULL,
			engine_id int NOT NULL,
			status tinyint unsigned DEFAULT 30,
			create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			PRIMARY KEY(table_name, engine_id DESC)
		);
	`, schema, checkpointTableNameEngine))
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = common.ExecWithRetry(ctx, db, "(create chunks checkpoints table)", fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			table_name varchar(261) NOT NULL,
			engine_id int unsigned NOT NULL,
			path varchar(2048) NOT NULL,
			offset bigint NOT NULL,
			columns text NULL,
			should_include_row_id BOOL NOT NULL,
			end_offset bigint NOT NULL,
			pos bigint NOT NULL,
			prev_rowid_max bigint NOT NULL,
			rowid_max bigint NOT NULL,
			kvc_bytes bigint unsigned NOT NULL DEFAULT 0,
			kvc_kvs bigint unsigned NOT NULL DEFAULT 0,
			kvc_checksum bigint unsigned NOT NULL DEFAULT 0,
			create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			PRIMARY KEY(table_name, engine_id, path(500), offset)
		);
	`, schema, checkpointTableNameChunk))
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Create a relatively unique number (on the same node) as the session ID.
	session := uint64(time.Now().UnixNano())

	return &MySQLCheckpointsDB{
		db:      db,
		schema:  schema,
		session: session,
	}, nil
}

func (cpdb *MySQLCheckpointsDB) Initialize(ctx context.Context, dbInfo map[string]*TidbDBInfo) error {
	// We can have at most 65535 placeholders https://stackoverflow.com/q/4922345/
	// Since this step is not performance critical, we just insert the rows one-by-one.

	err := common.TransactWithRetry(ctx, cpdb.db, "(insert checkpoints)", func(c context.Context, tx *sql.Tx) error {
		// If `node_id` is not the same but the `table_name` duplicates,
		// the CASE expression will return NULL, which can be used to violate
		// the NOT NULL requirement of `session` column, and caused this INSERT
		// statement to fail with an irrecoverable error.
		// We do need to capture the error is display a user friendly message
		// (multiple nodes cannot import the same table) though.
		stmt, err := tx.PrepareContext(c, fmt.Sprintf(`
			INSERT INTO %s.%s (node_id, session, table_name, hash) VALUES (?, ?, ?, ?)
			ON DUPLICATE KEY UPDATE session = CASE
				WHEN node_id = VALUES(node_id) AND hash = VALUES(hash)
				THEN VALUES(session)
			END;
		`, cpdb.schema, checkpointTableNameTable))
		if err != nil {
			return errors.Trace(err)
		}
		defer stmt.Close()

		for _, db := range dbInfo {
			for _, table := range db.Tables {
				tableName := common.UniqueTable(db.Name, table.Name)
				_, err = stmt.ExecContext(c, nodeID, cpdb.session, tableName, 0)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}

		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (cpdb *MySQLCheckpointsDB) Close() error {
	return errors.Trace(cpdb.db.Close())
}

func (cpdb *MySQLCheckpointsDB) Get(ctx context.Context, tableName string) (*TableCheckpoint, error) {
	cp := new(TableCheckpoint)

	purpose := "(read checkpoint " + tableName + ")"
	err := common.TransactWithRetry(ctx, cpdb.db, purpose, func(c context.Context, tx *sql.Tx) error {
		// 1. Populate the engines.

		engineQuery := fmt.Sprintf(`
			SELECT engine_id, status FROM %s.%s WHERE table_name = ? ORDER BY engine_id DESC;
		`, cpdb.schema, checkpointTableNameEngine)
		engineRows, err := tx.QueryContext(c, engineQuery, tableName)
		if err != nil {
			return errors.Trace(err)
		}
		defer engineRows.Close()
		for engineRows.Next() {
			var (
				engineID int
				status   uint8
			)
			if err := engineRows.Scan(&engineID, &status); err != nil {
				return errors.Trace(err)
			}
			for len(cp.Engines) <= engineID {
				cp.Engines = append(cp.Engines, new(EngineCheckpoint))
			}
			cp.Engines[engineID].Status = CheckpointStatus(status)
		}
		if err := engineRows.Err(); err != nil {
			return errors.Trace(err)
		}

		// 2. Populate the chunks.

		chunkQuery := fmt.Sprintf(`
			SELECT
				engine_id, path, offset, columns, should_include_row_id,
				pos, end_offset, prev_rowid_max, rowid_max,
				kvc_bytes, kvc_kvs, kvc_checksum
			FROM %s.%s WHERE table_name = ?
			ORDER BY engine_id, path, offset;
		`, cpdb.schema, checkpointTableNameChunk)
		chunkRows, err := tx.QueryContext(c, chunkQuery, tableName)
		if err != nil {
			return errors.Trace(err)
		}
		defer chunkRows.Close()
		for chunkRows.Next() {
			var (
				value       = new(ChunkCheckpoint)
				engineID    int
				kvcBytes    uint64
				kvcKVs      uint64
				kvcChecksum uint64
			)
			if err := chunkRows.Scan(
				&engineID, &value.Key.Path, &value.Key.Offset, &value.Columns, &value.ShouldIncludeRowID,
				&value.Chunk.Offset, &value.Chunk.EndOffset, &value.Chunk.PrevRowIDMax, &value.Chunk.RowIDMax,
				&kvcBytes, &kvcKVs, &kvcChecksum,
			); err != nil {
				return errors.Trace(err)
			}
			value.Checksum = verify.MakeKVChecksum(kvcBytes, kvcKVs, kvcChecksum)
			cp.Engines[engineID].Chunks = append(cp.Engines[engineID].Chunks, value)
		}
		if err := chunkRows.Err(); err != nil {
			return errors.Trace(err)
		}

		// 3. Fill in the remaining table info

		tableQuery := fmt.Sprintf(`
			SELECT status, alloc_base FROM %s.%s WHERE table_name = ?
		`, cpdb.schema, checkpointTableNameTable)
		tableRow := tx.QueryRowContext(c, tableQuery, tableName)

		var status uint8
		if err := tableRow.Scan(&status, &cp.AllocBase); err != nil {
			return errors.Trace(err)
		}
		cp.Status = CheckpointStatus(status)
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return cp, nil
}

func (cpdb *MySQLCheckpointsDB) InsertEngineCheckpoints(ctx context.Context, tableName string, checkpoints []*EngineCheckpoint) error {
	err := common.TransactWithRetry(ctx, cpdb.db, "(update engine checkpoints for "+tableName+")", func(c context.Context, tx *sql.Tx) error {
		engineStmt, err := tx.PrepareContext(c, fmt.Sprintf(`
			REPLACE INTO %s.%s (table_name, engine_id, status) VALUES (?, ?, ?);
		`, cpdb.schema, checkpointTableNameEngine))
		if err != nil {
			return errors.Trace(err)
		}
		defer engineStmt.Close()

		chunkStmt, err := tx.PrepareContext(c, fmt.Sprintf(`
			REPLACE INTO %s.%s (
				table_name, engine_id,
				path, offset, columns, should_include_row_id,
				pos, end_offset, prev_rowid_max, rowid_max,
				kvc_bytes, kvc_kvs, kvc_checksum
			) VALUES (
				?, ?,
				?, ?, ?, ?,
				?, ?, ?, ?,
				?, ?, ?
			);
		`, cpdb.schema, checkpointTableNameChunk))
		if err != nil {
			return errors.Trace(err)
		}
		defer chunkStmt.Close()

		for engineID, engine := range checkpoints {
			_, err = engineStmt.ExecContext(c, tableName, engineID, engine.Status)
			if err != nil {
				return errors.Trace(err)
			}
			for _, value := range engine.Chunks {
				_, err = chunkStmt.ExecContext(
					c, tableName, engineID,
					value.Key.Path, value.Key.Offset, value.Columns, value.ShouldIncludeRowID,
					value.Chunk.Offset, value.Chunk.EndOffset, value.Chunk.PrevRowIDMax, value.Chunk.RowIDMax,
					value.Checksum.SumSize(), value.Checksum.SumKVS(), value.Checksum.Sum(),
				)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}

		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (cpdb *MySQLCheckpointsDB) Update(checkpointDiffs map[string]*TableCheckpointDiff) {
	chunkQuery := fmt.Sprintf(`
		UPDATE %s.%s SET pos = ?, prev_rowid_max = ?, kvc_bytes = ?, kvc_kvs = ?, kvc_checksum = ?
		WHERE (table_name, engine_id, path, offset) = (?, ?, ?, ?);
	`, cpdb.schema, checkpointTableNameChunk)
	checksumQuery := fmt.Sprintf(`
		UPDATE %s.%s SET alloc_base = GREATEST(?, alloc_base) WHERE table_name = ?;
	`, cpdb.schema, checkpointTableNameTable)
	tableStatusQuery := fmt.Sprintf(`
		UPDATE %s.%s SET status = ? WHERE table_name = ?;
	`, cpdb.schema, checkpointTableNameTable)
	engineStatusQuery := fmt.Sprintf(`
		UPDATE %s.%s SET status = ? WHERE (table_name, engine_id) = (?, ?);
	`, cpdb.schema, checkpointTableNameEngine)

	err := common.TransactWithRetry(context.Background(), cpdb.db, "(update checkpoints)", func(c context.Context, tx *sql.Tx) error {
		chunkStmt, e := tx.PrepareContext(c, chunkQuery)
		if e != nil {
			return errors.Trace(e)
		}
		defer chunkStmt.Close()
		checksumStmt, e := tx.PrepareContext(c, checksumQuery)
		if e != nil {
			return errors.Trace(e)
		}
		defer checksumStmt.Close()
		tableStatusStmt, e := tx.PrepareContext(c, tableStatusQuery)
		if e != nil {
			return errors.Trace(e)
		}
		defer tableStatusStmt.Close()
		engineStatusStmt, e := tx.PrepareContext(c, engineStatusQuery)
		if e != nil {
			return errors.Trace(e)
		}
		defer engineStatusStmt.Close()

		for tableName, cpd := range checkpointDiffs {
			if cpd.hasStatus {
				if _, e := tableStatusStmt.ExecContext(c, cpd.status, tableName); e != nil {
					return errors.Trace(e)
				}
			}
			if cpd.hasRebase {
				if _, e := checksumStmt.ExecContext(c, cpd.allocBase, tableName); e != nil {
					return errors.Trace(e)
				}
			}
			for engineID, engineDiff := range cpd.engines {
				if engineDiff.hasStatus {
					if _, e := engineStatusStmt.ExecContext(c, engineDiff.status, tableName, engineID); e != nil {
						return errors.Trace(e)
					}
				}
				for key, diff := range engineDiff.chunks {
					if _, e := chunkStmt.ExecContext(
						c,
						diff.pos, diff.rowID, diff.checksum.SumSize(), diff.checksum.SumKVS(), diff.checksum.Sum(),
						tableName, engineID, key.Path, key.Offset,
					); e != nil {
						return errors.Trace(e)
					}
				}
			}
		}

		return nil
	})
	if err != nil {
		common.AppLogger.Errorf("failed to save checkpoint: %v", err)
	}
}

type FileCheckpointsDB struct {
	lock        sync.Mutex // we need to ensure only a thread can access to `checkpoints` at a time
	checkpoints CheckpointsModel
	path        string
}

func NewFileCheckpointsDB(path string) *FileCheckpointsDB {
	cpdb := &FileCheckpointsDB{path: path}
	// ignore all errors -- file maybe not created yet (and it is fine).
	content, err := ioutil.ReadFile(path)
	if err == nil {
		cpdb.checkpoints.Unmarshal(content)
	} else {
		common.AppLogger.Warnf("failed to open checkpoint file %s, going to create a new one: %v", path, err)
	}
	return cpdb
}

func (cpdb *FileCheckpointsDB) save() error {
	serialized, err := cpdb.checkpoints.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	if err := ioutil.WriteFile(cpdb.path, serialized, 0644); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (cpdb *FileCheckpointsDB) Initialize(ctx context.Context, dbInfo map[string]*TidbDBInfo) error {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	if cpdb.checkpoints.Checkpoints == nil {
		cpdb.checkpoints.Checkpoints = make(map[string]*TableCheckpointModel)
	}

	for _, db := range dbInfo {
		for _, table := range db.Tables {
			tableName := common.UniqueTable(db.Name, table.Name)
			if _, ok := cpdb.checkpoints.Checkpoints[tableName]; !ok {
				cpdb.checkpoints.Checkpoints[tableName] = &TableCheckpointModel{
					Status: uint32(CheckpointStatusLoaded),
				}
			}
			// TODO check if hash matches
		}
	}

	return errors.Trace(cpdb.save())
}

func (cpdb *FileCheckpointsDB) Close() error {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	return errors.Trace(cpdb.save())
}

func (cpdb *FileCheckpointsDB) Get(_ context.Context, tableName string) (*TableCheckpoint, error) {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	tableModel := cpdb.checkpoints.Checkpoints[tableName]

	cp := &TableCheckpoint{
		Status:    CheckpointStatus(tableModel.Status),
		AllocBase: tableModel.AllocBase,
		Engines:   make([]*EngineCheckpoint, 0, len(tableModel.Engines)),
	}

	for _, engineModel := range tableModel.Engines {
		engine := &EngineCheckpoint{
			Status: CheckpointStatus(engineModel.Status),
			Chunks: make([]*ChunkCheckpoint, 0, len(engineModel.Chunks)),
		}

		for _, chunkModel := range engineModel.Chunks {
			engine.Chunks = append(engine.Chunks, &ChunkCheckpoint{
				Key: ChunkCheckpointKey{
					Path:   chunkModel.Path,
					Offset: chunkModel.Offset,
				},
				Columns:            chunkModel.Columns,
				ShouldIncludeRowID: chunkModel.ShouldIncludeRowId,
				Chunk: mydump.Chunk{
					Offset:       chunkModel.Pos,
					EndOffset:    chunkModel.EndOffset,
					PrevRowIDMax: chunkModel.PrevRowidMax,
					RowIDMax:     chunkModel.RowidMax,
				},
				Checksum: verify.MakeKVChecksum(chunkModel.KvcBytes, chunkModel.KvcKvs, chunkModel.KvcChecksum),
			})
		}

		sort.Slice(engine.Chunks, func(i, j int) bool {
			return engine.Chunks[i].Key.less(&engine.Chunks[j].Key)
		})

		cp.Engines = append(cp.Engines, engine)
	}

	return cp, nil
}

func (cpdb *FileCheckpointsDB) InsertEngineCheckpoints(_ context.Context, tableName string, checkpoints []*EngineCheckpoint) error {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	tableModel := cpdb.checkpoints.Checkpoints[tableName]
	for len(tableModel.Engines) < len(checkpoints) {
		tableModel.Engines = append(tableModel.Engines, &EngineCheckpointModel{
			Status: uint32(CheckpointStatusLoaded),
			Chunks: make(map[string]*ChunkCheckpointModel),
		})
	}

	for engineID, engine := range checkpoints {
		engineModel := tableModel.Engines[engineID]
		for _, value := range engine.Chunks {
			key := value.Key.String()
			chunk, ok := engineModel.Chunks[key]
			if !ok {
				chunk = &ChunkCheckpointModel{
					Path:               value.Key.Path,
					Offset:             value.Key.Offset,
					Columns:            value.Columns,
					ShouldIncludeRowId: value.ShouldIncludeRowID,
				}
				engineModel.Chunks[key] = chunk
			}
			chunk.Pos = value.Chunk.Offset
			chunk.EndOffset = value.Chunk.EndOffset
			chunk.PrevRowidMax = value.Chunk.PrevRowIDMax
			chunk.RowidMax = value.Chunk.RowIDMax
			chunk.KvcBytes = value.Checksum.SumSize()
			chunk.KvcKvs = value.Checksum.SumKVS()
			chunk.KvcChecksum = value.Checksum.Sum()
		}
	}

	return errors.Trace(cpdb.save())
}

func (cpdb *FileCheckpointsDB) Update(checkpointDiffs map[string]*TableCheckpointDiff) {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	for tableName, cpd := range checkpointDiffs {
		tableModel := cpdb.checkpoints.Checkpoints[tableName]
		if cpd.hasStatus {
			tableModel.Status = uint32(cpd.status)
		}
		if cpd.hasRebase {
			tableModel.AllocBase = cpd.allocBase
		}
		for engineID, engineDiff := range cpd.engines {
			engineModel := tableModel.Engines[engineID]
			if engineDiff.hasStatus {
				engineModel.Status = uint32(engineDiff.status)
			}

			for key, diff := range engineDiff.chunks {
				chunkModel := engineModel.Chunks[key.String()]
				chunkModel.Pos = diff.pos
				chunkModel.PrevRowidMax = diff.rowID
				chunkModel.KvcBytes = diff.checksum.SumSize()
				chunkModel.KvcKvs = diff.checksum.SumKVS()
				chunkModel.KvcChecksum = diff.checksum.Sum()
			}
		}
	}

	if err := cpdb.save(); err != nil {
		common.AppLogger.Errorf("failed to save checkpoint: %v", err)
	}
}

// Management functions ----------------------------------------------------------------------------

var cannotManageNullDB = errors.New("cannot perform this function while checkpoints is disabled")

func (*NullCheckpointsDB) RemoveCheckpoint(context.Context, string) error {
	return errors.Trace(cannotManageNullDB)
}
func (*NullCheckpointsDB) IgnoreErrorCheckpoint(context.Context, string) error {
	return errors.Trace(cannotManageNullDB)
}
func (*NullCheckpointsDB) DestroyErrorCheckpoint(context.Context, string) ([]DestroyedTableCheckpoint, error) {
	return nil, errors.Trace(cannotManageNullDB)
}
func (*NullCheckpointsDB) DumpTables(context.Context, io.Writer) error {
	return errors.Trace(cannotManageNullDB)
}
func (*NullCheckpointsDB) DumpEngines(context.Context, io.Writer) error {
	return errors.Trace(cannotManageNullDB)
}
func (*NullCheckpointsDB) DumpChunks(context.Context, io.Writer) error {
	return errors.Trace(cannotManageNullDB)
}

func (cpdb *MySQLCheckpointsDB) RemoveCheckpoint(ctx context.Context, tableName string) error {
	var (
		deleteChunkFmt  string
		deleteEngineFmt string
		deleteTableFmt  string
		arg             interface{}
	)

	if tableName == "all" {
		deleteChunkFmt = "DELETE FROM %[1]s.%[2]s WHERE table_name IN (SELECT table_name FROM %[1]s.%[3]s WHERE node_id = ?)"
		deleteEngineFmt = "DELETE FROM %[1]s.%[2]s WHERE table_name IN (SELECT table_name FROM %[1]s.%[3]s WHERE node_id = ?)"
		deleteTableFmt = "DELETE FROM %s.%s WHERE node_id = ?"
		arg = nodeID
	} else {
		deleteChunkFmt = "DELETE FROM %s.%s WHERE table_name = ?%.0s" // the %.0s is to consume the third parameter.
		deleteEngineFmt = "DELETE FROM %s.%s WHERE table_name = ?%.0s"
		deleteTableFmt = "DELETE FROM %s.%s WHERE table_name = ?"
		arg = tableName
	}

	deleteChunkQuery := fmt.Sprintf(deleteChunkFmt, cpdb.schema, checkpointTableNameChunk, checkpointTableNameTable)
	deleteEngineQuery := fmt.Sprintf(deleteEngineFmt, cpdb.schema, checkpointTableNameEngine, checkpointTableNameTable)
	deleteTableQuery := fmt.Sprintf(deleteTableFmt, cpdb.schema, checkpointTableNameTable)
	err := common.TransactWithRetry(ctx, cpdb.db, fmt.Sprintf("(remove checkpoints of %s)", tableName), func(c context.Context, tx *sql.Tx) error {
		if _, e := tx.ExecContext(c, deleteChunkQuery, arg); e != nil {
			return errors.Trace(e)
		}
		if _, e := tx.ExecContext(c, deleteEngineQuery, arg); e != nil {
			return errors.Trace(e)
		}
		if _, e := tx.ExecContext(c, deleteTableQuery, arg); e != nil {
			return errors.Trace(e)
		}
		return nil
	})
	return errors.Trace(err)
}

func (cpdb *MySQLCheckpointsDB) IgnoreErrorCheckpoint(ctx context.Context, tableName string) error {
	var (
		colName string
		arg     interface{}
	)
	if tableName == "all" {
		colName, arg = "node_id", nodeID
	} else {
		colName, arg = "table_name", tableName
	}
	engineQuery := fmt.Sprintf(`
		UPDATE %s.%s SET status = %d WHERE %s = ? AND status <= %d;
	`, cpdb.schema, checkpointTableNameEngine, CheckpointStatusLoaded, colName, CheckpointStatusMaxInvalid)
	tableQuery := fmt.Sprintf(`
		UPDATE %s.%s SET status = %d WHERE %s = ? AND status <= %d;
	`, cpdb.schema, checkpointTableNameTable, CheckpointStatusLoaded, colName, CheckpointStatusMaxInvalid)

	err := common.TransactWithRetry(ctx, cpdb.db, fmt.Sprintf("(ignore error checkpoints for %s)", tableName), func(c context.Context, tx *sql.Tx) error {
		if _, e := tx.ExecContext(c, engineQuery, arg); e != nil {
			return errors.Trace(e)
		}
		if _, e := tx.ExecContext(c, tableQuery, arg); e != nil {
			return errors.Trace(e)
		}
		return nil
	})
	return errors.Trace(err)
}

func (cpdb *MySQLCheckpointsDB) DestroyErrorCheckpoint(ctx context.Context, tableName string) ([]DestroyedTableCheckpoint, error) {
	var (
		conditionColumn string
		arg             interface{}
	)

	if tableName == "all" {
		conditionColumn = "node_id"
		arg = nodeID
	} else {
		conditionColumn = "table_name"
		arg = tableName
	}

	selectQuery := fmt.Sprintf(`
		SELECT
			t.table_name,
			COALESCE(MAX(e.engine_id) + 1, 0)
		FROM %[1]s.%[4]s t
		LEFT JOIN %[1]s.%[5]s e ON t.table_name = e.table_name
		WHERE t.%[2]s = ? AND t.status <= %[3]d
		GROUP BY t.table_name;
	`, cpdb.schema, conditionColumn, CheckpointStatusMaxInvalid, checkpointTableNameTable, checkpointTableNameEngine)
	deleteChunkQuery := fmt.Sprintf(`
		DELETE FROM %[1]s.%[4]s WHERE table_name IN (SELECT table_name FROM %[1]s.%[5]s WHERE %[2]s = ? AND status <= %[3]d)
	`, cpdb.schema, conditionColumn, CheckpointStatusMaxInvalid, checkpointTableNameChunk, checkpointTableNameTable)
	deleteEngineQuery := fmt.Sprintf(`
		DELETE FROM %[1]s.%[4]s WHERE table_name IN (SELECT table_name FROM %[1]s.%[5]s WHERE %[2]s = ? AND status <= %[3]d)
	`, cpdb.schema, conditionColumn, CheckpointStatusMaxInvalid, checkpointTableNameEngine, checkpointTableNameTable)
	deleteTableQuery := fmt.Sprintf(`
		DELETE FROM %s.%s WHERE %s = ? AND status <= %d
	`, cpdb.schema, checkpointTableNameTable, conditionColumn, CheckpointStatusMaxInvalid)

	var targetTables []DestroyedTableCheckpoint

	err := common.TransactWithRetry(ctx, cpdb.db, fmt.Sprintf("(destroy error checkpoints for %s)", tableName), func(c context.Context, tx *sql.Tx) error {
		// Obtain the list of tables
		targetTables = nil
		rows, e := tx.QueryContext(c, selectQuery, arg)
		if e != nil {
			return errors.Trace(e)
		}
		defer rows.Close()
		for rows.Next() {
			var dtc DestroyedTableCheckpoint
			if e := rows.Scan(&dtc.TableName, &dtc.EnginesCount); e != nil {
				return errors.Trace(e)
			}
			targetTables = append(targetTables, dtc)
		}
		if e := rows.Err(); e != nil {
			return errors.Trace(e)
		}

		// Delete the checkpoints
		if _, e := tx.ExecContext(c, deleteChunkQuery, arg); e != nil {
			return errors.Trace(e)
		}
		if _, e := tx.ExecContext(c, deleteEngineQuery, arg); e != nil {
			return errors.Trace(e)
		}
		if _, e := tx.ExecContext(c, deleteTableQuery, arg); e != nil {
			return errors.Trace(e)
		}
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return targetTables, nil
}

func (cpdb *MySQLCheckpointsDB) DumpTables(ctx context.Context, writer io.Writer) error {
	rows, err := cpdb.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			node_id,
			session,
			table_name,
			hex(hash) AS hash,
			status,
			alloc_base,
			create_time,
			update_time
		FROM %s.%s;
	`, cpdb.schema, checkpointTableNameTable))
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	return errors.Trace(sqltocsv.Write(writer, rows))
}

func (cpdb *MySQLCheckpointsDB) DumpEngines(ctx context.Context, writer io.Writer) error {
	rows, err := cpdb.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			table_name,
			engine_id,
			status,
			create_time,
			update_time
		FROM %s.%s;
	`, cpdb.schema, checkpointTableNameEngine))
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	return errors.Trace(sqltocsv.Write(writer, rows))
}

func (cpdb *MySQLCheckpointsDB) DumpChunks(ctx context.Context, writer io.Writer) error {
	rows, err := cpdb.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			table_name,
			path,
			offset,
			columns,
			pos,
			end_offset,
			prev_rowid_max,
			rowid_max,
			kvc_bytes,
			kvc_kvs,
			kvc_checksum,
			create_time,
			update_time
		FROM %s.%s;
	`, cpdb.schema, checkpointTableNameChunk))
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	return errors.Trace(sqltocsv.Write(writer, rows))
}

func (cpdb *FileCheckpointsDB) RemoveCheckpoint(_ context.Context, tableName string) error {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	if tableName == "all" {
		cpdb.checkpoints.Reset()
	} else {
		delete(cpdb.checkpoints.Checkpoints, tableName)
	}
	return errors.Trace(cpdb.save())
}

func (cpdb *FileCheckpointsDB) IgnoreErrorCheckpoint(_ context.Context, targetTableName string) error {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	for tableName, tableModel := range cpdb.checkpoints.Checkpoints {
		if !(targetTableName == "all" || targetTableName == tableName) {
			continue
		}
		if tableModel.Status <= uint32(CheckpointStatusMaxInvalid) {
			tableModel.Status = uint32(CheckpointStatusLoaded)
		}
		for _, engineModel := range tableModel.Engines {
			if engineModel.Status <= uint32(CheckpointStatusMaxInvalid) {
				engineModel.Status = uint32(CheckpointStatusLoaded)
			}
		}
	}
	return errors.Trace(cpdb.save())
}

func (cpdb *FileCheckpointsDB) DestroyErrorCheckpoint(_ context.Context, targetTableName string) ([]DestroyedTableCheckpoint, error) {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	var targetTables []DestroyedTableCheckpoint

	for tableName, tableModel := range cpdb.checkpoints.Checkpoints {
		// Obtain the list of tables
		if !(targetTableName == "all" || targetTableName == tableName) {
			continue
		}
		if tableModel.Status <= uint32(CheckpointStatusMaxInvalid) {
			targetTables = append(targetTables, DestroyedTableCheckpoint{
				TableName:    tableName,
				EnginesCount: len(tableModel.Engines),
			})
		}
	}

	// Delete the checkpoints
	for _, dtcp := range targetTables {
		delete(cpdb.checkpoints.Checkpoints, dtcp.TableName)
	}
	if err := cpdb.save(); err != nil {
		return nil, errors.Trace(err)
	}

	return targetTables, nil
}

func (cpdb *FileCheckpointsDB) DumpTables(context.Context, io.Writer) error {
	return errors.Errorf("dumping file checkpoint into CSV not unsupported, you may copy %s instead", cpdb.path)
}

func (cpdb *FileCheckpointsDB) DumpEngines(context.Context, io.Writer) error {
	return errors.Errorf("dumping file checkpoint into CSV not unsupported, you may copy %s instead", cpdb.path)
}

func (cpdb *FileCheckpointsDB) DumpChunks(context.Context, io.Writer) error {
	return errors.Errorf("dumping file checkpoint into CSV not unsupported, you may copy %s instead", cpdb.path)
}
