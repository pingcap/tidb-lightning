package restore

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/joho/sqltocsv"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/mydump"
	verify "github.com/pingcap/tidb-lightning/lightning/verification"
)

type CheckpointStatus uint8

const (
	CheckpointStatusMaxInvalid     CheckpointStatus = 25
	CheckpointStatusLoaded         CheckpointStatus = 30
	CheckpointStatusAllWritten     CheckpointStatus = 60
	CheckpointStatusClosed         CheckpointStatus = 90
	CheckpointStatusImported       CheckpointStatus = 120
	CheckpointStatusAlteredAutoInc CheckpointStatus = 150
	CheckpointStatusCompleted      CheckpointStatus = 180
)

const insertCheckpointRetry = 3

const nodeID = 0

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
	case CheckpointStatusCompleted:
		return "checksum"
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

type ChunkCheckpoint struct {
	Key                ChunkCheckpointKey
	Columns            []byte
	ShouldIncludeRowID bool
	Chunk              mydump.Chunk
	Checksum           verify.KVChecksum
}

type TableCheckpoint struct {
	Status    CheckpointStatus
	Engine    uuid.UUID
	AllocBase int64
	Chunks    []*ChunkCheckpoint // a sorted array
}

func (cp *TableCheckpoint) resetChunks() {
	cp.Chunks = nil
}

type chunkCheckpointDiff struct {
	path     string
	offset   int64
	pos      int64
	rowID    int64
	checksum verify.KVChecksum
}

type TableCheckpointDiff struct {
	hasStatus bool
	hasChunks bool
	status    CheckpointStatus
	allocBase int64
	chunks    map[ChunkCheckpointKey]chunkCheckpointDiff
}

func NewTableCheckpointDiff() *TableCheckpointDiff {
	return &TableCheckpointDiff{
		status: CheckpointStatusMaxInvalid + 1,
		chunks: make(map[ChunkCheckpointKey]chunkCheckpointDiff),
	}
}

func (cpd *TableCheckpointDiff) String() string {
	return fmt.Sprintf(
		"{hasStatus:%v, hasChunks:%v, status:%d, allocBase:%d, chunks:[%d]}",
		cpd.hasStatus, cpd.hasChunks, cpd.status, cpd.allocBase, len(cpd.chunks),
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
	Status CheckpointStatus
}

func (merger *StatusCheckpointMerger) SetInvalid() {
	merger.Status /= 10
}

func (merger *StatusCheckpointMerger) MergeInto(cpd *TableCheckpointDiff) {
	cpd.status = merger.Status
	cpd.hasStatus = true
}

type ChunkCheckpointMerger struct {
	Key       ChunkCheckpointKey
	AllocBase int64
	Checksum  verify.KVChecksum
	Pos       int64
	RowID     int64
}

func (merger *ChunkCheckpointMerger) MergeInto(cpd *TableCheckpointDiff) {
	cpd.hasChunks = true
	cpd.allocBase = merger.AllocBase
	cpd.chunks[merger.Key] = chunkCheckpointDiff{
		pos:      merger.Pos,
		rowID:    merger.RowID,
		checksum: merger.Checksum,
	}
}

type CheckpointsDB interface {
	Initialize(ctx context.Context, dbInfo map[string]*TidbDBInfo) error
	Get(ctx context.Context, tableName string) (*TableCheckpoint, error)
	Close() error
	InsertChunkCheckpoints(ctx context.Context, tableName string, checkpoints []*ChunkCheckpoint) error
	Update(checkpointDiffs map[string]*TableCheckpointDiff)

	RemoveCheckpoint(ctx context.Context, tableName string) error
	IgnoreErrorCheckpoint(ctx context.Context, tableName string) error
	DestroyErrorCheckpoint(ctx context.Context, tableName string, target *TiDBManager) error
	DumpTables(ctx context.Context, csv io.Writer) error
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

func (*NullCheckpointsDB) Get(_ context.Context, tableName string) (*TableCheckpoint, error) {
	return &TableCheckpoint{
		Status: CheckpointStatusLoaded,
		Engine: uuid.NewV4(),
	}, nil
}

func (*NullCheckpointsDB) InsertChunkCheckpoints(_ context.Context, _ string, _ []*ChunkCheckpoint) error {
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

	// Apparently we could execute multiple DDL statements in Exec()
	err := common.ExecWithRetry(ctx, db, "(create checkpoints database)", fmt.Sprintf(`
		CREATE DATABASE IF NOT EXISTS %[1]s;
		CREATE TABLE IF NOT EXISTS %[1]s.table_v1 (
			node_id int unsigned NOT NULL,
			session bigint unsigned NOT NULL,
			table_name varchar(261) NOT NULL PRIMARY KEY,
			hash binary(32) NOT NULL,
			engine binary(16) NOT NULL,
			status tinyint unsigned DEFAULT 30,
			alloc_base bigint NOT NULL DEFAULT 0,
			create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			INDEX(node_id, session)
		);
		CREATE TABLE IF NOT EXISTS %[1]s.chunk_v3 (
			table_name varchar(261) NOT NULL,
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
			PRIMARY KEY(table_name, path, offset)
		);
	`, schema))
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
			INSERT INTO %s.table_v1 (node_id, session, table_name, hash, engine) VALUES (?, ?, ?, ?, ?)
			ON DUPLICATE KEY UPDATE session = CASE
				WHEN node_id = VALUES(node_id) AND hash = VALUES(hash)
				THEN VALUES(session)
			END;
		`, cpdb.schema))
		if err != nil {
			return errors.Trace(err)
		}
		defer stmt.Close()

		for _, db := range dbInfo {
			for _, table := range db.Tables {
				tableName := common.UniqueTable(db.Name, table.Name)
				_, err = stmt.ExecContext(c, nodeID, cpdb.session, tableName, 0, uuid.NewV4().Bytes())
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
	cp.resetChunks()

	purpose := "(read checkpoint " + tableName + ")"
	err := common.TransactWithRetry(ctx, cpdb.db, purpose, func(c context.Context, tx *sql.Tx) error {
		query := fmt.Sprintf(`
			SELECT
				path, offset, columns, should_include_row_id,
				pos, end_offset, prev_rowid_max, rowid_max,
				kvc_bytes, kvc_kvs, kvc_checksum
			FROM %s.chunk_v3 WHERE table_name = ?
			ORDER BY path, offset;
		`, cpdb.schema)
		rows, err := tx.QueryContext(c, query, tableName)
		if err != nil {
			return errors.Trace(err)
		}
		defer rows.Close()
		for rows.Next() {
			var (
				value       = new(ChunkCheckpoint)
				kvcBytes    uint64
				kvcKVs      uint64
				kvcChecksum uint64
			)
			if err := rows.Scan(
				&value.Key.Path, &value.Key.Offset, &value.Columns, &value.ShouldIncludeRowID,
				&value.Chunk.Offset, &value.Chunk.EndOffset, &value.Chunk.PrevRowIDMax, &value.Chunk.RowIDMax,
				&kvcBytes, &kvcKVs, &kvcChecksum,
			); err != nil {
				return errors.Trace(err)
			}
			value.Checksum = verify.MakeKVChecksum(kvcBytes, kvcKVs, kvcChecksum)
			cp.Chunks = append(cp.Chunks, value)
		}
		if err := rows.Err(); err != nil {
			return errors.Trace(err)
		}

		query = fmt.Sprintf(`
			SELECT status, engine, alloc_base FROM %s.table_v1 WHERE table_name = ?
		`, cpdb.schema)
		row := tx.QueryRowContext(c, query, tableName)

		var (
			status uint8
			engine []byte
		)
		if err := row.Scan(&status, &engine, &cp.AllocBase); err != nil {
			cp.resetChunks()
			return errors.Trace(err)
		}
		cp.Engine, err = uuid.FromBytes(engine)
		if err != nil {
			cp.resetChunks()
			return errors.Trace(err)
		}
		cp.Status = CheckpointStatus(status)
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	if cp.Status <= CheckpointStatusMaxInvalid {
		return nil, errors.Errorf("Checkpoint for %s has invalid status: %d", tableName, cp.Status)
	}

	return cp, nil
}

func (cpdb *MySQLCheckpointsDB) InsertChunkCheckpoints(ctx context.Context, tableName string, checkpoints []*ChunkCheckpoint) error {
	err := common.TransactWithRetry(ctx, cpdb.db, "(update chunk checkpoints for "+tableName+")", func(c context.Context, tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(c, fmt.Sprintf(`
			REPLACE INTO %s.chunk_v3 (
				table_name, path, offset, columns, should_include_row_id,
				pos, end_offset, prev_rowid_max, rowid_max,
				kvc_bytes, kvc_kvs, kvc_checksum
			) VALUES (
				?, ?, ?, ?, ?,
				?, ?, ?, ?,
				?, ?, ?
			);
		`, cpdb.schema))
		if err != nil {
			return errors.Trace(err)
		}
		defer stmt.Close()

		for _, value := range checkpoints {
			_, err = stmt.ExecContext(
				c,
				tableName, value.Key.Path, value.Key.Offset, value.Columns, value.ShouldIncludeRowID,
				value.Chunk.Offset, value.Chunk.EndOffset, value.Chunk.PrevRowIDMax, value.Chunk.RowIDMax,
				value.Checksum.SumSize(), value.Checksum.SumKVS(), value.Checksum.Sum(),
			)
			if err != nil {
				return errors.Trace(err)
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
		UPDATE %s.chunk_v3 SET pos = ?, prev_rowid_max = ?, kvc_bytes = ?, kvc_kvs = ?, kvc_checksum = ?
		WHERE table_name = ? AND path = ? AND offset = ?;
	`, cpdb.schema)
	checksumQuery := fmt.Sprintf(`
		UPDATE %s.table_v1 SET alloc_base = GREATEST(?, alloc_base) WHERE table_name = ?;
	`, cpdb.schema)
	statusQuery := fmt.Sprintf(`
		UPDATE %s.table_v1 SET status = ? WHERE table_name = ?;
	`, cpdb.schema)

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
		statusStmt, e := tx.PrepareContext(c, statusQuery)
		if e != nil {
			return errors.Trace(e)
		}
		defer statusStmt.Close()

		for tableName, cpd := range checkpointDiffs {
			if cpd.hasStatus {
				if _, e := statusStmt.ExecContext(c, cpd.status, tableName); e != nil {
					return errors.Trace(e)
				}
			}
			if cpd.hasChunks {
				if _, e := checksumStmt.ExecContext(c, cpd.allocBase, tableName); e != nil {
					return errors.Trace(e)
				}
			}
			for key, diff := range cpd.chunks {
				if _, e := chunkStmt.ExecContext(
					c,
					diff.pos, diff.rowID, diff.checksum.SumSize(), diff.checksum.SumKVS(), diff.checksum.Sum(),
					tableName, key.Path, key.Offset,
				); e != nil {
					return errors.Trace(e)
				}
			}
		}

		return nil
	})
	if err != nil {
		common.AppLogger.Errorf("failed to save checkpoint: %v", err)
	}
}

// Management functions ----------------------------------------------------------------------------

var cannotManageNullDB = errors.NotSupportedf("checkpoints is disabled")

func (*NullCheckpointsDB) RemoveCheckpoint(context.Context, string) error {
	return errors.Trace(cannotManageNullDB)
}
func (*NullCheckpointsDB) IgnoreErrorCheckpoint(context.Context, string) error {
	return errors.Trace(cannotManageNullDB)
}
func (*NullCheckpointsDB) DestroyErrorCheckpoint(context.Context, string, *TiDBManager) error {
	return errors.Trace(cannotManageNullDB)
}
func (*NullCheckpointsDB) DumpTables(context.Context, io.Writer) error {
	return errors.Trace(cannotManageNullDB)
}
func (*NullCheckpointsDB) DumpChunks(context.Context, io.Writer) error {
	return errors.Trace(cannotManageNullDB)
}

func (cpdb *MySQLCheckpointsDB) RemoveCheckpoint(ctx context.Context, tableName string) error {
	var (
		deleteChunkFmt string
		deleteTableFmt string
		arg            interface{}
	)

	if tableName == "all" {
		deleteChunkFmt = "DELETE FROM %[1]s.chunk_v3 WHERE table_name IN (SELECT table_name FROM %[1]s.table_v1 WHERE node_id = ?)"
		deleteTableFmt = "DELETE FROM %s.table_v1 WHERE node_id = ?"
		arg = nodeID
	} else {
		deleteChunkFmt = "DELETE FROM %s.chunk_v3 WHERE table_name = ?"
		deleteTableFmt = "DELETE FROM %s.table_v1 WHERE table_name = ?"
		arg = tableName
	}

	deleteChunkQuery := fmt.Sprintf(deleteChunkFmt, cpdb.schema)
	deleteTableQuery := fmt.Sprintf(deleteTableFmt, cpdb.schema)
	err := common.TransactWithRetry(ctx, cpdb.db, fmt.Sprintf("(remove checkpoints of %s)", tableName), func(c context.Context, tx *sql.Tx) error {
		if _, e := tx.ExecContext(c, deleteChunkQuery, arg); e != nil {
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
	query := fmt.Sprintf(`
		UPDATE %s.table_v1 SET status = %d WHERE %s = ? AND status <= %d;
	`, cpdb.schema, CheckpointStatusLoaded, colName, CheckpointStatusMaxInvalid)

	err := common.ExecWithRetry(ctx, cpdb.db, fmt.Sprintf("(ignore error checkpoints for %s)", tableName), query, arg)
	return errors.Trace(err)
}

func (cpdb *MySQLCheckpointsDB) DestroyErrorCheckpoint(ctx context.Context, tableName string, target *TiDBManager) error {
	targetTables, err := cpdb.destroyErrorCheckpoints(ctx, tableName)
	if err != nil {
		return errors.Trace(err)
	}

	common.AppLogger.Info("Going to drop the following tables:", targetTables)
	for _, tableName := range targetTables {
		query := "DROP TABLE " + tableName
		err := common.ExecWithRetry(ctx, target.db, query, query)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (cpdb *MySQLCheckpointsDB) destroyErrorCheckpoints(ctx context.Context, tableName string) ([]string, error) {
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
		SELECT table_name FROM %s.table_v1 WHERE %s = ? AND status <= %d;
	`, cpdb.schema, conditionColumn, CheckpointStatusMaxInvalid)
	deleteChunkQuery := fmt.Sprintf(`
		DELETE FROM %[1]s.chunk_v3 WHERE table_name IN (SELECT table_name FROM %[1]s.table_v1 WHERE %[2]s = ? AND status <= %[3]d)
	`, cpdb.schema, conditionColumn, CheckpointStatusMaxInvalid)
	deleteTableQuery := fmt.Sprintf(`
		DELETE FROM %s.table_v1 WHERE %s = ? AND status <= %d
	`, cpdb.schema, conditionColumn, CheckpointStatusMaxInvalid)

	var targetTables []string

	err := common.TransactWithRetry(ctx, cpdb.db, fmt.Sprintf("(destroy error checkpoints for %s)", tableName), func(c context.Context, tx *sql.Tx) error {
		// Obtain the list of tables
		targetTables = nil
		rows, e := tx.QueryContext(c, selectQuery, arg)
		if e != nil {
			return errors.Trace(e)
		}
		defer rows.Close()
		for rows.Next() {
			var matchedTableName string
			if e := rows.Scan(&matchedTableName); e != nil {
				return errors.Trace(e)
			}
			targetTables = append(targetTables, matchedTableName)
		}
		if e := rows.Err(); e != nil {
			return errors.Trace(e)
		}

		// Delete the checkpoints
		if _, e := tx.ExecContext(c, deleteChunkQuery, arg); e != nil {
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
			hex(engine) AS engine,
			status,
			alloc_base,
			create_time,
			update_time
		FROM %s.table_v1;
	`, cpdb.schema))
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
		FROM %s.chunk_v3;
	`, cpdb.schema))
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	return errors.Trace(sqltocsv.Write(writer, rows))
}
