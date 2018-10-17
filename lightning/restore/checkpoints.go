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

type TableCheckpoint struct {
	Status    CheckpointStatus
	Engine    uuid.UUID
	AllocBase int64
	Checksum  verify.KVChecksum
	chunks    map[chunkCheckpoint]int64
}

func (cp *TableCheckpoint) resetChunks() {
	cp.chunks = make(map[chunkCheckpoint]int64)
}

func (cp *TableCheckpoint) ChunkPos(path string, offset int64) (int64, bool) {
	pos, ok := cp.chunks[chunkCheckpoint{path: path, offset: offset}]
	return pos, ok
}

type chunkCheckpoint struct {
	path   string
	offset int64
}

type TableCheckpointDiff struct {
	hasStatus   bool
	hasChecksum bool
	status      CheckpointStatus
	allocBase   int64
	checksum    verify.KVChecksum
	chunks      map[chunkCheckpoint]int64
}

func NewTableCheckpointDiff() *TableCheckpointDiff {
	return &TableCheckpointDiff{
		status: CheckpointStatusMaxInvalid + 1,
		chunks: make(map[chunkCheckpoint]int64),
	}
}

func (cpd *TableCheckpointDiff) String() string {
	return fmt.Sprintf("{hasStatus:%v, hasChecksum:%v, status:%d, allocBase:%d, checksum:%v, chunks:[%d]}", cpd.hasStatus, cpd.hasChecksum, cpd.status, cpd.allocBase, cpd.checksum, len(cpd.chunks))
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
	AllocBase int64
	Checksum  verify.KVChecksum
	Path      string
	Offset    int64
	Pos       int64
}

func (merger *ChunkCheckpointMerger) MergeInto(cpd *TableCheckpointDiff) {
	cpd.hasChecksum = true
	cpd.allocBase = merger.AllocBase
	cpd.checksum = merger.Checksum
	chcp := chunkCheckpoint{path: merger.Path, offset: merger.Offset}
	cpd.chunks[chcp] = merger.Pos
}

type CheckpointsDB interface {
	Initialize(ctx context.Context, dbInfo map[string]*TidbDBInfo) error
	Get(ctx context.Context, tableName string) (*TableCheckpoint, error)
	Close() error
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
		chunks: make(map[chunkCheckpoint]int64),
	}, nil
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
			kvc_bytes bigint unsigned NOT NULL DEFAULT 0,
			kvc_kvs bigint unsigned NOT NULL DEFAULT 0,
			kvc_checksum bigint unsigned NOT NULL DEFAULT 0,
			create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			INDEX(node_id, session)
		);
		CREATE TABLE IF NOT EXISTS %[1]s.chunk_v2 (
			table_name varchar(261) NOT NULL,
			path varchar(2048) NOT NULL,
			offset bigint NOT NULL,
			pos bigint NOT NULL,
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
		query := fmt.Sprintf(`SELECT path, offset, pos FROM %s.chunk_v2 WHERE table_name = ?`, cpdb.schema)
		rows, err := tx.QueryContext(c, query, tableName)
		if err != nil {
			return errors.Trace(err)
		}
		defer rows.Close()
		for rows.Next() {
			var (
				ccp chunkCheckpoint
				pos int64
			)
			if err := rows.Scan(&ccp.path, &ccp.offset, &pos); err != nil {
				return errors.Trace(err)
			}
			cp.chunks[ccp] = pos
		}
		if err := rows.Err(); err != nil {
			return errors.Trace(err)
		}

		query = fmt.Sprintf(`
			SELECT status, engine, alloc_base, kvc_bytes, kvc_kvs, kvc_checksum
			FROM %s.table_v1 WHERE table_name = ?
		`, cpdb.schema)
		row := tx.QueryRowContext(c, query, tableName)

		var (
			status      uint8
			engine      []byte
			kvcBytes    uint64
			kvcKVs      uint64
			kvcChecksum uint64
		)
		if err := row.Scan(&status, &engine, &cp.AllocBase, &kvcBytes, &kvcKVs, &kvcChecksum); err != nil {
			cp.resetChunks()
			return errors.Trace(err)
		}
		cp.Engine, err = uuid.FromBytes(engine)
		if err != nil {
			cp.resetChunks()
			return errors.Trace(err)
		}
		cp.Status = CheckpointStatus(status)
		cp.Checksum = verify.MakeKVChecksum(kvcBytes, kvcKVs, kvcChecksum)
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

func (cpdb *MySQLCheckpointsDB) Update(checkpointDiffs map[string]*TableCheckpointDiff) {
	chunkQuery := fmt.Sprintf(`
		REPLACE INTO %s.chunk_v2 (table_name, path, offset, pos) VALUES (?, ?, ?, ?);
	`, cpdb.schema)
	checksumQuery := fmt.Sprintf(`
		UPDATE %s.table_v1 SET alloc_base = ?, kvc_bytes = ?, kvc_kvs = ?, kvc_checksum = ? WHERE table_name = ?;
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
			if cpd.hasChecksum {
				if _, e := checksumStmt.ExecContext(c, cpd.allocBase, cpd.checksum.SumSize(), cpd.checksum.SumKVS(), cpd.checksum.Sum(), tableName); e != nil {
					return errors.Trace(e)
				}
			}
			for chcp, pos := range cpd.chunks {
				if _, e := chunkStmt.ExecContext(c, tableName, chcp.path, chcp.offset, pos); e != nil {
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
		deleteChunkFmt = "DELETE FROM %[1]s.chunk_v2 WHERE table_name IN (SELECT table_name FROM %[1]s.table_v1 WHERE node_id = ?)"
		deleteTableFmt = "DELETE FROM %s.table_v1 WHERE node_id = ?"
		arg = nodeID
	} else {
		deleteChunkFmt = "DELETE FROM %s.chunk_v2 WHERE table_name = ?"
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
		DELETE FROM %[1]s.chunk_v2 WHERE table_name IN (SELECT table_name FROM %[1]s.table_v1 WHERE %[2]s = ? AND status <= %[3]d)
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
			kvc_bytes,
			kvc_kvs,
			kvc_checksum,
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
			pos
		FROM %s.chunk_v2;
	`, cpdb.schema))
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	return errors.Trace(sqltocsv.Write(writer, rows))
}
