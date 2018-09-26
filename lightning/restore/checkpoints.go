package restore

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/satori/go.uuid"
	"golang.org/x/net/context"

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
	chunks    map[chunkCheckpoint]struct{}
}

func (cp *TableCheckpoint) resetChunks() {
	cp.chunks = make(map[chunkCheckpoint]struct{})
}

func (cp *TableCheckpoint) HasChunk(path string, offset int64) bool {
	_, ok := cp.chunks[chunkCheckpoint{path: path, offset: offset}]
	return ok
}

type chunkCheckpoint struct {
	path   string
	offset int64
}

type InvalidatedEngine struct {
	TableName string
	Engine    uuid.UUID
}

type CheckpointsDB interface {
	InvalidateNonUniqueTables(ctx context.Context, tableNames []string) ([]InvalidatedEngine, error)
	Load(ctx context.Context, dbInfo map[string]*TidbDBInfo) error
	Get(ctx context.Context, tableName string) (*TableCheckpoint, error)
	Close() error
	UpdateChunk(
		tableName string,
		allocBase int64,
		checksum verify.KVChecksum,
		path string,
		offset int64,
	)
	UpdateStatus(
		tableName string,
		status CheckpointStatus,
	)
	UpdateFailure(
		tableName string,
	)
}

// NullCheckpointsDB is a checkpoints database with no checkpoints.
type NullCheckpointsDB struct{}

func NewNullCheckpointsDB() *NullCheckpointsDB {
	return &NullCheckpointsDB{}
}

func (*NullCheckpointsDB) InvalidateNonUniqueTables(context.Context, []string) ([]InvalidatedEngine, error) {
	return nil, nil
}
func (*NullCheckpointsDB) Load(context.Context, map[string]*TidbDBInfo) error {
	return nil
}
func (*NullCheckpointsDB) Close() error {
	return nil
}

func (*NullCheckpointsDB) Get(_ context.Context, tableName string) (*TableCheckpoint, error) {
	return &TableCheckpoint{
		Status: CheckpointStatusLoaded,
		Engine: uuid.NewV4(),
		chunks: make(map[chunkCheckpoint]struct{}),
	}, nil
}

func (*NullCheckpointsDB) UpdateChunk(string, int64, verify.KVChecksum, string, int64) {
}
func (*NullCheckpointsDB) UpdateStatus(string, CheckpointStatus) {
}
func (*NullCheckpointsDB) UpdateFailure(string) {
}

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
		CREATE TABLE IF NOT EXISTS %[1]s.chunk_v1 (
			table_name varchar(261) NOT NULL,
			path varchar(2048) NOT NULL,
			offset bigint NOT NULL,
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

func (cpdb *MySQLCheckpointsDB) InvalidateNonUniqueTables(ctx context.Context, tableNames []string) ([]InvalidatedEngine, error) {
	if len(tableNames) == 0 {
		return nil, nil
	}

	// Let's hope the user is sane and won't create >65535 tables without unique columns :|
	selectPlaceholders := strings.Repeat(",?", len(tableNames))
	selectPlaceholders = selectPlaceholders[1:]

	selectQuery := fmt.Sprintf(`
		SELECT table_name, engine FROM %s.table_v1 WHERE status < %d AND table_name IN (%s)
	`, cpdb.schema, CheckpointStatusAllWritten, selectPlaceholders)

	selectArgs := make([]interface{}, len(tableNames))
	engines := make([]InvalidatedEngine, 0, len(tableNames))
	for i, tableName := range tableNames {
		selectArgs[i] = tableName
	}

	err := common.TransactWithRetry(ctx, cpdb.db, "(invalidate non-unique partially written engines)", func(c context.Context, tx *sql.Tx) error {
		// Find all tables with status < 60
		rows, err := tx.QueryContext(c, selectQuery, selectArgs...)
		if err != nil {
			return errors.Trace(err)
		}

		deleteArgs := make([]interface{}, 0, len(tableNames))
		for rows.Next() {
			var tableName string
			var rawEngine []byte
			if err := rows.Scan(&tableName, &rawEngine); err != nil {
				return errors.Trace(err)
			}
			engines = append(engines, InvalidatedEngine{
				TableName: tableName,
				Engine:    uuid.FromBytesOrNil(rawEngine),
			})
			deleteArgs = append(deleteArgs, tableName)
		}

		// No bad tables \o/
		if len(deleteArgs) == 0 {
			return nil
		}

		deletePlaceholders := strings.Repeat(",?", len(deleteArgs))
		deletePlaceholders = deletePlaceholders[1:]

		// Invalidate those checkpoints
		_, err = tx.ExecContext(c, fmt.Sprintf(`
			DELETE FROM %s.chunk_v1 WHERE table_name IN (%s)
		`, cpdb.schema, deletePlaceholders), deleteArgs...)
		if err != nil {
			return errors.Trace(err)
		}
		_, err = tx.ExecContext(c, fmt.Sprintf(`
			DELETE FROM %s.table_v1 WHERE table_name IN (%s)
		`, cpdb.schema, deletePlaceholders), deleteArgs...)
		if err != nil {
			return errors.Trace(err)
		}

		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return engines, nil
}

func (cpdb *MySQLCheckpointsDB) Load(ctx context.Context, dbInfo map[string]*TidbDBInfo) error {
	// We can have at most 65535 placeholders https://stackoverflow.com/q/4922345/
	// Since this step is not performance critical, we just insert the rows one-by-one.

	err := common.TransactWithRetry(ctx, cpdb.db, "(insert checkpoints)", func(c context.Context, tx *sql.Tx) error {
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
		query := fmt.Sprintf(`SELECT path, offset FROM %s.chunk_v1 WHERE table_name = ?`, cpdb.schema)
		rows, err := tx.QueryContext(c, query, tableName)
		if err != nil {
			return errors.Trace(err)
		}
		for rows.Next() {
			var ccp chunkCheckpoint
			if err := rows.Scan(&ccp.path, &ccp.offset); err != nil {
				return errors.Trace(err)
			}
			cp.chunks[ccp] = struct{}{}
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

func (cpdb *MySQLCheckpointsDB) UpdateChunk(
	tableName string,
	allocBase int64,
	checksum verify.KVChecksum,
	path string,
	offset int64,
) {
	purpose := fmt.Sprintf("(update chunk checkpoint of %s for %s|%d)", tableName, path, offset)
	insertQuery := fmt.Sprintf(`
		INSERT IGNORE %s.chunk_v1 (table_name, path, offset) VALUES (?, ?, ?);
	`, cpdb.schema)
	updateQuery := fmt.Sprintf(`
		UPDATE %s.table_v1 SET alloc_base = ?, kvc_bytes = ?, kvc_kvs = ?, kvc_checksum = ? WHERE table_name = ?;
	`, cpdb.schema)

	err := common.TransactWithRetry(context.Background(), cpdb.db, purpose, func(c context.Context, tx *sql.Tx) error {
		if _, e := tx.ExecContext(c, insertQuery, tableName, path, offset); e != nil {
			return errors.Trace(e)
		}
		if _, e := tx.ExecContext(c, updateQuery, allocBase, checksum.SumSize(), checksum.SumKVS(), checksum.Sum(), tableName); e != nil {
			return errors.Trace(e)
		}
		return nil
	})
	if err != nil {
		common.AppLogger.Warnf("[%s] failed to save chunk checkpoint: %v", tableName, err)
	}
}

func (cpdb *MySQLCheckpointsDB) UpdateStatus(
	tableName string,
	status CheckpointStatus,
) {
	purpose := fmt.Sprintf("(update status checkpoint of %s)", tableName)
	query := fmt.Sprintf(`
		UPDATE %s.table_v1 SET status = ? WHERE table_name = ?;
	`, cpdb.schema)
	err := common.ExecWithRetry(context.Background(), cpdb.db, purpose, query, uint8(status), tableName)
	if err != nil {
		common.AppLogger.Warnf("[%s] failed to save status checkpoint: %v", tableName, err)
	}
}

func (cpdb *MySQLCheckpointsDB) UpdateFailure(tableName string) {
	purpose := fmt.Sprintf("(update status checkpoint of %s)", tableName)
	query := fmt.Sprintf(`
		UPDATE %s.table_v1 SET status = status / 10 WHERE table_name = ? AND status > 25;
	`, cpdb.schema)
	err := common.ExecWithRetry(context.Background(), cpdb.db, purpose, query, tableName)
	if err != nil {
		common.AppLogger.Errorf("[%s] failed to save failure checkpoint: %v", tableName, err)
	}
}
