package restore

import (
	"github.com/satori/go.uuid"
	"golang.org/x/net/context"

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

func (cp *TableCheckpoint) HasChunk(path string, offset int64) bool {
	_, ok := cp.chunks[chunkCheckpoint{path: path, offset: offset}]
	return ok
}

type chunkCheckpoint struct {
	path   string
	offset int64
}

type CheckpointsDB interface {
	Load(ctx context.Context, dbInfo map[string]*TidbDBInfo) error
	Get(ctx context.Context, tableName string) (*TableCheckpoint, error)
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

// EmptyCheckpointsDB is a checkpoints database with no checkpoints.
type NullCheckpointsDB struct{}

func NewNullCheckpointsDB() *NullCheckpointsDB {
	return &NullCheckpointsDB{}
}

func (*NullCheckpointsDB) Load(context.Context, map[string]*TidbDBInfo) error {
	// In the real DB implementation, this method should return error immediately if any checkpoint
	// contains an invalid status.
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
