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

package backend

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/errors"
	kv "github.com/pingcap/kvproto/pkg/import_kvpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/table"
	uuid "github.com/satori/go.uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/log"
)

const (
	defaultRetryBackoffTime = time.Second * 3
)

var (
	requiredTiDBVersion = *semver.New("2.1.0")
	requiredPDVersion   = *semver.New("2.1.0")
	requiredTiKVVersion = *semver.New("2.1.0")
)

// importer represents a gRPC connection to tikv-importer. This type is
// goroutine safe: you can share this instance and execute any method anywhere.
type importer struct {
	conn   *grpc.ClientConn
	cli    kv.ImportKVClient
	pdAddr string
	tls    *common.TLS

	mutationPool sync.Pool
}

// NewImporter creates a new connection to tikv-importer. A single connection
// per tidb-lightning instance is enough.
func NewImporter(ctx context.Context, tls *common.TLS, importServerAddr string, pdAddr string) (Backend, error) {
	conn, err := grpc.DialContext(ctx, importServerAddr, tls.ToGRPCDialOption())
	if err != nil {
		return MakeBackend(nil), errors.Trace(err)
	}

	return MakeBackend(&importer{
		conn:         conn,
		cli:          kv.NewImportKVClient(conn),
		pdAddr:       pdAddr,
		tls:          tls,
		mutationPool: sync.Pool{New: func() interface{} { return &kv.Mutation{} }},
	}), nil
}

// NewMockImporter creates an *unconnected* importer based on a custom
// ImportKVClient. This is provided for testing only. Do not use this function
// outside of tests.
func NewMockImporter(cli kv.ImportKVClient, pdAddr string) Backend {
	return MakeBackend(&importer{
		conn:         nil,
		cli:          cli,
		pdAddr:       pdAddr,
		mutationPool: sync.Pool{New: func() interface{} { return &kv.Mutation{} }},
	})
}

// Close the importer connection.
func (importer *importer) Close() {
	if importer.conn != nil {
		if err := importer.conn.Close(); err != nil {
			log.L().Warn("close importer gRPC connection failed", zap.Error(err))
		}
	}
}

func (*importer) RetryImportDelay() time.Duration {
	return defaultRetryBackoffTime
}

func (*importer) MaxChunkSize() int {
	// 31 MB. hardcoded by importer, so do we
	return 31 << 10
}

func (*importer) ShouldPostProcess() bool {
	return true
}

// isIgnorableOpenCloseEngineError checks if the error from
// OpenEngine/CloseEngine can be safely ignored.
func isIgnorableOpenCloseEngineError(err error) bool {
	// We allow "FileExists" error. This happens when the engine has been opened
	// and closed before. This error typically arise when resuming from a
	// checkpoint with a partially-imported engine.
	//
	// If the error is legit in a no-checkpoints settings, the later WriteEngine
	// API will bail us out to keep us safe.
	return err == nil || strings.Contains(err.Error(), "FileExists")
}

func (importer *importer) OpenEngine(ctx context.Context, engineUUID uuid.UUID) error {
	req := &kv.OpenEngineRequest{
		Uuid: engineUUID.Bytes(),
	}

	_, err := importer.cli.OpenEngine(ctx, req)
	if !isIgnorableOpenCloseEngineError(err) {
		return errors.Trace(err)
	}
	return nil
}

func (importer *importer) CloseEngine(ctx context.Context, engineUUID uuid.UUID) error {
	req := &kv.CloseEngineRequest{
		Uuid: engineUUID.Bytes(),
	}

	_, err := importer.cli.CloseEngine(ctx, req)
	if !isIgnorableOpenCloseEngineError(err) {
		return errors.Trace(err)
	}
	return nil
}

func (importer *importer) ImportEngine(ctx context.Context, engineUUID uuid.UUID) error {
	req := &kv.ImportEngineRequest{
		Uuid:   engineUUID.Bytes(),
		PdAddr: importer.pdAddr,
	}

	_, err := importer.cli.ImportEngine(ctx, req)
	return errors.Trace(err)
}

func (importer *importer) CleanupEngine(ctx context.Context, engineUUID uuid.UUID) error {
	req := &kv.CleanupEngineRequest{
		Uuid: engineUUID.Bytes(),
	}

	_, err := importer.cli.CleanupEngine(ctx, req)
	return errors.Trace(err)
}

func (importer *importer) WriteRows(
	ctx context.Context,
	engineUUID uuid.UUID,
	tableName string,
	columnNames []string,
	ts uint64,
	rows Rows,
) (finalErr error) {
	kvs := rows.(kvPairs)
	if len(kvs) == 0 {
		return nil
	}

	wstream, err := importer.cli.WriteEngine(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	logger := log.With(zap.Stringer("engineUUID", engineUUID))

	defer func() {
		if _, closeErr := wstream.CloseAndRecv(); closeErr != nil {
			if finalErr == nil {
				finalErr = errors.Trace(closeErr)
			} else {
				// just log the close error, we need to propagate the earlier error instead
				logger.Warn("close write stream failed", log.ShortError(closeErr))
			}
		}
	}()

	// Bind uuid for this write request
	req := &kv.WriteEngineRequest{
		Chunk: &kv.WriteEngineRequest_Head{
			Head: &kv.WriteHead{
				Uuid: engineUUID.Bytes(),
			},
		},
	}
	if err := wstream.Send(req); err != nil {
		return errors.Trace(err)
	}

	// Send kv paris as write request content
	mutations := make([]*kv.Mutation, len(kvs))
	for i, pair := range kvs {
		mutations[i] = importer.mutationPool.Get().(*kv.Mutation)
		mutations[i].Op = kv.Mutation_Put
		mutations[i].Key = pair.Key
		mutations[i].Value = pair.Val
	}

	req.Reset()
	req.Chunk = &kv.WriteEngineRequest_Batch{
		Batch: &kv.WriteBatch{
			CommitTs:  ts,
			Mutations: mutations,
		},
	}

	err = wstream.Send(req)
	for _, mutation := range mutations {
		importer.mutationPool.Put(mutation)
	}

	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (*importer) MakeEmptyRows() Rows {
	return kvPairs(nil)
}

func (*importer) NewEncoder(tbl table.Table, options *SessionOptions) Encoder {
	return NewTableKVEncoder(tbl, options)
}

func (importer *importer) CheckRequirements() error {
	if err := checkTiDBVersion(importer.tls, requiredTiDBVersion); err != nil {
		return err
	}
	if err := checkPDVersion(importer.tls, importer.pdAddr, requiredPDVersion); err != nil {
		return err
	}
	if err := checkTiKVVersion(importer.tls, importer.pdAddr, requiredTiKVVersion); err != nil {
		return err
	}
	return nil
}

func extractTiDBVersion(version string) (*semver.Version, error) {
	// version format: "5.7.10-TiDB-v2.1.0-rc.1-7-g38c939f"
	//                               ^~~~~~~~~^ we only want this part
	// version format: "5.7.10-TiDB-v2.0.4-1-g06a0bf5"
	//                               ^~~~^
	// version format: "5.7.10-TiDB-v2.0.7"
	//                               ^~~~^
	// version format: "5.7.25-TiDB-v3.0.0-beta-211-g09beefbe0-dirty"
	//                               ^~~~~~~~~^
	// The version is generated by `git describe --tags` on the TiDB repository.
	versions := strings.Split(strings.TrimSuffix(version, "-dirty"), "-")
	end := len(versions)
	switch end {
	case 3, 4:
	case 5, 6:
		end -= 2
	default:
		return nil, errors.Errorf("not a valid TiDB version: %s", version)
	}
	rawVersion := strings.Join(versions[2:end], "-")
	rawVersion = strings.TrimPrefix(rawVersion, "v")
	return semver.NewVersion(rawVersion)
}

func checkTiDBVersion(tls *common.TLS, requiredVersion semver.Version) error {
	var status struct{ Version string }
	err := tls.GetJSON("/status", &status)
	if err != nil {
		return err
	}

	version, err := extractTiDBVersion(status.Version)
	if err != nil {
		return errors.Trace(err)
	}
	return checkVersion("TiDB", requiredVersion, *version)
}

func checkPDVersion(tls *common.TLS, pdAddr string, requiredVersion semver.Version) error {
	var rawVersion string
	err := tls.WithHost(pdAddr).GetJSON("/pd/api/v1/config/cluster-version", &rawVersion)
	if err != nil {
		return err
	}

	version, err := semver.NewVersion(rawVersion)
	if err != nil {
		return errors.Trace(err)
	}

	return checkVersion("PD", requiredVersion, *version)
}

func checkTiKVVersion(tls *common.TLS, pdAddr string, requiredVersion semver.Version) error {
	return ForAllStores(
		context.Background(),
		tls.WithHost(pdAddr),
		StoreStateDown,
		func(c context.Context, store *Store) error {
			component := fmt.Sprintf("TiKV (at %s)", store.Address)
			version, err := semver.NewVersion(strings.TrimPrefix(store.Version, "v"))
			if err != nil {
				return errors.Annotate(err, component)
			}
			return checkVersion(component, requiredVersion, *version)
		},
	)
}

func checkVersion(component string, expected, actual semver.Version) error {
	if actual.Compare(expected) >= 0 {
		return nil
	}
	return errors.Errorf(
		"%s version too old, expected '>=%s', found '%s'",
		component,
		expected,
		actual,
	)
}

func (importer *importer) FetchRemoteTableModels(schema string) ([]*model.TableInfo, error) {
	return fetchRemoteTableModelsFromTLS(importer.tls, schema)
}
