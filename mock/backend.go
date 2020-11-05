// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/pingcap/tidb-lightning/lightning/backend (interfaces: AbstractBackend,Encoder,Rows,Row)

// $ mockgen -package mock -mock_names 'AbstractBackend=MockBackend' github.com/pingcap/tidb-lightning/lightning/backend AbstractBackend,Encoder,Rows,Row

// Package mock is a generated GoMock package.
package mock

import (
	context "context"
	reflect "reflect"
	time "time"

	gomock "github.com/golang/mock/gomock"
	model "github.com/pingcap/parser/model"
	table "github.com/pingcap/tidb/table"
	types "github.com/pingcap/tidb/types"
	"github.com/google/uuid"

	backend "github.com/pingcap/tidb-lightning/lightning/backend"
	log "github.com/pingcap/tidb-lightning/lightning/log"
	verification "github.com/pingcap/tidb-lightning/lightning/verification"
)

// MockBackend is a mock of AbstractBackend interface
type MockBackend struct {
	ctrl     *gomock.Controller
	recorder *MockBackendMockRecorder
}

// MockBackendMockRecorder is the mock recorder for MockBackend
type MockBackendMockRecorder struct {
	mock *MockBackend
}

// NewMockBackend creates a new mock instance
func NewMockBackend(ctrl *gomock.Controller) *MockBackend {
	mock := &MockBackend{ctrl: ctrl}
	mock.recorder = &MockBackendMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockBackend) EXPECT() *MockBackendMockRecorder {
	return m.recorder
}

// CheckRequirements mocks base method
func (m *MockBackend) CheckRequirements() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CheckRequirements")
	ret0, _ := ret[0].(error)
	return ret0
}

// CheckRequirements indicates an expected call of CheckRequirements
func (mr *MockBackendMockRecorder) CheckRequirements() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CheckRequirements", reflect.TypeOf((*MockBackend)(nil).CheckRequirements))
}

// CleanupEngine mocks base method
func (m *MockBackend) CleanupEngine(arg0 context.Context, arg1 uuid.UUID) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CleanupEngine", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// CleanupEngine indicates an expected call of CleanupEngine
func (mr *MockBackendMockRecorder) CleanupEngine(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CleanupEngine", reflect.TypeOf((*MockBackend)(nil).CleanupEngine), arg0, arg1)
}

// Close mocks base method
func (m *MockBackend) Close() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Close")
}

// Close indicates an expected call of Close
func (mr *MockBackendMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockBackend)(nil).Close))
}

// CloseEngine mocks base method
func (m *MockBackend) CloseEngine(arg0 context.Context, arg1 uuid.UUID) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CloseEngine", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// CloseEngine indicates an expected call of CloseEngine
func (mr *MockBackendMockRecorder) CloseEngine(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CloseEngine", reflect.TypeOf((*MockBackend)(nil).CloseEngine), arg0, arg1)
}

// FetchRemoteTableModels mocks base method
func (m *MockBackend) FetchRemoteTableModels(arg0 context.Context, arg1 string) ([]*model.TableInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FetchRemoteTableModels", arg0, arg1)
	ret0, _ := ret[0].([]*model.TableInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FetchRemoteTableModels indicates an expected call of FetchRemoteTableModels
func (mr *MockBackendMockRecorder) FetchRemoteTableModels(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FetchRemoteTableModels", reflect.TypeOf((*MockBackend)(nil).FetchRemoteTableModels), arg0, arg1)
}

// ImportEngine mocks base method
func (m *MockBackend) ImportEngine(arg0 context.Context, arg1 uuid.UUID) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ImportEngine", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// ImportEngine indicates an expected call of ImportEngine
func (mr *MockBackendMockRecorder) ImportEngine(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ImportEngine", reflect.TypeOf((*MockBackend)(nil).ImportEngine), arg0, arg1)
}

// MakeEmptyRows mocks base method
func (m *MockBackend) MakeEmptyRows() backend.Rows {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "MakeEmptyRows")
	ret0, _ := ret[0].(backend.Rows)
	return ret0
}

// MakeEmptyRows indicates an expected call of MakeEmptyRows
func (mr *MockBackendMockRecorder) MakeEmptyRows() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MakeEmptyRows", reflect.TypeOf((*MockBackend)(nil).MakeEmptyRows))
}

// MaxChunkSize mocks base method
func (m *MockBackend) MaxChunkSize() int {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "MaxChunkSize")
	ret0, _ := ret[0].(int)
	return ret0
}

// MaxChunkSize indicates an expected call of MaxChunkSize
func (mr *MockBackendMockRecorder) MaxChunkSize() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MaxChunkSize", reflect.TypeOf((*MockBackend)(nil).MaxChunkSize))
}

// NewEncoder mocks base method
func (m *MockBackend) NewEncoder(arg0 table.Table, arg1 *backend.SessionOptions) backend.Encoder {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewEncoder", arg0, arg1)
	ret0, _ := ret[0].(backend.Encoder)
	return ret0
}

// NewEncoder indicates an expected call of NewEncoder
func (mr *MockBackendMockRecorder) NewEncoder(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewEncoder", reflect.TypeOf((*MockBackend)(nil).NewEncoder), arg0, arg1)
}

// OpenEngine mocks base method
func (m *MockBackend) OpenEngine(arg0 context.Context, arg1 uuid.UUID) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OpenEngine", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// OpenEngine indicates an expected call of OpenEngine
func (mr *MockBackendMockRecorder) OpenEngine(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OpenEngine", reflect.TypeOf((*MockBackend)(nil).OpenEngine), arg0, arg1)
}

// RetryImportDelay mocks base method
func (m *MockBackend) RetryImportDelay() time.Duration {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RetryImportDelay")
	ret0, _ := ret[0].(time.Duration)
	return ret0
}

// RetryImportDelay indicates an expected call of RetryImportDelay
func (mr *MockBackendMockRecorder) RetryImportDelay() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RetryImportDelay", reflect.TypeOf((*MockBackend)(nil).RetryImportDelay))
}

// ShouldPostProcess mocks base method
func (m *MockBackend) ShouldPostProcess() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ShouldPostProcess")
	ret0, _ := ret[0].(bool)
	return ret0
}

// ShouldPostProcess indicates an expected call of ShouldPostProcess
func (mr *MockBackendMockRecorder) ShouldPostProcess() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ShouldPostProcess", reflect.TypeOf((*MockBackend)(nil).ShouldPostProcess))
}

// WriteRows mocks base method
func (m *MockBackend) WriteRows(arg0 context.Context, arg1 uuid.UUID, arg2 string, arg3 []string, arg4 uint64, arg5 backend.Rows) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WriteRows", arg0, arg1, arg2, arg3, arg4, arg5)
	ret0, _ := ret[0].(error)
	return ret0
}

// WriteRows indicates an expected call of WriteRows
func (mr *MockBackendMockRecorder) WriteRows(arg0, arg1, arg2, arg3, arg4, arg5 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WriteRows", reflect.TypeOf((*MockBackend)(nil).WriteRows), arg0, arg1, arg2, arg3, arg4, arg5)
}

// MockEncoder is a mock of Encoder interface
type MockEncoder struct {
	ctrl     *gomock.Controller
	recorder *MockEncoderMockRecorder
}

// MockEncoderMockRecorder is the mock recorder for MockEncoder
type MockEncoderMockRecorder struct {
	mock *MockEncoder
}

// NewMockEncoder creates a new mock instance
func NewMockEncoder(ctrl *gomock.Controller) *MockEncoder {
	mock := &MockEncoder{ctrl: ctrl}
	mock.recorder = &MockEncoderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockEncoder) EXPECT() *MockEncoderMockRecorder {
	return m.recorder
}

// Close mocks base method
func (m *MockEncoder) Close() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Close")
}

// Close indicates an expected call of Close
func (mr *MockEncoderMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockEncoder)(nil).Close))
}

// Encode mocks base method
func (m *MockEncoder) Encode(arg0 log.Logger, arg1 []types.Datum, arg2 int64, arg3 []int) (backend.Row, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Encode", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(backend.Row)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Encode indicates an expected call of Encode
func (mr *MockEncoderMockRecorder) Encode(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Encode", reflect.TypeOf((*MockEncoder)(nil).Encode), arg0, arg1, arg2, arg3)
}

// MockRows is a mock of Rows interface
type MockRows struct {
	ctrl     *gomock.Controller
	recorder *MockRowsMockRecorder
}

// MockRowsMockRecorder is the mock recorder for MockRows
type MockRowsMockRecorder struct {
	mock *MockRows
}

// NewMockRows creates a new mock instance
func NewMockRows(ctrl *gomock.Controller) *MockRows {
	mock := &MockRows{ctrl: ctrl}
	mock.recorder = &MockRowsMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockRows) EXPECT() *MockRowsMockRecorder {
	return m.recorder
}

// Clear mocks base method
func (m *MockRows) Clear() backend.Rows {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Clear")
	ret0, _ := ret[0].(backend.Rows)
	return ret0
}

// Clear indicates an expected call of Clear
func (mr *MockRowsMockRecorder) Clear() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Clear", reflect.TypeOf((*MockRows)(nil).Clear))
}

// SplitIntoChunks mocks base method
func (m *MockRows) SplitIntoChunks(arg0 int) []backend.Rows {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SplitIntoChunks", arg0)
	ret0, _ := ret[0].([]backend.Rows)
	return ret0
}

// SplitIntoChunks indicates an expected call of SplitIntoChunks
func (mr *MockRowsMockRecorder) SplitIntoChunks(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SplitIntoChunks", reflect.TypeOf((*MockRows)(nil).SplitIntoChunks), arg0)
}

// MockRow is a mock of Row interface
type MockRow struct {
	ctrl     *gomock.Controller
	recorder *MockRowMockRecorder
}

// MockRowMockRecorder is the mock recorder for MockRow
type MockRowMockRecorder struct {
	mock *MockRow
}

// NewMockRow creates a new mock instance
func NewMockRow(ctrl *gomock.Controller) *MockRow {
	mock := &MockRow{ctrl: ctrl}
	mock.recorder = &MockRowMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockRow) EXPECT() *MockRowMockRecorder {
	return m.recorder
}

// ClassifyAndAppend mocks base method
func (m *MockRow) ClassifyAndAppend(arg0 *backend.Rows, arg1 *verification.KVChecksum, arg2 *backend.Rows, arg3 *verification.KVChecksum) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "ClassifyAndAppend", arg0, arg1, arg2, arg3)
}

// ClassifyAndAppend indicates an expected call of ClassifyAndAppend
func (mr *MockRowMockRecorder) ClassifyAndAppend(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ClassifyAndAppend", reflect.TypeOf((*MockRow)(nil).ClassifyAndAppend), arg0, arg1, arg2, arg3)
}
