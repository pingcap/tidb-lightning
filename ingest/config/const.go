package config

const (
	_K = int64(1 << 10)
	_M = _K << 10
	_G = _M << 10

	// mydumper
	// ReadBlockSize int64 = 32 * _K
	// MinRegionSize int64 = 256 * _M

	// // kv-deliver
	MaxFlushSize int64 = 200 * _G
)
