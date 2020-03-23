package backend_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/import_sstpb"

	kv "github.com/pingcap/tidb-lightning/lightning/backend"
	"github.com/pingcap/tidb-lightning/lightning/common"
)

type tikvSuite struct{}

var _ = Suite(&tikvSuite{})

func (s *tikvSuite) TestForAllStores(c *C) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Write([]byte(`
			{
				"count": 5,
				"stores": [
					{
						"store": {
							"id": 1,
							"address": "127.0.0.1:20160",
							"version": "3.0.0-beta.1",
							"state_name": "Up"
						},
						"status": {}
					},
					{
						"store": {
							"id": 2,
							"address": "127.0.0.1:20161",
							"version": "3.0.0-rc.1",
							"state_name": "Down"
						},
						"status": {}
					},
					{
						"store": {
							"id": 3,
							"address": "127.0.0.1:20162",
							"version": "3.0.0-rc.2",
							"state_name": "Disconnected"
						},
						"status": {}
					},
					{
						"store": {
							"id": 4,
							"address": "127.0.0.1:20163",
							"version": "3.0.0",
							"state_name": "Tombstone"
						},
						"status": {}
					},
					{
						"store": {
							"id": 5,
							"address": "127.0.0.1:20164",
							"version": "3.0.1",
							"state_name": "Offline"
						},
						"status": {}
					}
				]
			}
		`))
	}))
	defer server.Close()

	ctx := context.Background()
	var (
		allStoresLock sync.Mutex
		allStores     []*kv.Store
	)
	tls := common.NewTLSFromMockServer(server)
	err := kv.ForAllStores(ctx, tls, kv.StoreStateDown, func(c2 context.Context, store *kv.Store) error {
		allStoresLock.Lock()
		allStores = append(allStores, store)
		allStoresLock.Unlock()
		return nil
	})
	c.Assert(err, IsNil)

	sort.Slice(allStores, func(i, j int) bool { return allStores[i].Address < allStores[j].Address })

	c.Assert(allStores, DeepEquals, []*kv.Store{
		{
			Address: "127.0.0.1:20160",
			Version: "3.0.0-beta.1",
			State:   kv.StoreStateUp,
		},
		{
			Address: "127.0.0.1:20161",
			Version: "3.0.0-rc.1",
			State:   kv.StoreStateDown,
		},
		{
			Address: "127.0.0.1:20162",
			Version: "3.0.0-rc.2",
			State:   kv.StoreStateDisconnected,
		},
		{
			Address: "127.0.0.1:20164",
			Version: "3.0.1",
			State:   kv.StoreStateOffline,
		},
	})
}

func (s *tikvSuite) TestFetchModeFromMetrics(c *C) {
	testCases := []struct {
		metrics string
		mode    import_sstpb.SwitchMode
		isErr   bool
	}{
		{
			metrics: `tikv_config_rocksdb{cf="default",name="hard_pending_compaction_bytes_limit"} 274877906944`,
			mode:    import_sstpb.SwitchMode_Normal,
		},
		{
			metrics: `tikv_config_rocksdb{cf="default",name="hard_pending_compaction_bytes_limit"} 0`,
			mode:    import_sstpb.SwitchMode_Import,
		},
		{
			metrics: ``,
			isErr:   true,
		},
	}

	for _, tc := range testCases {
		comment := Commentf("test case '%s'", tc.metrics)
		mode, err := kv.FetchModeFromMetrics(tc.metrics)
		if tc.isErr {
			c.Assert(err, NotNil, comment)
		} else {
			c.Assert(err, IsNil, comment)
			c.Assert(mode, Equals, tc.mode, comment)
		}
	}
}
