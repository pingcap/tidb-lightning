package kv_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"sync"

	. "github.com/pingcap/check"

	"github.com/pingcap/tidb-lightning/lightning/kv"
)

type tikvSuite struct{}

var _ = Suite(&tikvSuite{})

func (s *tikvSuite) TestForAllStores(c *C) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
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

	serverURL, err := url.Parse(server.URL)
	c.Assert(err, IsNil)

	ctx := context.Background()
	var (
		allStoresLock sync.Mutex
		allStores     []*kv.Store
	)
	err = kv.ForAllStores(ctx, server.Client(), serverURL.Host, kv.StoreStateDown, func(c2 context.Context, store *kv.Store) error {
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
