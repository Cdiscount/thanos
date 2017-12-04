package store

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/shipper"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
)

func TestGCSStore_e2e(t *testing.T) {
	bkt, cleanup := testutil.NewObjectStoreBucket(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir, err := ioutil.TempDir("", "test_gcsstore_e2e")
	testutil.Ok(t, err)
	defer os.RemoveAll(dir)

	series := []labels.Labels{
		labels.FromStrings("a", "1", "b", "1"),
		labels.FromStrings("a", "1", "b", "2"),
		labels.FromStrings("a", "2", "b", "1"),
		labels.FromStrings("a", "2", "b", "2"),
		labels.FromStrings("a", "1", "c", "1"),
		labels.FromStrings("a", "1", "c", "2"),
		labels.FromStrings("a", "2", "c", "1"),
		labels.FromStrings("a", "2", "c", "2"),
	}
	start := time.Now()
	now := start
	remote := shipper.NewGCSRemote(log.NewNopLogger(), nil, bkt.Handle())

	minTime := int64(0)
	maxTime := int64(0)
	for i := 0; i < 3; i++ {
		mint := timestamp.FromTime(now)
		now = now.Add(2 * time.Hour)
		maxt := timestamp.FromTime(now)

		if minTime == 0 {
			minTime = mint
		}
		maxTime = maxt

		// Create two blocks per time slot. Only add 10 samples each so only one chunk
		// gets created each. This way we can easily verify we got 10 chunks per series below.
		id1, err := testutil.CreateBlock(dir, series[:4], 10, mint, maxt)
		testutil.Ok(t, err)
		id2, err := testutil.CreateBlock(dir, series[4:], 10, mint, maxt)
		testutil.Ok(t, err)

		dir1, dir2 := filepath.Join(dir, id1.String()), filepath.Join(dir, id2.String())

		// Add labels to the meta of the second block.
		meta, err := block.ReadMetaFile(dir2)
		testutil.Ok(t, err)
		meta.Thanos.Labels = map[string]string{"ext": "value"}
		testutil.Ok(t, block.WriteMetaFile(dir2, meta))

		// TODO(fabxc): remove the component dependency by factoring out the block interface.
		testutil.Ok(t, remote.Upload(ctx, id1, dir1))
		testutil.Ok(t, remote.Upload(ctx, id2, dir2))

		testutil.Ok(t, os.RemoveAll(dir1))
		testutil.Ok(t, os.RemoveAll(dir2))
	}

	var gossipMinTime, gossipMaxTime int64
	store, err := NewGCSStore(nil, nil, bkt, func(mint int64, maxt int64) {
		gossipMinTime = mint
		gossipMaxTime = maxt
	}, dir)
	testutil.Ok(t, err)

	go func() {
		runutil.Repeat(100*time.Millisecond, ctx.Done(), func() error {
			return store.SyncBlocks(ctx)
		})
	}()

	ctx, _ = context.WithTimeout(ctx, 30*time.Second)

	err = runutil.Retry(100*time.Millisecond, ctx.Done(), func() error {
		if store.numBlocks() < 6 {
			return errors.New("not all blocks loaded")
		}
		return nil
	})
	testutil.Ok(t, err)

	testutil.Equals(t, minTime, gossipMinTime)
	testutil.Equals(t, maxTime, gossipMaxTime)

	vals, err := store.LabelValues(ctx, &storepb.LabelValuesRequest{Label: "a"})
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"1", "2"}, vals.Values)

	pbseries := [][]storepb.Label{
		{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}},
		{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}},
		{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}, {Name: "ext", Value: "value"}},
		{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}, {Name: "ext", Value: "value"}},
		{{Name: "a", Value: "2"}, {Name: "b", Value: "1"}},
		{{Name: "a", Value: "2"}, {Name: "b", Value: "2"}},
		{{Name: "a", Value: "2"}, {Name: "c", Value: "1"}, {Name: "ext", Value: "value"}},
		{{Name: "a", Value: "2"}, {Name: "c", Value: "2"}, {Name: "ext", Value: "value"}},
	}
	srv := &testStoreSeriesServer{ctx: ctx}

	err = store.Series(&storepb.SeriesRequest{
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_RE, Name: "a", Value: "1|2"},
		},
		MinTime: timestamp.FromTime(start),
		MaxTime: timestamp.FromTime(now),
	}, srv)
	testutil.Ok(t, err)
	testutil.Equals(t, len(pbseries), len(srv.series))

	for i, s := range srv.series {
		testutil.Equals(t, pbseries[i], s.Labels)
		testutil.Equals(t, 3, len(s.Chunks))
	}

	pbseries = [][]storepb.Label{
		{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}},
		{{Name: "a", Value: "2"}, {Name: "b", Value: "2"}},
	}
	srv = &testStoreSeriesServer{ctx: ctx}

	err = store.Series(&storepb.SeriesRequest{
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "b", Value: "2"},
		},
		MinTime: timestamp.FromTime(start),
		MaxTime: timestamp.FromTime(now),
	}, srv)
	testutil.Ok(t, err)
	testutil.Equals(t, len(pbseries), len(srv.series))

	for i, s := range srv.series {
		testutil.Equals(t, pbseries[i], s.Labels)
		testutil.Equals(t, 3, len(s.Chunks))
	}

	// Matching by external label should work as well.
	pbseries = [][]storepb.Label{
		{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}, {Name: "ext", Value: "value"}},
		{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}, {Name: "ext", Value: "value"}},
	}
	srv = &testStoreSeriesServer{ctx: ctx}

	err = store.Series(&storepb.SeriesRequest{
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
			{Type: storepb.LabelMatcher_EQ, Name: "ext", Value: "value"},
		},
		MinTime: timestamp.FromTime(start),
		MaxTime: timestamp.FromTime(now),
	}, srv)
	testutil.Ok(t, err)
	testutil.Equals(t, len(pbseries), len(srv.series))

	for i, s := range srv.series {
		testutil.Equals(t, pbseries[i], s.Labels)
		testutil.Equals(t, 3, len(s.Chunks))
	}

	srv = &testStoreSeriesServer{ctx: ctx}
	err = store.Series(&storepb.SeriesRequest{
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
			{Type: storepb.LabelMatcher_EQ, Name: "ext", Value: "wrong-value"},
		},
		MinTime: timestamp.FromTime(start),
		MaxTime: timestamp.FromTime(now),
	}, srv)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(srv.series))
}

func TestGCSBlock_matches(t *testing.T) {
	makeMeta := func(mint, maxt int64, lset map[string]string) *block.Meta {
		return &block.Meta{
			BlockMeta: tsdb.BlockMeta{
				MinTime: mint,
				MaxTime: maxt,
			},
			Thanos: block.ThanosMeta{Labels: lset},
		}
	}
	cases := []struct {
		meta             *block.Meta
		mint, maxt       int64
		matchers         []labels.Matcher
		expBlockMatchers []labels.Matcher
		ok               bool
	}{
		{
			meta: makeMeta(100, 200, nil),
			mint: 0,
			maxt: 99,
			ok:   false,
		},
		{
			meta: makeMeta(100, 200, nil),
			mint: 0,
			maxt: 100,
			ok:   true,
		},
		{
			meta: makeMeta(100, 200, nil),
			mint: 201,
			maxt: 250,
			ok:   false,
		},
		{
			meta: makeMeta(100, 200, nil),
			mint: 200,
			maxt: 250,
			ok:   true,
		},
		{
			meta: makeMeta(100, 200, nil),
			mint: 150,
			maxt: 160,
			matchers: []labels.Matcher{
				labels.NewEqualMatcher("a", "b"),
			},
			expBlockMatchers: []labels.Matcher{
				labels.NewEqualMatcher("a", "b"),
			},
			ok: true,
		},
		{
			meta: makeMeta(100, 200, map[string]string{"a": "b"}),
			mint: 150,
			maxt: 160,
			matchers: []labels.Matcher{
				labels.NewEqualMatcher("a", "b"),
			},
			ok: true,
		},
		{
			meta: makeMeta(100, 200, map[string]string{"a": "b"}),
			mint: 150,
			maxt: 160,
			matchers: []labels.Matcher{
				labels.NewEqualMatcher("a", "c"),
			},
			ok: false,
		},
		{
			meta: makeMeta(100, 200, map[string]string{"a": "b"}),
			mint: 150,
			maxt: 160,
			matchers: []labels.Matcher{
				labels.NewEqualMatcher("a", "b"),
				labels.NewEqualMatcher("d", "e"),
			},
			expBlockMatchers: []labels.Matcher{
				labels.NewEqualMatcher("d", "e"),
			},
			ok: true,
		},
	}

	for i, c := range cases {
		b := &gcsBlock{meta: c.meta}
		blockMatchers, ok := b.blockMatchers(c.mint, c.maxt, c.matchers...)
		testutil.Assert(t, c.ok == ok, "test case %d failed", i)
		testutil.Equals(t, c.expBlockMatchers, blockMatchers)
	}
}

type testStoreSeriesServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	storepb.Store_SeriesServer
	ctx    context.Context
	series []storepb.Series
}

func (s *testStoreSeriesServer) Send(r *storepb.SeriesResponse) error {
	s.series = append(s.series, r.Series)
	return nil
}

func (s *testStoreSeriesServer) Context() context.Context {
	return s.ctx
}