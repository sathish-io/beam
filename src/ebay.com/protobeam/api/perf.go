package api

import (
	"io"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"ebay.com/protobeam/table"
	"ebay.com/protobeam/web"
	"github.com/julienschmidt/httprouter"
)

func (s *Server) txPerf(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	dur, err := time.ParseDuration(r.URL.Query().Get("d"))
	if err != nil {
		web.WriteError(w, http.StatusBadRequest, "Unable to parse duration param 'd': %v", err)
		return
	}
	n, err := strconv.Atoi(r.URL.Query().Get("n"))
	if err != nil {
		web.WriteError(w, http.StatusBadRequest, "Unable to parse concurrency param 'n': %v", err)
		return
	}
	s.metrics.UnregisterAll() // start with a clean set of metrics
	keys, err := s.source.SampleKeys(uint32(100) * uint32(n))
	if err != nil {
		web.WriteError(w, http.StatusInternalServerError, "Unable to fetch starting keys: %v", err)
		return
	}
	end := time.Now().Add(dur)
	keyStart := 0
	res := make(perfOneResults, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		keyEnd := keyStart + 100
		r := &res[i]
		go s.txPerfOne(keys[keyStart:keyEnd], end, r, &wg)
		keyStart = keyEnd
	}
	wg.Wait()
	pt := make([][]string, n+2)
	pt[0] = []string{"n", "Count", "Commits", "Aborts", "Errors", "p25", "p50", "p90", "p99"}
	for idx, r := range res {
		pt[idx+1] = r.results(strconv.Itoa(idx))
	}
	pt[len(pt)-1] = res.totals().results("Totals")

	w.Header().Set("Content-Type", "text/plain")
	table.PrettyPrint(w, pt, true, true)
	io.WriteString(w, "\n\n")

	table.PrettyPrint(w, table.MetricsTable(s.metrics, time.Millisecond), true, false)
}

type perfOneResults []perfOneResult

func (p perfOneResults) totals() *perfOneResult {
	t := perfOneResult{}
	for _, x := range p {
		t.commits += x.commits
		t.aborts += x.aborts
		t.errors += x.errors
		t.times = append(t.times, x.times...)
	}
	return &t
}

type perfOneResult struct {
	commits int
	aborts  int
	errors  int
	times   []time.Duration
}

func (r *perfOneResult) init() {
	r.commits = 0
	r.aborts = 0
	r.errors = 0
	r.times = make([]time.Duration, 0, 1024)
}

func (r *perfOneResult) results(label string) []string {
	sort.Slice(r.times, func(a, b int) bool {
		return r.times[a] < r.times[b]
	})
	return []string{
		label,
		strconv.Itoa(len(r.times)),
		strconv.Itoa(r.commits),
		strconv.Itoa(r.aborts),
		strconv.Itoa(r.errors),
		r.times[len(r.times)/4].String(),      // p25
		r.times[len(r.times)/2].String(),      // p50
		r.times[len(r.times)*9/10].String(),   // p90
		r.times[len(r.times)*99/100].String(), // p99
	}
}

func (r *perfOneResult) consume(dur time.Duration, commited bool, err error) {
	if err != nil {
		r.errors++
	} else if commited {
		r.commits++
	} else {
		r.aborts++
	}
	r.times = append(r.times, dur)
}

func (s *Server) txPerfOne(keys []string, until time.Time, res *perfOneResult, wg *sync.WaitGroup) {
	defer wg.Done()
	keyIdx := 0
	for time.Now().Before(until) {
		st := time.Now()
		commited, _, err := s.concatTx(keys[keyIdx], keys[keyIdx+1], keys[keyIdx+2], 0)
		res.consume(time.Now().Sub(st), commited, err)
		keyIdx += 3
		if keyIdx > len(keys)-3 {
			keyIdx = 0
		}
	}
}
