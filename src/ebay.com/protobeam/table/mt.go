package table

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/rcrowley/go-metrics"
)

// MetricsTable generates a table of metric values from the supplied registry,
// formatting durations into the scale duration provided.
// You can use PrettyPrintTable to render it to a writer.
func MetricsTable(r metrics.Registry, scale time.Duration) [][]string {
	res := [][]string{
		{"Name", "Count", "Mean", "Min", "p25", "p50", "p90", "p99", "p99.9", "Max"},
	}
	colCount := len(res[0])
	addRow := func(cols ...string) {
		for len(cols) < colCount {
			cols = append(cols, "")
		}
		res = append(res, cols)
	}
	du := float64(scale)
	suffix := scale.String()[1:]

	r.Each(func(n string, m interface{}) {
		switch metric := m.(type) {
		case metrics.Timer:
			t := metric.Snapshot()
			ps := t.Percentiles([]float64{0.25, 0.5, 0.9, 0.99, 0.999})
			addRow(
				n,
				strconv.FormatInt(t.Count(), 10),
				fmt.Sprintf("%f%s", float64(t.Mean())/du, suffix),
				fmt.Sprintf("%f%s", float64(t.Min())/du, suffix),
				fmt.Sprintf("%f%s", ps[0]/du, suffix),
				fmt.Sprintf("%f%s", ps[1]/du, suffix),
				fmt.Sprintf("%f%s", ps[2]/du, suffix),
				fmt.Sprintf("%f%s", ps[3]/du, suffix),
				fmt.Sprintf("%f%s", ps[4]/du, suffix),
				fmt.Sprintf("%f%s", float64(t.Max())/du, suffix),
			)
		case metrics.Gauge:
			t := metric.Snapshot()
			addRow(n, strconv.FormatInt(t.Value(), 10))
		case metrics.Counter:
			t := metric.Snapshot()
			addRow(n, strconv.FormatInt(t.Count(), 10))
		}
	})
	dr := res[1:]
	sort.Slice(dr, func(a, b int) bool {
		return dr[a][0] < dr[b][0]
	})
	return res
}
