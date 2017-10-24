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
		{"Name", "Count", "Min", "Max", "Mean", "p50", "p90", "p99"},
	}
	du := float64(scale)
	suffix := scale.String()[1:]

	r.Each(func(n string, m interface{}) {
		switch metric := m.(type) {
		case metrics.Timer:
			t := metric.Snapshot()
			ps := t.Percentiles([]float64{0.5, 0.9, 0.99})
			res = append(res, []string{
				n,
				strconv.FormatInt(t.Count(), 10),
				fmt.Sprintf("%f%s", float64(t.Min())/du, suffix),
				fmt.Sprintf("%f%s", float64(t.Max())/du, suffix),
				fmt.Sprintf("%f%s", float64(t.Mean())/du, suffix),
				fmt.Sprintf("%f%s", ps[0]/du, suffix),
				fmt.Sprintf("%f%s", ps[1]/du, suffix),
				fmt.Sprintf("%f%s", ps[2]/du, suffix),
			})
		}
	})
	dr := res[1:]
	sort.Slice(dr, func(a, b int) bool {
		return dr[a][0] < dr[b][0]
	})
	return res
}
