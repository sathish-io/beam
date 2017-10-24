package api

import (
	"bufio"
	"io"
	"strings"
)

// prettyPrintTable prints 't' as a nicely formatted table
// if hasHeaderRow / hasFooterRow is true, then a divider will be added between
// the header and/or footer rows
func prettyPrintTable(dest io.Writer, t [][]string, hasHeaderRow, hasFooterRow bool) {
	w := bufio.NewWriterSize(dest, 256)
	defer w.Flush()
	for c := range t[0] {
		w := 0
		for r := range t {
			w = max(w, len(t[r][c]))
		}
		for r := range t {
			t[r][c] = strings.Repeat(" ", w+1-len(t[r][c])) + t[r][c] + " |"
		}
	}
	divider := func() {
		for _, c := range t[0] {
			io.WriteString(w, " ")
			for i := 0; i < len(c)-3; i++ {
				io.WriteString(w, "-")
			}
			io.WriteString(w, " |")
		}
		io.WriteString(w, "\n")
	}
	for idx, r := range t {
		for _, c := range r {
			io.WriteString(w, c)
		}
		io.WriteString(w, "\n")
		if (hasHeaderRow && (idx == 0)) || (hasFooterRow && (idx == len(t)-2)) {
			divider()
		}
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
