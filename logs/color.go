package logs

import "io"

type ansiColorWriter struct {
	w    io.Writer
	mode outputMode
}

func (cw *ansiColorWriter) Write(p []byte) (int, error) {
	return cw.w.Write(p)
}
