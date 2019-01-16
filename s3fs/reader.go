package s3fs

import (
	"bytes"
	"io"
	"net/http"
	"sync"
)

type reader struct {
	o           *s3file
	buf         *bytes.Buffer
	pos         int64
	totalsize   int64
	initializer sync.Once
	err         error
}

func (r *reader) Read(b []byte) (int, error) {
	r.initializer.Do(func() {
		fi, err := r.o.fs.Lstat(r.o.key)
		if err != nil {
			r.err = err
			return
		}
		r.totalsize = fi.Size()

		// w := int64(len(b))
		req, err := http.NewRequest("GET", r.o.fs.url(r.o.key), nil)
		if err != nil {
			r.err = err
			return
		}
		// req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", r.pos, r.pos+w))
		r.o.fs.signRequest(req)

		resp, err := r.o.fs.client.Do(req)
		if err != nil {
			r.err = err
			return
		}

		defer resp.Body.Close()

		n, err := io.Copy(r.buf, resp.Body)
		if err != nil {
			r.err = err
			return
		}
		r.pos = r.pos + int64(n)
	})
	if r.err != nil {
		return 0, r.err
	}

	// if r.buf.Cap() > 0 {
	return r.buf.Read(b)
	// }
}
