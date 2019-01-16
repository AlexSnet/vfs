package s3fs

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"

	"github.com/alexsnet/vfs"
)

func GetEtag(f vfs.File, partSize int64) (string, error) {
	fi, err := f.Stat()
	if err != nil {
		return "", err
	}

	size := fi.Size()

	var etag string
	if size > partSize && partSize != 0 {
		contentToHash := bytes.Buffer{}

		parts := 0

		for {
			hash := md5.New()
			n, err := io.CopyN(hash, f, partSize)
			if err != nil && err != io.EOF {
				return "", err
			}

			if err == io.EOF {
				break
			}

			contentToHash.Write(hash.Sum(nil)[:])
			parts++

			if n < partSize {
				break
			}
		}

		etag = fmt.Sprintf("%x-%d", md5.Sum(contentToHash.Bytes()[:]), parts)

	} else {
		hash := md5.New()
		if _, err := io.Copy(hash, f); err != nil {
			return "", err
		}
		etag = fmt.Sprintf("%x", hash.Sum(nil))
	}

	return etag, nil
}
