package s3fs

import (
	"bytes"
	"os"
	"path"
	"sync"
)

type s3file struct {
	fs         *S3FS
	rwl        sync.RWMutex
	key        string
	writer     *writer
	reader     *reader
	onceWriter sync.Once
	onceReader sync.Once
}

func (file *s3file) Name() string {
	return path.Base(file.key)
}

func (file *s3file) Stat() (os.FileInfo, error) {
	return file.fs.Stat(file.key)
}

func (file *s3file) Sync() error {
	if file.reader == nil {
		return nil
	}
	return nil
}

func (file *s3file) Truncate(int64) error {
	panic("not implemented")
}

func (file *s3file) Read(p []byte) (n int, err error) {
	file.onceWriter.Do(func() {
		file.rwl.Lock()
		defer file.rwl.Unlock()
		file.reader = &reader{
			o:   file,
			buf: new(bytes.Buffer),
		}
	})
	return file.reader.Read(p)
}

func (file *s3file) ReadAt(p []byte, off int64) (n int, err error) {
	panic("not implemented")

}

func (file *s3file) Seek(offset int64, whence int) (int64, error) {
	panic("not implemented")
}

func (file *s3file) Write(p []byte) (n int, err error) {
	file.onceWriter.Do(func() {
		file.writer = newWriter(file)
	})

	return file.writer.Write(p)
}

func (file *s3file) Close() error {
	file.onceWriter.Do(func() {
		file.writer = newWriter(file)
	})
	return file.writer.Close()
}
