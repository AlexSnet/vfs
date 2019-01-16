package s3fs

import (
	"os"
	"path"
	"time"
)

type FileInfo struct {
	name    string
	size    int64
	dir     bool
	modTime time.Time
	sys     *Stat
}

// Stat contains information about an S3 object or directory.
// It is the "underlying data source" returned by method Sys
// for each FileInfo produced by this package.
//   fi.Sys().(*s3util.Stat)
// For the meaning of these fields, see
// http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html.
type Stat struct {
	Key          string
	LastModified string
	ETag         string // ETag value, without double quotes.
	Size         string
	StorageClass string
	OwnerID      string `xml:"Owner>ID"`
	OwnerName    string `xml:"Owner>DisplayName"`
}

type listObjectsResult struct {
	Name                  string
	IsTruncated           bool
	NextContinuationToken string
	Contents              []Stat
	Directories           []string `xml:"CommonPrefixes>Prefix"` // Suffix "/" trimmed
}

func (f *FileInfo) Name() string { return path.Base(f.name) }
func (f *FileInfo) Size() int64  { return f.size }
func (f *FileInfo) Mode() os.FileMode {
	if f.dir {
		return 0755 | os.ModeDir
	}
	return 0644
}
func (f *FileInfo) ModTime() time.Time {
	if f.modTime.IsZero() && f.sys != nil {
		// we return the zero value if a parse error ever happens.
		f.modTime, _ = time.Parse(time.RFC3339Nano, f.sys.LastModified)
	}
	return f.modTime
}
func (f *FileInfo) IsDir() bool      { return f.dir }
func (f *FileInfo) Sys() interface{} { return f.sys }
