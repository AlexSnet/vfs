package s3fs

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/alexsnet/vfs"
	"github.com/sirupsen/logrus"
)

// A S3FS that prefixes the path in each vfs.Filesystem operation.
type S3FS struct {
	// Bucket is the S3 bucket to use
	Bucket string
	// AccessKey is the S3 access key
	AccessKey string
	// Secret is the S3 secret
	Secret string
	Proto  string
	Host   string

	client             *http.Client
	concurrencyUploads int
}

// Create returns a file system
func Create(bucket, accessKey, secret, host, proto string) *S3FS {
	return &S3FS{
		Bucket:             bucket,
		AccessKey:          accessKey,
		Secret:             secret,
		Host:               host,
		Proto:              proto,
		client:             http.DefaultClient,
		concurrencyUploads: 5,
	}
}

// PathSeparator implements vfs.Filesystem.
func (fs *S3FS) PathSeparator() uint8 { return '/' }

// OpenFile implements vfs.Filesystem.
func (fs *S3FS) OpenFile(name string, flag int, perm os.FileMode) (vfs.File, error) {
	// @TODO: make work with flags and permisions
	f := &s3file{
		fs:  fs,
		key: name,
	}
	return f, nil
}

// Remove implements vfs.Filesystem.
func (fs *S3FS) Remove(name string) error {
	req, err := http.NewRequest("DELETE", fs.url(name), nil)
	if err != nil {
		return err
	}
	fs.signRequest(req)

	resp, err := fs.client.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != 204 {
		return fmt.Errorf("Can not remove file")
	}

	return nil
}

// Rename implements vfs.Filesystem.
func (fs *S3FS) Rename(oldpath, newpath string) error {
	// o := fs.s3.Object(oldpath)
	return nil
}

// Mkdir implements vfs.Filesystem.
func (fs *S3FS) Mkdir(name string, perm os.FileMode) error {
	return nil
}

// Stat implements vfs.Filesystem.
func (fs *S3FS) Stat(name string) (os.FileInfo, error) {
	return fs.Lstat(name)
}

// Lstat implements vfs.Filesystem.
func (fs *S3FS) Lstat(name string) (os.FileInfo, error) {
	req, err := http.NewRequest("HEAD", fs.url(name), nil)
	if err != nil {
		return nil, err
	}
	fs.signRequest(req)

	resp, err := fs.client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, os.ErrNotExist
	}

	t, err := time.Parse(time.RFC1123, resp.Header.Get("Date"))
	if err != nil {
		return nil, err
	}
	isDir := false
	if resp.ContentLength == -1 {
		isDir = true
	}
	size := resp.ContentLength
	if size < 0 {
		size = 0
	}

	fi := FileInfo{
		name:    name,
		size:    size,
		dir:     isDir,
		modTime: t,
		sys: &Stat{
			LastModified: resp.Header.Get("Date"),
			Key:          name,
			ETag: func(resp *http.Response) string {
				var etag string
				if len(resp.Header.Get("Etag")) > 8 {
					etag = strings.Trim(resp.Header.Get("Etag"), ` "`)
				}
				return etag
			}(resp),
			StorageClass: resp.Header.Get("X-Amz-Storage-Class"),
		},
	}
	return &fi, nil
}

// ReadDir implements vfs.Filesystem.
func (fs *S3FS) ReadDir(path string) ([]os.FileInfo, error) {
	infos := []os.FileInfo{}
	continuationToken := ""
	for {
		vars := url.Values{}
		vars.Add("delimiter", "/")
		vars.Add("list-type", "2")

		if len(continuationToken) > 0 {
			vars.Add("continuation-token", continuationToken)
		}

		prefix := strings.TrimLeft(path, "/")
		if prefix != "" && !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
		if prefix != "" {
			vars.Set("prefix", prefix)
		}

		uri, _ := url.Parse(fs.url(path))
		uri.RawQuery = vars.Encode()
		uri.Path = fmt.Sprintf("/%s/", fs.Bucket)

		req, _ := http.NewRequest("GET", uri.String(), nil)
		req.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))

		fs.signRequest(req)

		resp, err := fs.client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf(resp.Status)
		}

		decoder := xml.NewDecoder(resp.Body)
		result := listObjectsResult{}

		err = decoder.Decode(&result)
		if err != nil {
			return nil, err
		}

		var size int64
		var name string
		var is_dir bool
		for _, content := range result.Contents {
			c := content
			c.ETag = strings.Trim(c.ETag, `"`)
			size, _ = strconv.ParseInt(c.Size, 10, 0)
			if size == 0 && strings.HasSuffix(c.Key, "/") {
				name = strings.TrimRight(c.Key, "/")
				is_dir = true
			} else {
				name = c.Key
				is_dir = false
			}
			modTime, err := time.Parse(time.RFC3339Nano, c.LastModified)
			if err != nil {
				logrus.WithError(err).WithField("time", c.LastModified).Error("can not parse time")
			}
			infos = append(infos, &FileInfo{
				name:    name,
				size:    size,
				dir:     is_dir,
				sys:     &c,
				modTime: modTime,
			})
		}
		for _, dir := range result.Directories {
			infos = append(infos, &FileInfo{
				name: strings.TrimRight(dir, "/"),
				size: 0,
				dir:  true,
			})
		}

		if result.IsTruncated && len(result.NextContinuationToken) > 0 {
			continuationToken = result.NextContinuationToken
		} else {
			break
		}
	}
	return infos, nil
}

func (fs *S3FS) url(query string) string {
	base, err := url.Parse(fs.Proto + `://` + fs.Host + `/`)
	if err != nil {
		panic(err)
	}

	if strings.HasPrefix(query, "/") {
		query = strings.TrimLeft(query, "/")
	}

	path, err := url.ParseRequestURI(`/` + fs.Bucket + `/` + query)
	if err != nil {
		panic(err)
	}
	u := base.ResolveReference(path)
	return u.String()
}

func (fs *S3FS) GetLifecycle() {
	base, err := url.Parse(fs.Proto + `://` + fs.Bucket + `.` + fs.Host + `/`)
	if err != nil {
		panic(err)
	}
	base.RawQuery = "lifecycle"

	req, err := http.NewRequest("GET", base.String(), nil)
	if err != nil {
		// return nil, err
		return
	}
	fs.signRequest(req)

	resp, err := fs.client.Do(req)
	if err != nil {
		// return nil, err
		return
	}

	b, _ := httputil.DumpResponse(resp, true)
	fmt.Println(string(b))
}
