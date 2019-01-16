package s3fs

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html
func (fs *S3FS) authString(req *http.Request) string {
	if req.Header.Get("Date") == "" {
		req.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	}

	// canonicalize amz headers
	a := make([]string, 0, 1)
	for k, _ := range req.Header {
		k = strings.ToLower(k)
		if strings.HasPrefix(k, "x-amz-") {
			a = append(a, k)
		}
	}

	sort.Strings(a)

	for i, v := range a {
		k := http.CanonicalHeaderKey(v)
		vv := req.Header[k]
		a[i] = v + `:` + strings.Join(vv, `,`) + "\n"
	}

	canonicalAmzHeaders := strings.Join(a, "")

	// canonicalize resource
	var cres string
	cres = fmt.Sprintf("/%s/", fs.Bucket)

	if req.URL.Path == cres || req.URL.Path == "/" {
		if strings.Contains(req.URL.RawQuery, "lifecycle") {
			cres = cres + `?` + req.URL.RawQuery
		}
	} else {
		c, rawQuery := canonicalResource(req.URL.Path, req.URL.Query())
		cres = c
		req.URL.RawQuery = rawQuery
	}

	return strings.Join([]string{
		strings.TrimSpace(req.Method),
		req.Header.Get("Content-MD5"),
		req.Header.Get("Content-Type"),
		req.Header.Get("Date"),
		canonicalAmzHeaders + cres,
	}, "\n")
}

func canonicalResource(path string, query url.Values) (cres, rawQuery string) {
	p := strings.Split(path, `/`)
	for i, v := range p {
		p[i] = escape(v)
	}
	cres = strings.Join(p, `/`)

	if len(query) > 0 {
		a := make([]string, 0, 1)
		for k := range query {
			a = append(a, k)
		}

		sort.Strings(a)

		parts := make([]string, 0, len(a))
		for _, k := range a {
			vv := query[k]
			for _, v := range vv {
				if v == "" {
					parts = append(parts, escape(k))
				} else {
					parts = append(parts, fmt.Sprintf("%s=%s", escape(k), escape(v)))
				}
			}
		}

		qs := strings.Join(parts, "&")

		rawQuery = qs
		cres += `?` + qs
	}

	return
}

// escape ensures everything is properly escaped and spaces use %20 instead of +
func escape(s string) string {
	return strings.Replace(url.QueryEscape(s), `+`, `%20`, -1)
}

func (fs *S3FS) signRequest(req *http.Request) {
	authStr := fs.authString(req)

	// reqd, _ := httputil.DumpRequest(req, true)
	// fmt.Println("\n++++++++++++++++\n++++++++++++++++\n" + string(reqd) + "\n++++++++++++++++\n" + authStr + "\n++++++++++++++++\n++++++++++++++++\n")

	h := hmac.New(sha1.New, []byte(fs.Secret))
	h.Write([]byte(authStr))

	h64 := base64.StdEncoding.EncodeToString(h.Sum(nil))
	auth := "AWS " + fs.AccessKey + ":" + h64
	req.Header.Set("Authorization", auth)
}
