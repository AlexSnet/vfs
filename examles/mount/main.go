package main

import (
	"github.com/alexsnet/vfs"
	"github.com/alexsnet/vfs/fuse"
	"github.com/alexsnet/vfs/memfs"
	"github.com/alexsnet/vfs/mountfs"
	"github.com/alexsnet/vfs/s3fs"
)

func main() {
	m := memfs.Create()
	o := vfs.OS()

	s3 := s3fs.Create(
		"",
		"FWDHGU11FMPKZN7DHUZ7",
		"FdQ/sSANAfGgUsS+eppyEWeIL9f0ZiJx+CCaLeJP",
		"127.0.0.1:9000",
		"http",
	)

	mount := mountfs.Create(m)
	mount.Mount(o, "/os/")
	mount.Mount(s3, "/s3/")

	ffs := fuse.Create(mount)
	defer ffs.Close()
	ffs.SetMountPoint("/tmp/vfs")
	err := ffs.Mount()
	if err != nil {
		panic(err)
	}
}
