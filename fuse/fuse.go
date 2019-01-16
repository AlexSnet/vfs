package fuse

import (
	"bytes"
	"context"
	"os"
	"path"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/alexsnet/vfs"
	"github.com/sirupsen/logrus"
)

type fuseFS struct {
	root      vfs.Filesystem
	mountPath string
	fuseConn  *fuse.Conn
}

func (f *fuseFS) Root() (fs.Node, error) {
	return &fuseNode{fs: f, path: ""}, nil
}

func (f *fuseFS) SetMountPoint(mountPath string) {
	f.mountPath = mountPath
}

func (f *fuseFS) Mount() error {
	c, err := fuse.Mount(
		f.mountPath,
		fuse.FSName("vfs"),
		// fuse.Subtype("hellofs"),
		fuse.LocalVolume(),
		fuse.VolumeName("vfs"),
	)
	if err != nil {
		return err
	}
	f.fuseConn = c

	err = fs.Serve(c, f)
	if err != nil {
		return err
	}

	return nil
}

func (f *fuseFS) Close() error {
	return f.fuseConn.Close()
}

type fuseNode struct {
	fs   *fuseFS
	path string
}

func (fn *fuseNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	fi, err := fn.fs.root.Stat(fn.path)
	if err != nil {
		return nil
	}

	// This is a directory
	if fi.IsDir() {
		// attr.Inode =
		attr.Mode = os.ModeDir | 0777
		return nil
	}

	// attr.Inode = 2
	attr.Mode = 0444
	attr.Size = uint64(fi.Size())
	attr.Mtime = fi.ModTime()

	return nil
}

func (fn *fuseNode) Lookup(ctx context.Context, name string) (fs.Node, error) {
	logrus.WithField("path", fn.path).WithField("name", name).Info("Lookup")
	return &fuseNode{fs: fn.fs, path: path.Join(fn.path, name)}, nil
}

func (fn *fuseNode) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	logrus.WithField("path", fn.path).Info("ReadDirAll")
	fil, err := fn.fs.root.ReadDir(fn.path)
	if err != nil {
		return nil, err
	}

	resp := make([]fuse.Dirent, len(fil))
	for i, fi := range fil {
		fde := fuse.Dirent{
			Inode: uint64(i + 2),
			Name:  fi.Name(),
			Type:  fuse.DT_File,
		}
		if fi.IsDir() {
			fde.Type = fuse.DT_Dir
		}

		resp = append(resp, fde)
	}
	return resp, nil
}

func (fn *fuseNode) ReadAll(ctx context.Context) ([]byte, error) {
	f, err := fn.fs.root.OpenFile(fn.path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	b := bytes.Buffer{}
	b.ReadFrom(f)
	return b.Bytes(), nil
}

func Create(fs vfs.Filesystem) *fuseFS {
	return &fuseFS{root: fs}
}
