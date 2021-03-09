package att

import (
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/valuefile"
	"os"
	"path/filepath"
)

type ShardStore interface {
	WriteShard(ctx context.Context, key string, store *valuefile.FileValueStore, shardVal types.Value) error
	ReadShard(ctx context.Context, key string) (types.Value, error)
	CopyShard(ctx context.Context, sourceKey, destKey string) error
	Join(keyElements ...string) string
}

var _ ShardStore = (*FilesysShardStore)(nil)

type FilesysShardStore struct {
	rootDir string
}

func NewFilesysShardStore(rootDir string) (*FilesysShardStore, error) {
	absRoot, err := filepath.Abs(rootDir)

	if err != nil {
		return nil, err
	}

	return &FilesysShardStore{
		rootDir: absRoot,
	}, nil
}

func (f *FilesysShardStore) WriteShard(ctx context.Context, key string, store *valuefile.FileValueStore, shardVal types.Value) error {
	absPath := f.Join(key)

	dir := filepath.Dir(absPath)
	err := os.MkdirAll(dir, os.ModePerm)

	if err != nil {
		return err
	}

	//fmt.Println("writing", absPath)
	return valuefile.WriteValueFile(ctx, absPath, store, shardVal)
}

func (f *FilesysShardStore) ReadShard(ctx context.Context, key string) (types.Value, error) {
	absPath := f.Join(key)

	fmt.Println("reading", absPath)
	vals, err := valuefile.ReadValueFile(ctx, absPath)

	if os.IsNotExist(err) {
		return nil, ErrSummaryDoesntExist
	} else if err != nil {
		return nil, err
	}

	return vals[0], nil
}

func (f *FilesysShardStore) CopyShard(ctx context.Context, sourceKey, destKey string) error {
	absSource := f.Join(sourceKey)
	absDest := f.Join(destKey)

	dir := filepath.Dir(absDest)
	err := os.MkdirAll(dir, os.ModePerm)

	if err != nil {
		return err
	}

	fmt.Println("Copying", absSource, "to", absDest)
	_, err = iohelp.CopyFile(absSource, absDest)
	return err
}

func (f *FilesysShardStore) Join(keyElements ...string) string {
	path := filepath.Join(keyElements...)

	if !filepath.IsAbs(path) {
		return filepath.Join(f.rootDir, path)
	}

	return path
}
