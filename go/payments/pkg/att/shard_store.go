// Copyright 2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package att

import (
	"context"
	"os"
	"path/filepath"

	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/valuefile"
)

// ShardStore is an interface for storing and retrieving shard data
type ShardStore interface {
	// WriteShard presists shard data stored in noms.valuefile format
	WriteShard(ctx context.Context, key string, store *valuefile.FileValueStore, shardVal types.Value) error
	// ReadShard reads shard data
	ReadShard(ctx context.Context, key string) (*MemShard, error)
	// Join joins key elements into a single key with delimiters that are appropriate for the backing storage
	Join(keyElements ...string) string
}

// MemShard is the in memory representation of a shard and its |Value|.
type MemShard struct {
	Value types.Value
	Vrw   types.ValueReadWriter
	Ns    tree.NodeStore
}

// ensure *FilesysShardStoree implements ShardStore
var _ ShardStore = (*FilesysShardStore)(nil)

// FilesysShardStore is a ShardStore implementation that reads from and writes to the local file system
type FilesysShardStore struct {
	rootDir string
}

// NewFilesysShardStore returns a new FilesysShardStore object
func NewFilesysShardStore(rootDir string) (*FilesysShardStore, error) {
	absRoot, err := filepath.Abs(rootDir)
	if err != nil {
		return nil, err
	}

	return &FilesysShardStore{
		rootDir: absRoot,
	}, nil
}

// WriteShard presists shard data stored in noms.valuefile format
func (f *FilesysShardStore) WriteShard(ctx context.Context, key string, store *valuefile.FileValueStore, shardVal types.Value) error {
	absPath := filepath.Join(f.rootDir, key)

	dir := filepath.Dir(absPath)
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return err
	}

	return valuefile.WriteValueFile(ctx, absPath, store, shardVal)
}

// ReadShard reads shard data
func (f *FilesysShardStore) ReadShard(ctx context.Context, key string) (*MemShard, error) {
	absPath := filepath.Join(f.rootDir, key)

	vf, err := valuefile.ReadValueFile(ctx, absPath)
	if os.IsNotExist(err) {
		return nil, ErrSummaryDoesntExist
	} else if err != nil {
		return nil, err
	}

	return &MemShard{
		Value: vf.Values[0],
		Vrw:   vf.Vrw,
		Ns:    vf.Ns,
	}, nil
}

// Join joins key elements into a single key with delimiters that are appropriate for the backing storage.  In this case
// it uses the filesys appropriate file separator.
func (f *FilesysShardStore) Join(keyElements ...string) string {
	return filepath.Join(keyElements...)
}
