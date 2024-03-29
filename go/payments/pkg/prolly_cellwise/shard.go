// Copyright 2022 Dolthub, Inc.
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

package prolly_cellwise

type Shard struct {
	// Table is the name of the table this shard is referring to
	Table string `json:"table"`
	// Path is the ShardStore key used for persisting / retrieving the attribution data for this shard
	Path string `json:"path"`
	// CommitCounts is a slice of counts this shard has attributed to commits
	CommitCounts []uint64 `json:"commit_counts"`
	// StartInclusive is a val.Tuple
	StartInclusive []byte `json:"start_inclusive"`
	// EndInclusive is a val.Tuple
	EndInclusive []byte `json:"end_inclusive"`
}
