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
