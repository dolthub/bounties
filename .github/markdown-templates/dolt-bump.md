:coffee: *An Automated Dolt Version Bump PR* :crown:

### Initial Changes

The initial changes contained in this PR were produced by `go get`ing the lastest `dolt`.

```
$ cd ./go
$ go get -u github.com/dolthub/dolt/go@<commit>
```

### Before Merging

This PR must have passing CI and a review before merging.

Please ensure the`go` tests all pass successfully. If they do not, please implement the fixes
and re-run CI `go` tests until they do.

### After Merging

An automatic PR will be opened against the LD repo that bumps the bounties version there.
