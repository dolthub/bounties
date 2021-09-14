:coffee: *An Automated Dependency Version Bump PR* :crown:

### Initial Changes

The initial changes contained in this PR were produced by `go get`ing the dependency.

```
$ cd ./go
$ go get github.com/dolthub/<dependency>/go@<commit>
$ go mod tidy
```

### Before Merging

This PR must have passing CI and a review before merging.

### After Merging

An automatic PR will be opened against the LD repo that bumps the bounties version there.
