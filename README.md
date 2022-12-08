[DoltHub bounties](https://www.dolthub.com/bounties) have been designed to encourage data collaboration by incentivizing collaborators with real cash prizes.
In order to be transparent about how prizes are calculated, we are open sourcing the code that we use to attribute 
contributions made during a bounty to the PRs that they came from.

Check out our [website](https://www.dolthub.com), [team](https://www.dolthub.com/team), and [documentation](https://docs.dolthub.com/introduction/what-is-dolt) to learn more about Dolt.

# Overview of Repository

The codebase is made up of protobuf definition files and go files.  The proto files are under `proto/` and the go code is
under `go/`.

## The Go codebase

The go codebase is split up into generated code, and command line tools and library packages for bounty payment processing.  
The command line tools reside within `go/payments/cmd/` and the library packages are in `go/payments/pkg`.

### calc_payments

`calc_payments` is a command line utility that allows executing of attribution models on a local dolt database.

#### Installation

From the `go/payments/cmd/calc_payments` run `go install .`

#### Usage

```
	calc_payments [-incremental <DIR>] [-repo-dir <DIR>] [-method <METHOD_NAME>] -start <COMMIT_HASH> -end <COMMIT_HASH> 

      -start COMMIT_HASH
            Commit hash representing the start of a bounty before any contributions are made.
      -end COMMIT_HASH
            Last commit hash included in the payment calculation.
      -method METHOD_NAME
            The method used to calculate payments.  Supported options: 'cellwise'
      -repo-dir DIRECTORY
            Directory of the repository being examened (default "./")
      -incremental DIRECTORY
            When a directory is provided, incremental attribution is enabled and state files are read from and written to the provided directory.
``` 

### cellwise attribution

The folder `go/pkg/cellwise` contains the code use to do cellwise change attribution.  Cellwise attribution looks at all
the changes that have occurred to a dataset during a bounty, and attributes the changes to the first merge commit where
that value appears.  Based on the way bounties are run, and changes are accepted, each merge commit should correspond to
an accepted PR.

In order to be able to do this without re-processing the entire history each time a new change is merged, we walk the 
commit graph from the starting point of the bounty until the latest commit, and at every merge commit we iterate over
all the changes.  Any value for a cell that is new we will attribute to the current commit.  If the new value was seen
before, during the bounty we attribute the change back to the original commit where it was first seen.  This allows us
to save state between attribution runs and only process new commits.
