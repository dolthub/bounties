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

package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/dolthub/bounties/go/payments/pkg/att"
	"github.com/dolthub/bounties/go/payments/pkg/cellwise"
	"github.com/dolthub/bounties/go/payments/pkg/prolly_cellwise"
	"github.com/dolthub/dolt/go/store/marshal"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/valuefile"
)

func errExit(message string) {
	fmt.Fprintln(os.Stderr, message+"\n")
	os.Exit(1)
}

func main() {
	ctx := context.Background()

	methodStr := flag.String("method", "", "The method used to calculate payments.  Supported options: 'cellwise', 'prolly'.")
	summaryFile := flag.String("summary-file", "", "summary file to print")
	flag.Parse()

	if len(*methodStr) == 0 {
		errExit("Missing required parameter '-method'.")
	} else if len(*summaryFile) == 0 {
		errExit("Missing required parameter '-summary-file'")
	}

	switch *methodStr {
	case "cellwise":
		vf, err := valuefile.ReadValueFile(ctx, *summaryFile)
		if os.IsNotExist(err) {
			errExit(fmt.Sprintf("'%s' does not exist\n", *summaryFile))
		} else if err != nil {
			errExit(fmt.Sprintf("Failed to read '%s': %v\n", *summaryFile, err))
		}

		var summary cellwise.CellwiseAttSummary
		err = marshal.Unmarshal(ctx, types.Format_Default, vf.Values[0], &summary)
		if err != nil {
			errExit(fmt.Sprintf("Failed to unmarshall '%s': %v\n", *summaryFile, err))
		}

		printSummary(ctx, summary)
	case "prolly":
		vf, err := valuefile.ReadValueFile(ctx, *summaryFile)
		if os.IsNotExist(err) {
			errExit(fmt.Sprintf("'%s' does not exist\n", *summaryFile))
		} else if err != nil {
			errExit(fmt.Sprintf("Failed to read '%s': %v\n", *summaryFile, err))
		}

		var summary prolly_cellwise.ProllyAttSummary
		err = marshal.Unmarshal(ctx, types.Format_Default, vf.Values[0], &summary)
		if err != nil {
			errExit(fmt.Sprintf("Failed to unmarshall '%s': %v\n", *summaryFile, err))
		}

		printSummary(ctx, summary)
	default:
		errExit(fmt.Sprintf("unknown method: %s", *methodStr))
	}
}

func printSummary(ctx context.Context, summary att.Summary) {
	commitToCount, err := summary.CommitToCount(ctx)
	if err != nil {
		panic(err)
	}

	fmt.Println("Commit Counts:")
	for h, c := range commitToCount {
		fmt.Printf("%s: %d\n", h.String(), c)
	}
}
