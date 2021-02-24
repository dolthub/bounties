// Copyright 2020 Dolthub, Inc.
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

package cellwise

import (
	payments "github.com/dolthub/bounties/go/gen/proto/payments/v1alpha1"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/golang/protobuf/proto"
)

// Serialize takes a celwise.DatabaseAttribution object and converts it to a proto defined object before serializing it
// to a byte slice
func Serialize(att *DatabaseAttribution) ([]byte, error) {
	var commitsProto [][]byte
	for _, commitHash := range att.Commits {
		h := commitHash
		commitsProto = append(commitsProto, h[:])
	}

	attProto := &payments.CellwiseDBAttribution{
		StartHash:      att.AttribStartPoint[:],
		Commits:        commitsProto,
		NameToTableAtt: make(map[string]*payments.TableAttribution, len(att.NameToTableAttribution)),
	}

	for name, tblAtt := range att.NameToTableAttribution {
		commitToCountProto := convertCommitToCountProto(tblAtt.CommitToCellCount)
		attProto.NameToTableAtt[name] = &payments.TableAttribution{
			RowAttribution:  serializeRowAtt(tblAtt.pkHashToTagToCellAtt),
			CommitToCount:   commitToCountProto,
			AttributedCells: uint64(tblAtt.AttributedCells),
		}
	}

	return proto.Marshal(attProto)
}

func convertCommitToCountProto(commitToCellCount map[int16]int) map[int32]uint32 {
	commitToCountProto := make(map[int32]uint32, len(commitToCellCount))
	for commitIdx, count := range commitToCellCount {
		commitToCountProto[int32(commitIdx)] = uint32(count)
	}
	return commitToCountProto
}

func serializeRowAtt(pkHashToTagToCellAtt map[hash.Hash]map[uint64]*cellAtt) *payments.PKHashToTagToCellAttribution {
	var pkHashToTagToCellProto payments.PKHashToTagToCellAttribution
	for pkHash, tagToCellAtt := range pkHashToTagToCellAtt {
		var tagToCellAttProto payments.TagToCellAttribution
		for tag, cellAtt := range tagToCellAtt {
			tagToCellAttProto.Tags = append(tagToCellAttProto.Tags, tag)
			tagToCellAttProto.CellAtt = append(tagToCellAttProto.CellAtt, &payments.CellAttribution{
				CommitIdx:  int32(cellAtt.CurrentOwner),
				PastValues: serializePastValues(cellAtt.PastValues),
			})
		}

		h := pkHash
		pkHashToTagToCellProto.PkHashes = append(pkHashToTagToCellProto.PkHashes, h[:])
		pkHashToTagToCellProto.TagToCell = append(pkHashToTagToCellProto.TagToCell, &tagToCellAttProto)
	}

	return &pkHashToTagToCellProto
}

func serializePastValues(pastValues map[hash.Hash]int16) *payments.HashToIndex {
	if len(pastValues) > 0 {
		var hToIdx payments.HashToIndex
		for valHash, commitIdx := range pastValues {
			h := valHash
			hToIdx.PastHashes = append(hToIdx.PastHashes, h[:])
			hToIdx.PastCommitIndexes = append(hToIdx.PastCommitIndexes, int32(commitIdx))
		}

		return &hToIdx
	}

	return nil
}
// Deserialize takes a byte slice and attempts to deserialize based on the proto definition and then converts the proto
// object to a cellwise.DatabaseAttribution object
func DeserializeWithoutCellData(bytes []byte) (*DatabaseAttribution, error) {
	var dbAttProto payments.CellwiseDBAttribution
	err := proto.Unmarshal(bytes, &dbAttProto)

	if err != nil {
		return nil, err
	}

	return dbAttributionNoCells(&dbAttProto)
}


// Deserialize takes a byte slice and attempts to deserialize based on the proto definition and then converts the proto
// object to a cellwise.DatabaseAttribution object
func Deserialize(bytes []byte) (*DatabaseAttribution, error) {
	var dbAttProto payments.CellwiseDBAttribution
	err := proto.Unmarshal(bytes, &dbAttProto)

	if err != nil {
		return nil, err
	}

	dbAtt, err := dbAttributionNoCells(&dbAttProto)

	if err != nil {
		return nil, err
	}

	err = addCellAttData(&dbAttProto, dbAtt)

	if err != nil {
		return nil, err
	}

	return dbAtt, nil
}

func dbAttributionNoCells(dbAttProto *payments.CellwiseDBAttribution) (*DatabaseAttribution, error) {
	var commits []hash.Hash
	for _, bytes := range dbAttProto.Commits {
		commits = append(commits, hash.New(bytes))
	}

	dbAtt := &DatabaseAttribution{
		AttribStartPoint:       hash.New(dbAttProto.StartHash),
		Commits:                commits,
		NameToTableAttribution: make(map[string]*TableAttribution),
	}

	for name, tblAttProto := range dbAttProto.NameToTableAtt {
		dbAtt.NameToTableAttribution[name] = &TableAttribution{
			CommitToCellCount:    convertProtoToCommitToCount(tblAttProto.CommitToCount),
			AttributedCells:      int64(tblAttProto.AttributedCells),
		}
	}

	return dbAtt, nil
}

func addCellAttData(dbAttProto *payments.CellwiseDBAttribution, dbAtt *DatabaseAttribution) error {
	for name, tblAttProto := range dbAttProto.NameToTableAtt {
		dbAtt.NameToTableAttribution[name].pkHashToTagToCellAtt = deserializeRowAtt(tblAttProto.RowAttribution)
	}

	return nil
}

func convertProtoToCommitToCount(commitToCountProto map[int32]uint32) map[int16]int {
	commitToCellCount := make(map[int16]int, len(commitToCountProto))
	for commitIdx, count := range commitToCountProto {
		commitToCellCount[int16(commitIdx)] = int(count)
	}
	return commitToCellCount
}

func deserializeRowAtt(rowAttProto *payments.PKHashToTagToCellAttribution) map[hash.Hash]map[uint64]*cellAtt {
	pkHashToTagToCellAtt := make(map[hash.Hash]map[uint64]*cellAtt, len(rowAttProto.PkHashes))
	for i := 0; i < len(rowAttProto.PkHashes); i++ {
		pkHash := hash.New(rowAttProto.PkHashes[i])
		tagToCell := rowAttProto.TagToCell[i]

		tagToCellAtt := make(map[uint64]*cellAtt, len(tagToCell.Tags))
		for j := 0; j < len(tagToCell.Tags); j++ {
			tag := tagToCell.Tags[j]
			cell := tagToCell.CellAtt[j]

			tagToCellAtt[tag] = &cellAtt{
				CurrentOwner: int16(cell.CommitIdx),
				PastValues:   deserializePastValues(cell.PastValues),
			}
		}

		pkHashToTagToCellAtt[pkHash] = tagToCellAtt
	}

	return pkHashToTagToCellAtt
}

func deserializePastValues(hToIdx *payments.HashToIndex) map[hash.Hash]int16 {
	if hToIdx == nil {
		return nil
	}

	pastValues := make(map[hash.Hash]int16, len(hToIdx.PastCommitIndexes))
	for i := 0; i < len(hToIdx.PastCommitIndexes); i++ {
		h := hToIdx.PastHashes[i]
		idx := hToIdx.PastCommitIndexes[i]
		pastValues[hash.New(h)] = int16(idx)
	}

	return pastValues
}
