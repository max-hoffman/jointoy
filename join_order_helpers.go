package jointoy

import (
	"fmt"
	"math"
	"math/bits"
)

//go:generate stringer -type=JoinType -linecomment

// commute returns true if the given join edge type is commutable.
func commute(op JoinType) bool {
	return op == InnerJoinType || op == CrossJoinType
}

type JoinType uint16

const (
	CrossJoinType     JoinType = iota // Inner
	InnerJoinType                     // Left
	SemiJoinType                      // Semi
	AntiJoinType                      // Anti
	LeftJoinType                      // Cross
	FullOuterJoinType                 // FullOuter
	GroupByJoinType                   // GroupBy
)

// conflictRule is a pair of vertex sets which carry the requirement that if the
// 'from' set intersects a set of prospective join input relations, then the
// 'to' set must be a subset of the input relations (from -> to). Take the
// following query as an example:
//
//	SELECT * FROM xy
//	INNER JOIN (SELECT * FROM ab LEFT JOIN uv ON a = u)
//	ON x = a
//
// During execution of the CD-C algorithm, the following conflict rule would
// be added to inner join edge: [uv -> ab]. This means that, for any join that
// uses this edge, the presence of uv in the set of input relations implies the
// presence of ab. This prevents an inner join between relations xy and uv
// (since then ab would not be an input relation). Note that, in practice, this
// conflict rule would be absorbed into the TES because ab is a part of the
// inner join edge's SES (see the addRule func).
type conflictRule struct {
	from vertexSet
	to   vertexSet
}

// lookupTableEntry is an entry in one of the join property lookup tables
// defined below (associative, left-asscom and right-asscom properties). A
// lookupTableEntry can be unconditionally true or false, as well as true
// conditional on the null-rejecting properties of the edge filters.
type lookupTableEntry uint8

const (
	// never indicates that the transformation represented by the table entry is
	// unconditionally incorrect.
	never lookupTableEntry = 0

	// always indicates that the transformation represented by the table entry is
	// unconditionally correct.
	always lookupTableEntry = 1 << (iota - 1)

	// filterA indicates that the filters of the "A" join edge must reject
	// nulls for the set of vertexes specified by rejectsOnLeftA, rejectsOnRightA,
	// etc.
	filterA

	// filterB indicates that the filters of the "B" join edge must reject
	// nulls for the set of vertexes specified by rejectsOnLeftA, rejectsOnRightA,
	// etc.
	filterB

	// rejectsOnLeftA indicates that the filters must reject nulls for the left
	// input relations of edge "A".
	rejectsOnLeftA

	// rejectsOnRightA indicates that the filters must reject nulls for the right
	// input relations of edge "A".
	rejectsOnRightA

	// rejectsOnRightB indicates that the filters must reject nulls for the right
	// input relations of edge "B".
	rejectsOnRightB

	// table2Note1 indicates that the filters of edge "B" must reject nulls on
	// the relations of the right input of edge "A".
	// Citations: [8] Table 2 Footnote 1.
	table2Note1 = filterB | rejectsOnRightA

	// table2Note2 indicates that the filters of operators "A" and "B" must reject
	// nulls on the relations of the right input of edge "A".
	// Citations: [8] Table 2 Footnote 2.
	table2Note2 = (filterA | filterB) | rejectsOnRightA

	// table3Note1 indicates that the filters of edge "A" must reject nulls on
	// the relations of the left input of edge "A".
	// Citations: [8] Table 3 Footnote 1.
	table3Note1 = filterA | rejectsOnLeftA

	// table3Note2 indicates that the filters of edge "B" must reject nulls on
	// the relations of the right input of edge "B".
	// Citations: [8] Table 3 Footnote 1]2.
	table3Note2 = filterB | rejectsOnRightB

	// table3Note3 indicates that the filters of operators "A" and "B" must reject
	// nulls on the relations of the left input of edge "A".
	// Citations: [8] Table 3 Footnote 3.
	table3Note3 = (filterA | filterB) | rejectsOnLeftA

	// table3Note4 indicates that the filters of operators "A" and "B" must reject
	// nulls on the relations of the right input of edge "B".
	// Citations: [8] Table 3 Footnote 4.
	table3Note4 = (filterA | filterB) | rejectsOnRightB
)

// assocTable is a lookup table indicating whether it is correct to apply the
// associative transformation to pairs of join operators.
// citations: [8] table 2
var assocTable = [7][7]lookupTableEntry{
	//             cross-B inner-B semi-B  anti-B  left-B  full-B group-B
	/* cross-A */ {always, always, always, always, always, never, always},
	/* inner-A */ {always, always, always, always, always, never, always},
	/* semi-A  */ {never, never, never, never, never, never, never},
	/* anti-A  */ {never, never, never, never, never, never, never},
	/* left-A  */ {never, never, never, never, table2Note1, never, never},
	/* full-A  */ {never, never, never, never, table2Note1, table2Note2, never},
	/* group-A  */ {never, never, never, never, never, never, never},
}

// leftAsscomTable is a lookup table indicating whether it is correct to apply
// the left-asscom transformation to pairs of join operators.
// citations: [8] table 3
var leftAsscomTable = [7][7]lookupTableEntry{
	//             cross-A inner-B semi-B  anti-B  left-B  full-B
	/* cross-A */ {always, always, always, always, always, never, always},
	/* inner-A */ {always, always, always, always, always, never, always},
	/* semi-A  */ {always, always, always, always, always, never, always},
	/* anti-A  */ {always, always, always, always, always, never, always},
	/* left-A  */ {always, always, always, always, always, table3Note1, always},
	/* full-A  */ {never, never, never, never, table3Note2, table3Note3, never},
	/* group-A */ {always, always, always, always, always, never, always},
}

// rightAsscomTable is a lookup table indicating whether it is correct to apply
// the right-asscom transformation to pairs of join operators.
// citations: [8] table 3
var rightAsscomTable = [7][7]lookupTableEntry{
	//             cross-B inner-B semi-B anti-B left-B full-B
	/* cross-A */ {always, always, never, never, never, never},
	/* inner-A */ {always, always, never, never, never, never},
	/* semi-A  */ {never, never, never, never, never, never},
	/* anti-A  */ {never, never, never, never, never, never},
	/* left-A  */ {never, never, never, never, never, never},
	/* full-A  */ {never, never, never, never, never, table3Note4},
	/* group-A */ {never, never, never, never, never, never, never},
}

// checkProperty returns true if the transformation represented by the given
// property lookup table is allowed for the two given edges. Note that while
// most table entries are either true or false, some are conditionally true,
// depending on the null-rejecting properties of the edge filters (for example,
// association for two full joins).
func checkProperty(table [7][7]lookupTableEntry, edgeA, edgeB *edge) bool {
	entry := table[getOpIdx(edgeA)][getOpIdx(edgeB)]

	if entry == never {
		// Application of this transformation property is unconditionally incorrect.
		return false
	}
	if entry == always {
		// Application of this transformation property is unconditionally correct.
		return true
	}

	// This property is conditionally applicable. Get the relations that must be
	// null-rejected by the filters.
	var candidateNullRejectRels vertexSet
	if entry&rejectsOnLeftA != 0 {
		// Filters must null-reject on the left input vertexes of edgeA.
		candidateNullRejectRels = edgeA.op.leftVertices
	} else if entry&rejectsOnRightA != 0 {
		// Filters must null-reject on the right input vertexes of edgeA.
		candidateNullRejectRels = edgeA.op.rightVertices
	} else if entry&rejectsOnRightB != 0 {
		// Filters must null-reject on the right input vertexes of edgeB.
		candidateNullRejectRels = edgeA.op.rightVertices
	}

	// Check whether the edge filters reject nulls on nullRejectRelations.
	if entry&filterA != 0 {
		// The filters of edgeA must reject nulls on one or more of the relations in
		// nullRejectRelations.
		if !edgeA.nullRejectedRels.intersects(candidateNullRejectRels) {
			return false
		}
	}
	if entry&filterB != 0 {
		// The filters of edgeB must reject nulls on one or more of the relations in
		// nullRejectRelations.
		if !edgeB.nullRejectedRels.intersects(candidateNullRejectRels) {
			return false
		}
	}
	return true
}

// getOpIdx returns an index into the join property static lookup tables given an edge
// with an associated edge type. I originally used int(joinType), but this is fragile
// to reordering the type definitions.
func getOpIdx(e *edge) int {
	switch e.op.joinType {
	case CrossJoinType:
		return 0
	case InnerJoinType:
		return 1
	case SemiJoinType:
		return 2
	case AntiJoinType:
		return 3
	case LeftJoinType:
		return 4
	case FullOuterJoinType:
		return 5
	case GroupByJoinType:
		return 6
	default:
		panic(fmt.Sprintf("invalid operator: %v", e.op.joinType))
	}
}

type edgeSet = FastIntSet

type bitSet uint64

// vertexSet represents a set of base relations that form the vertexes of the
// join graph.
type vertexSet = bitSet

const maxSetSize = 63

// vertexIndex represents the ordinal position of a base relation in the
// JoinOrderBuilder vertexes field. vertexIndex must be less than maxSetSize.
type vertexIndex = uint64

// add returns a copy of the bitSet with the given element added.
func (s bitSet) add(idx uint64) bitSet {
	if idx > maxSetSize {
		panic(fmt.Sprintf("cannot insert %d into bitSet", idx))
	}
	return s | (1 << idx)
}

// union returns the set union of this set with the given set.
func (s bitSet) union(o bitSet) bitSet {
	return s | o
}

// intersection returns the set intersection of this set with the given set.
func (s bitSet) intersection(o bitSet) bitSet {
	return s & o
}

// difference returns the set difference of this set with the given set.
func (s bitSet) difference(o bitSet) bitSet {
	return s & ^o
}

// intersects returns true if this set and the given set intersect.
func (s bitSet) intersects(o bitSet) bool {
	return s.intersection(o) != 0
}

// isSubsetOf returns true if this set is a subset of the given set.
func (s bitSet) isSubsetOf(o bitSet) bool {
	return s.union(o) == o
}

// isSingleton returns true if the set has exactly one element.
func (s bitSet) isSingleton() bool {
	return s > 0 && (s&(s-1)) == 0
}

// next returns the next element in the set after the given start index, and
// a bool indicating whether such an element exists.
func (s bitSet) next(startVal uint64) (elem uint64, ok bool) {
	if startVal < maxSetSize {
		if ntz := bits.TrailingZeros64(uint64(s >> startVal)); ntz < 64 {
			return startVal + uint64(ntz), true
		}
	}
	return uint64(math.MaxInt64), false
}

// len returns the number of elements in the set.
func (s bitSet) len() int {
	return bits.OnesCount64(uint64(s))
}

func (s bitSet) String() string {
	var str string
	var i vertexSet = 1
	cnt := 0
	for cnt < s.len() {
		if (i & s) != 0 {
			str += "1"
			cnt++
		} else {
			str += "0"
		}
		i = i << 1
	}
	return str
}
