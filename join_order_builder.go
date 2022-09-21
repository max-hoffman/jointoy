package jointoy

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"strings"
)

// joinOrderBuilder enumerates valid plans for a join tree.  We build the join
// tree bottom up, first joining single nodes with join condition "edges", then
// single nodes to hypernodes (1+n), and finally hyper nodes to
// other hypernodes (n+m).
//
// Every valid combination of subtrees is considered with two exceptions.
//
// 1) Cross joins and other joins with degenerate predicates are never pushed
// lower in the tree.
//
// 2) Transformations that are valid but create degenerate filters (new
// cross joins) are not considered.
//
// The logic for this module is sourced from
// https://www.researchgate.net/publication/262216932_On_the_correct_and_complete_enumeration_of_the_core_search_space
// with help from
// https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/opt/xform/join_order_builder.go.
//
// Two theoretical observations underpin the enumeration:
//
// 1) Either associativity or l-asscom can be applied to left nesting, but not
// both. Either associativity or r-asscom can be applied to right nesting, but
// not both.
//
// 2) Every transformation in the core search space (of two operators) can be
// reached by one commutative transformation on each operator and one assoc,
// l-asscom, or r-asscom.
//
// We use this assumption to implement the dbSube enumeration search, using a
// CD-C conflict detection rules to encode reordering applicability:
//
// 1) Use bitsets to iterate all combinations of join plan subtrees, starting
// with two tables and building upwards. For example, a join A x B x C would
// start with 10 x 01 (A x B) and build up to sets 100 x 011 (A x (BC)) and 101
// x 010 ((AC) x B).
//
// 2) Attempt to make a new plan with every combination of subtrees
// (hypernodes) for every join operator. This takes the form (op s1 s2), where
// s1 is the right subtree, s2 is the left subtree, and op is the edge
// corresponding to a specific join type and filter. Most of the time one
// operator => one edge, except when join conjunctions are split to make
// multiple edges for one join operator. We differentiate innerEdges and
// nonInnerEdges to avoid innerJoins plans overwriting the often slower
// nonInner join plans. A successful edge between two sets adds a join plan to
// the memo.
//
// 3) Save the best plan for every memo group (unordered group of table joins)
// moving upwards through the tree, finishing with the optimal memo group for
// the join including every table.
//
// Applicability rules:
//
// We build applicibility rules before exhaustive enumeration to filter valid
// plans. We consider a reordering valid by checking:
//
// 1) Transform compatibility: a lookup table between two join operator types
//
// 2) Eligibility sets: left and right subtree dependencies required for a
// valid reordering. The syntactic eligibility set (SES) is the table
// dependencies of the edge's filter. For example, the SES for a filter a.x =
// b.y is (a,b).  The total eligibility set (TES) is an expansion of the SES
// that conceptually behaves similarly to an SES; i.e. only hypernodes that
// completely intersect an edge's TES are valid. The difference is that TES is
// expanded based on the original edge's join operator subdependencies.  The
// paper referenced encodes an algorithms for building a TES to fully encode
// associativity, left-asscom, and right-asscom (commutativity is a unary
// operator property).
//
// For example, the INNER JOIN in the query below is subject to assoc(left
// join, inner join) = false:
//
//	SELECT *
//	FROM (SELECT * FROM ab LEFT JOIN uv ON a = u)
//	INNER JOIN xy
//	ON x = v
//
// As a result, the inner join's TES, initialized as the SES(xy, uv), is
// expanded to include ab. The final TES(xy, uv, ab) invalidates
// LEFT JOIN association during exhaustive enumeration:
//
//	SELECT *
//	FROM (SELECT * FROM ab LEFT JOIN xy ON x = v)
//	INNER JOIN uv
//	ON a = u
//
// Note the disconnect between the global nature of the TES, which is built
// fully before enumeration, and local nature of testing every enumeration
// against the pre-computed TES.
//
// In special cases, the TES is not expressive enough to represent certain
// table dependencies. Conflict rules of the form R1 -> R2 are an escape hatch
// to require the dependency table set R2 when any subset of R1 is present in a
// candidate plan.
//
// TODO: transitive predicates
// TODO: null rejecting tables
// TODO: redundancy checks
// TODO: functional dependencies
type joinOrderBuilder struct {
	// plans maps from a set of base relations to the memo group for the join tree
	// that contains those relations (and only those relations). As an example,
	// the group for [xy, ab, uv] might contain the join trees (xy, (ab, uv)),
	// ((xy, ab), uv), (ab, (xy, uv)), etc.
	//
	// The group for a single base relation is simply the base relation itself.
	plans         map[vertexSet]sql.Node
	m             Memo
	edges         []edge
	vertices      []sql.Node
	vertexNames   []string
	innerEdges    edgeSet
	nonInnerEdges edgeSet
}

func newJoinOrderBuilder(m Memo) *joinOrderBuilder {
	return &joinOrderBuilder{
		plans: make(map[vertexSet]sql.Node),
		m:     m,
	}
}

func (j *joinOrderBuilder) reorderJoin(n sql.Node) {
	j.populateSubgraph(n)
	j.dbSube()
}

// populateSubgraph recursively tracks new join nodes as edges and new
// leaf nodes as vertices to the joinOrderBuilder graph, returning
// the subgraph's newly tracked vertices and edges.
func (j *joinOrderBuilder) populateSubgraph(n sql.Node) (vertexSet, edgeSet) {
	startV := j.allVertices()
	startE := j.allEdges()
	// build operator
	switch n := n.(type) {
	case plan.JoinNode, *plan.CrossJoin:
		// we're going to add an edge with this join condition and type
		// recursively build the children
		j.buildJoinOp(n)
	case analyzer.NameableNode:
		// base case add join tree leaf
		// initialize plan for vertex set with just this relation
		j.buildJoinLeaf(n)
	default:
		panic("join leafs should be nameable")
	}
	return j.allVertices().difference(startV), j.allEdges().Difference(startE)
}

func (j *joinOrderBuilder) buildJoinOp(n sql.Node) {
	bn := n.(sql.BinaryNode)
	leftV, leftE := j.populateSubgraph(bn.Left())
	rightV, rightE := j.populateSubgraph(bn.Right())
	var isInner bool
	var filter sql.Expression
	var typ JoinType
	switch n := n.(type) {
	case *plan.CrossJoin:
		typ = CrossJoinType
		isInner = true
	case *plan.InnerJoin:
		typ = InnerJoinType
		isInner = true
		filter = n.Cond
	case *plan.LeftJoin:
		typ = LeftJoinType
		filter = n.Cond
	case *plan.RightJoin:
		typ = LeftJoinType
		leftV, rightV = rightV, leftV
		leftE, rightE = rightE, leftE
		filter = n.Cond
	case *FullJoin:
		typ = FullOuterJoinType
		filter = n.filter
	case *SemiJoin:
		typ = SemiJoinType
		filter = n.filter
	case *AntiJoin:
		typ = AntiJoinType
		filter = n.filter
	case *GroupJoin:
		panic("not supported")
	default:
		panic("unreachable")
	}

	// make op
	op := &operator{
		joinType:      typ,
		leftVertices:  leftV,
		rightVertices: rightV,
		leftEdges:     leftE,
		rightEdges:    rightE,
	}
	if !isInner {
		j.buildNonInnerEdge(op, filter)
		return
	}
	j.buildInnerEdge(op, filter)
}

func (j *joinOrderBuilder) buildJoinLeaf(n analyzer.NameableNode) {
	j.checkSize()
	j.vertices = append(j.vertices, n)
	j.vertexNames = append(j.vertexNames, n.Name())

	// Initialize the plan for this vertex.
	idx := vertexIndex(len(j.vertices) - 1)
	relSet := vertexSet(0).add(idx)
	j.plans[relSet] = n
}

func (j *joinOrderBuilder) buildInnerEdge(op *operator, filter sql.Expression) {
	if filter == nil {
		// cross join
		j.edges = append(j.edges, *j.makeEdge(op, filter))
		j.innerEdges.Add(len(j.edges) - 1)
		return
	}
	if and, ok := filter.(*expression.And); ok {
		// edge for each conjunct
		j.buildInnerEdge(op, and.Left)
		j.buildInnerEdge(op, and.Right)
		return
	}
	j.edges = append(j.edges, *j.makeEdge(op, filter))
	j.innerEdges.Add(len(j.edges) - 1)
}

func (j *joinOrderBuilder) buildNonInnerEdge(op *operator, filter sql.Expression) {
	// only single edge for non-inner
	j.edges = append(j.edges, *j.makeEdge(op, filter))
	j.nonInnerEdges.Add(len(j.edges) - 1)
}

func (j *joinOrderBuilder) makeEdge(op *operator, filter sql.Expression) *edge {
	// edge is an instance of operator with a unique set of transform rules depending
	// on the subset of filters used
	e := &edge{
		op:     op,
		filter: filter,
	}
	// we build the graph upwards. so when me make this edge, all
	// of the dependency operators and edges have already been constructed
	e.populateEdgeProps(j.allVertices(), j.vertexNames, j.edges)
	return e
}

// checkSize prevents more than 64 tables
func (j *joinOrderBuilder) checkSize() {
}

// populateEligibilitySets expands TES beyond SES
func (j *joinOrderBuilder) populateEligibilitySets() {
}

// dpSube
func (j *joinOrderBuilder) dbSube() {
	all := j.allVertices()
	for subset := vertexSet(1); subset <= all; subset++ {
		if subset.isSingleton() {
			continue
		}
		for s1 := vertexSet(1); s1 <= subset/2; s1++ {
			if !s1.isSubsetOf(subset) {
				continue
			}
			s2 := subset.difference(s1)
			j.addPlans(s1, s2)
		}
	}
}

func setPrinter(all, s1, s2 vertexSet) {
	s1Arr := make([]string, all.len())
	for i := range s1Arr {
		s1Arr[i] = "0"
	}
	s2Arr := make([]string, all.len())
	for i := range s2Arr {
		s2Arr[i] = "0"
	}
	for idx, ok := s1.next(0); ok; idx, ok = s1.next(idx + 1) {
		s1Arr[idx] = "1"
	}
	for idx, ok := s2.next(0); ok; idx, ok = s2.next(idx + 1) {
		s2Arr[idx] = "1"
	}
	fmt.Printf("s1: %s, s2: %s\n", strings.Join(s1Arr, ""), strings.Join(s2Arr, ""))
}

// addPlans finds operators that let us join (s1 op s2) and (s2 op s1).
func (j *joinOrderBuilder) addPlans(s1, s2 vertexSet) {
	// all inner filters could be applied
	if j.plans[s1] == nil || j.plans[s2] == nil {
		// Both inputs must have plans.
		// need this to prevent cross-joins higher in tree
		return
	}

	//TODO collect all inner join filters that can be used as select filters
	//TODO collect functional dependencies to avoid redundant filters
	//TODO relational nodes track functional dependencies

	var innerJoinFilters []sql.Expression
	var addInnerJoin bool
	for i, ok := j.innerEdges.Next(0); ok; i, ok = j.innerEdges.Next(i + 1) {
		op := &j.edges[i]
		// Ensure that this edge forms a valid connection between the two sets.
		if op.applicable(s1, s2) {
			innerJoinFilters = append(innerJoinFilters, op.filter)
			addInnerJoin = true
		}
	}

	// transitive closure can accidentally replace nonInner op with inner op
	for i, ok := j.nonInnerEdges.Next(0); ok; i, ok = j.nonInnerEdges.Next(i + 1) {
		e := &j.edges[i]
		if e.applicable(s1, s2) {
			j.addJoin(e.op.joinType, s1, s2, []sql.Expression{e.filter}, innerJoinFilters)
			return
		}
		if e.applicable(s2, s1) {
			// This is necessary because we only iterate s1 up to subset / 2
			// in DPSube()
			j.addJoin(e.op.joinType, s2, s1, []sql.Expression{e.filter}, innerJoinFilters)
			return
		}
	}

	if addInnerJoin {
		// Construct an inner join. Don't add in the case when a non-inner join has
		// already been constructed, because doing so can lead to a case where a
		// non-inner join operator 'disappears' because an inner join has replaced
		// it.
		j.addJoin(InnerJoinType, s1, s2, innerJoinFilters, nil)
	}
}

func (j *joinOrderBuilder) addJoin(op JoinType, s1, s2 vertexSet, joinFilter, selFilters []sql.Expression) {
	if s1.intersects(s2) {
		panic("sets are not disjoint")
	}
	union := s1.union(s2)
	left := j.plans[s1]
	right := j.plans[s2]

	j.plans[union] = j.m.memoizeJoin(op, left, right, joinFilter, selFilters)
	if commute(op) {
		j.plans[union] = j.m.memoizeJoin(op, right, left, joinFilter, selFilters)
	}
}

func (j *joinOrderBuilder) allVertices() vertexSet {
	// all bits set to one
	return vertexSet((1 << len(j.vertices)) - 1)
}

func (j *joinOrderBuilder) allEdges() edgeSet {
	all := edgeSet{}
	for i := range j.edges {
		all.Add(i)
	}
	return all
}

// operator contains the properties of a join operator from the original join
// tree. It is used in calculating the total eligibility sets for edges from any
// 'parent' joins which were originally above this one in the tree.
type operator struct {
	// joinType is the operator type of the original join operator.
	joinType JoinType

	// leftVertices is the set of vertexes (base relations) that were in the left
	// input of the original join operator.
	leftVertices vertexSet

	// rightVertices is the set of vertexes (base relations) that were in the
	// right input of the original join operator.
	rightVertices vertexSet

	// leftEdges is the set of edges that were constructed from join operators
	// that were in the left input of the original join operator.
	leftEdges edgeSet

	// rightEdgers is the set of edges that were constructed from join operators
	// that were in the right input of the original join operator.
	rightEdges edgeSet
}

// edge is a generalization of a join edge that embeds rules for
// determining the applicability of arbitrary subtrees. An edge is added to the
// join graph when a new plan can be constructed between two vertexSet.
type edge struct {
	// op is the original join node source for the edge. there are multiple edges
	// per op for inner joins with conjunct-predicate join conditions. Different predicates
	// will have different conflict rules.
	op *operator

	// filter is the set of join filter that will be used to construct new join
	// ON conditions.
	filter   sql.Expression
	freeVars []*expression.GetField

	// nullRejectedRels is the set of vertexes on which nulls are rejected by the
	// filters. We do not set any nullRejectedRels currently, which is not accurate
	// but prevents potentially invalid transformations.
	nullRejectedRels vertexSet

	// ses is the syntactic eligibility set of the edge; in other words, it is the
	// set of base relations (tables) referenced by the filters field.
	ses vertexSet

	// tes is the total eligibility set of the edge. The TES gives the set of base
	// relations (vertexes) that must be in the input of any join that uses the
	// filters from this edge in its ON condition. The TES is initialized with the
	// SES, and then expanded by the conflict detection algorithm.
	tes vertexSet

	// rules is a set of conflict rules which must evaluate to true in order for
	// a join between two sets of vertexes to be valid.
	rules []conflictRule
}

func (e *edge) populateEdgeProps(allV vertexSet, allNames []string, edges []edge) {
	var cols []*expression.GetField
	var nullAccepting []sql.Expression
	if e.filter != nil {
		transform.InspectExpr(e.filter, func(e sql.Expression) bool {
			// TODO this needs to separate null accepting before plucking getField
			switch e := e.(type) {
			case *expression.GetField:
				cols = append(cols, e)
			case *expression.NullSafeEquals, *expression.NullSafeGreaterThan, *expression.NullSafeLessThan,
				*expression.NullSafeGreaterThanOrEqual, *expression.NullSafeLessThanOrEqual, *expression.IsNull:
				nullAccepting = append(nullAccepting, e)
			default:
			}
			return false
		})
	}

	e.freeVars = cols

	// TODO implement, we currently limit transforms assuming no strong null safety
	//e.nullRejectedRels = e.nullRejectingTables(nullAccepting, allNames, allV)

	//SES is vertexSet of all tables referenced in cols
	e.calcSES(cols, allNames)
	// use CD-C to expand dependency sets for operators
	// front load preventing applicable operators that would push crossjoins
	e.calcTES(edges)
}

func (e *edge) String() string {
	b := strings.Builder{}
	b.WriteString("edge\n")
	b.WriteString(fmt.Sprintf("  - joinType: %s\n", e.op.joinType.String()))
	if e.filter != nil {
		b.WriteString(fmt.Sprintf("  - on: %s\n", e.filter.String()))
	}
	freeVars := make([]string, len(e.freeVars))
	for i, v := range e.freeVars {
		freeVars[i] = v.String()
	}
	b.WriteString(fmt.Sprintf("  - free vars: %s\n", e.freeVars))
	b.WriteString(fmt.Sprintf("  - ses: %s\n", e.ses.String()))
	b.WriteString(fmt.Sprintf("  - tes: %s\n", e.tes.String()))
	b.WriteString(fmt.Sprintf("  - nullRej: %s\n", e.nullRejectedRels.String()))
	return b.String()
}

// nullRejectingTables is a subset of the SES such that for every
// null rejecting table, if all attributes of the table are null,
// we can make a strong guarantee that the edge filter will not
// evaluate to TRUE (FALSE or NULL are OK).
//
// For example, the filter (a.x = b.x OR a.x IS NOT NULL) is null
// rejecting on (b), but not (a).
//
// A second more complicated example is null rejecting both on (a,b):
//
//	CASE
//	  WHEN a.x IS NOT NULL THEN a.x = b.x
//	  WHEN a.x <=> 2 THEN TRUE
//	  ELSE NULL
//	END
//
// Refer to https://dl.acm.org/doi/10.1145/244810.244812 for more examples.
// TODO implement this
func (e *edge) nullRejectingTables(nullAccepting []sql.Expression, allNames []string, allV vertexSet) vertexSet {
	panic("not implemented")
}

// calcSES updates the syntactic eligibility set for an edge. An SES
// represents all tables this edge's filter requires as input.
func (e *edge) calcSES(cols []*expression.GetField, allNames []string) {
	ses := vertexSet(0)
	for _, e := range cols {
		for i, n := range allNames {
			if n == e.Table() {
				ses = ses.add(vertexIndex(i))
			}
		}
	}
	e.ses = ses
}

// calcTES in place updates an edge's total eligibility set. TES is a way
// to expand the eligibility sets (the table dependencies) for an edge to
// prevent invalid plans.
func (e *edge) calcTES(edges []edge) {
	e.tes = e.ses

	// Degenerate predicates include 1) cross joins and 2) inner joins
	// whose filters do not restrict that cardinality of the subtree
	// inputs. We check for both by comparing i) the filter SES to ii) the tables
	// provided by the left/right subtrees. If one or both do not overlap,
	// the degenerate edge will be frozen in reference to the original plan
	// by expanding the TES to require the left/right subtree dependencies.
	//
	//	 note: this is different from the paper, which instead adds a check
	//   to applicable:
	//     op.leftVertices.intersect(s1) || op.rightVertices.intersect(s2)
	//   An operation is only applicable if the left tree provides a subset
	//   of s1 or the right tree provides a subset of s2. This is logically
	//   equivalent to expanding the TES here, but front-loads this logic
	//   because a bigger TES earlier reduces the conflict checking work.
	if !e.tes.intersects(e.op.leftVertices) {
		e.tes = e.tes.union(e.op.leftVertices)
	}
	if !e.tes.intersects(e.op.rightVertices) {
		e.tes = e.tes.union(e.op.rightVertices)
	}

	// CD-C algorithm
	// Note: the ordering of the transform(eA, eB) functions are important.
	// eA is the subtree child edge targeted for rearrangement. If the ordering
	// is switched, the output is nondeterministic.

	// iterate every eA in STO(left(eB))
	eB := e
	for idx, ok := eB.op.leftEdges.Next(0); ok; idx, ok = eB.op.leftEdges.Next(idx + 1) {
		if eB.op.leftVertices.isSubsetOf(eB.tes) {
			// Fast path to break out early: the TES includes all relations from the
			// left input.
			break
		}
		eA := &edges[idx]
		if !assoc(eA, eB) {
			// The edges are not associative, so add a conflict rule mapping from the
			// right input relations of the child to its left input relations.
			rule := conflictRule{from: eA.op.rightVertices}
			if eA.op.leftVertices.intersects(eA.ses) {
				// A less restrictive conflict rule can be added in this case.
				rule.to = eA.op.leftVertices.intersection(eA.ses)
			} else {
				rule.to = eA.op.leftVertices
			}
			eB.addRule(rule)
		}
		if !leftAsscom(eA, eB) {
			// Left-asscom does not hold, so add a conflict rule mapping from the
			// left input relations of the child to its right input relations.
			rule := conflictRule{from: eA.op.leftVertices}
			if eA.op.rightVertices.intersects(eA.ses) {
				// A less restrictive conflict rule can be added in this case.
				rule.to = eA.op.rightVertices.intersection(eA.ses)
			} else {
				rule.to = eA.op.rightVertices
			}
			eB.addRule(rule)
		}
	}

	for idx, ok := e.op.rightEdges.Next(0); ok; idx, ok = e.op.rightEdges.Next(idx + 1) {
		if e.op.rightVertices.isSubsetOf(e.tes) {
			// Fast path to break out early: the TES includes all relations from the
			// right input.
			break
		}
		eA := &edges[idx]
		if !assoc(eB, eA) {
			// The edges are not associative, so add a conflict rule mapping from the
			// left input relations of the child to its right input relations.
			rule := conflictRule{from: eA.op.leftVertices}
			if eA.op.rightVertices.intersects(eA.ses) {
				// A less restrictive conflict rule can be added in this case.
				rule.to = eA.op.rightVertices.intersection(eA.ses)
			} else {
				rule.to = eA.op.rightVertices
			}
			eB.addRule(rule)
		}
		if !rightAsscom(eB, eA) {
			// Right-asscom does not hold, so add a conflict rule mapping from the
			// right input relations of the child to its left input relations.
			rule := conflictRule{from: eA.op.rightVertices}
			if eA.op.leftVertices.intersects(eA.ses) {
				// A less restrictive conflict rule can be added in this case.
				rule.to = eA.op.leftVertices.intersection(eA.ses)
			} else {
				rule.to = eA.op.leftVertices
			}
			eB.addRule(rule)
		}
	}
}

// addRule adds the given conflict rule to the edge. Before the rule is added to
// the rules set, an effort is made to eliminate the need for the rule.
func (e *edge) addRule(rule conflictRule) {
	if rule.from.intersects(e.tes) {
		// If the 'from' relation set intersects the total eligibility set, simply
		// add the 'to' set to the TES because the rule will always be triggered.
		e.tes = e.tes.union(rule.to)
		return
	}
	if rule.to.isSubsetOf(e.tes) {
		// If the 'to' relation set is a subset of the total eligibility set, the
		// rule is a do-nothing.
		return
	}
	e.rules = append(e.rules, rule)
}

func (e *edge) applicable(s1, s2 vertexSet) bool {
	if !e.checkRules(s1, s2) {
		// The conflict rules for this edge are not satisfied for a join between s1
		// and s2.
		return false
	}
	switch e.op.joinType {
	case InnerJoinType:
		// The TES must be a subset of the relations of the candidate join inputs. In
		// addition, the TES must intersect both s1 and s2 (the edge must connect the
		// two vertex sets).
		return e.tes.isSubsetOf(s1.union(s2)) && e.tes.intersects(s1) && e.tes.intersects(s2)
	default:
		// The left TES must be a subset of the s1 relations, and the right TES must
		// be a subset of the s2 relations. In addition, the TES must intersect both
		// s1 and s2 (the edge must connect the two vertex sets).
		return e.tes.intersection(e.op.leftVertices).isSubsetOf(s1) &&
			e.tes.intersection(e.op.rightVertices).isSubsetOf(s2) &&
			e.tes.intersects(s1) && e.tes.intersects(s2)
	}
}

// checkRules iterates through the edge's rules and returns false if a conflict
// is detected for the given sets of join input relations. Otherwise, returns
// true.
func (e *edge) checkRules(s1, s2 vertexSet) bool {
	s := s1.union(s2)
	for _, rule := range e.rules {
		if rule.from.intersects(s) && !rule.to.isSubsetOf(s) {
			// The join is invalid because it does not obey this conflict rule.
			return false
		}
	}
	return true
}

// joinIsRedundant returns true if a join between the two sets of base relations
// was already present in the original join tree. If so, enumerating this join
// would be redundant, so it should be skipped.
func (e *edge) joinIsRedundant(s1, s2 vertexSet) bool {
	return e.op.leftVertices == s1 && e.op.rightVertices == s2
}

type assocTransform func(eA, eB *edge) bool

// assoc checks whether the associate is applicable
// to a binary operator tree. We consider 1) generating cross joins,
// and 2) the left/right operator join types for this specific transform.
// The below is a valid association that generates no crossjoin:
//
//	(e2 op_a_12 e1) op_b_13 e3
//	=>
//	e2 op_a_12 (e1 op_b_13 e3)
//
// note: important to compare edge ordering for left deep tree.
func assoc(eA, eB *edge) bool {
	if eB.ses.intersects(eA.op.leftVertices) || eA.ses.intersects(eB.op.rightVertices) {
		// associating two operators can estrange the distant relation.
		// for example:
		//   (e2 op_a_12 e1) op_b_13 e3
		//   =>
		//   e2 op_a_12 (e1 op_b_13 e3)
		// The first operator, a, takes explicit dependencies on e1 and e2.
		// The second operator, b, takes explicit dependencies on e1 and e3.
		// Associating these two will isolate e2 from op_b for the downward
		// transform, and e3 from op_a on the upward transform, both of which
		// are valid. The same is not true for the transform below:
		//   (e2 op_a_12 e1) op_b_32 e3
		//   =>
		//   e2 op_a_12 (e1 op_b_32 e3)
		// Isolating e2 from op_b makes op_b degenerate, producing a cross join.
		return false
	}
	return checkProperty(assocTable, eA, eB)
}

// leftAsscom checks whether the left-associate+commute is applicable
// to a binary operator tree. We consider 1) generating cross joins,
// and 2) the left/right operator join types for this specific transform.
// For example:
//
//	(e1 op_a_12 e2) op_b_13 e3
//	=>
//	(e1 op_b_13 e3) op_a_12 e2
func leftAsscom(eA, eB *edge) bool {
	if eB.ses.intersects(eA.op.rightVertices) || eA.ses.intersects(eB.op.rightVertices) {
		// Associating two operators can estrange the distant relation.
		// For example:
		//	(e1 op_a_12 e2) op_b_23 e3
		//	=>
		//	(e1 op_b_23 e3) op_a_12 e2
		// Isolating e2 from op_b makes op_b degenerate, producing a cross join.
		return false
	}
	return checkProperty(leftAsscomTable, eA, eB)
}

// rAsscom checks whether the left-associate+commute is applicable
// to a binary operator tree. We consider 1) generating cross joins,
// and 2) the left/right operator join types for this specific transform.
// For example:
//
//	e3 op_b_13 (e1 op_a_12 e2)
//	=>
//	e2 op_a_12 (e1 op_b_13 e3)
func rightAsscom(eA, eB *edge) bool {
	if eB.ses.intersects(eA.op.leftVertices) || eA.ses.intersects(eB.op.leftVertices) {
		// Associating two operators can estrange the distant relation.
		// For example:
		//	e3 op_b_23 (e1 op_a_12 e3)
		//	=>
		//	e2 op_a_12 (e1 op_b_23 e3)
		// Isolating e2 from op_b makes op_b degenerate, producing a cross join.
		return false
	}
	return checkProperty(rightAsscomTable, eA, eB)
}

// commute transforms an operator tree by alternating child
// join ordering.
// For example:
//
//	e1 op e2
//	=>
//	e2 op e1
func (e *edge) commute() bool {
	switch e.op.joinType {
	case InnerJoinType, CrossJoinType:
		return true
	default:
		return false
	}

}
