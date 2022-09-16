package jointoy

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// conflict detectors; calculate SES and TES

// conflict rule sets
//   - representation
//   - execution

// applicability rules (depend on TES, join types, s1 and s2)

// redundancy checks
// selection filters
// transitive closure
// functional deps

// exhaust plans
//  - try all sets, building upwards
//  - for each set, build non or inner join
//    - separation lets us cut space in half

func newJoinOrderBuilder() *joinOrderBuilder {
	return &joinOrderBuilder{
		plans: make(map[vertexSet]sql.Node),
	}
}

type joinOrderBuilder struct {
	// plans maps from a set of base relations to the memo group for the join tree
	// that contains those relations (and only those relations). As an example,
	// the group for [xy, ab, uv] might contain the join trees (xy, (ab, uv)),
	// ((xy, ab), uv), (ab, (xy, uv)), etc.
	//
	// The group for a single base relation is simply the base relation itself.
	plans         map[vertexSet]sql.Node
	edges         []edge
	vertices      []sql.Node
	vertexNames   []string
	innerEdges    edgeSet
	nonInnerEdges edgeSet
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

// addPlans finds operators that let us join (s1 op s2) and (s2 op s1).
func (j *joinOrderBuilder) addPlans(s1, s2 vertexSet) {
	// all inner filters could be applied
	if j.plans[s1] == nil || j.plans[s2] == nil {
		// Both inputs must have plans.
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
	//memoize the join plan
	var join string
	switch op {
	case InnerJoinType:
		join = "Inner"
	case LeftJoinType:
		join = "Left"
	}
	var s1Names string
	var rep string
	for idx, ok := s1.next(0); ok; idx, ok = s1.next(idx + 1) {
		s1Names += rep
		s1Names += j.vertexNames[idx]
		rep = ", "
	}
	var s2Names string
	rep = ""
	for idx, ok := s1.next(0); ok; idx, ok = s2.next(idx + 1) {
		s2Names += rep
		s2Names += j.vertexNames[idx]
		rep = ", "
	}
	fmt.Printf("%s\n  - s1: %s\n  - s2: %s\n  - filter: %s\n  - sel: %s\n", join, s1Names, s2Names, joinFilter, selFilters)
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
	freeVars []*expression.UnresolvedColumn

	// nullRejectedRels is the set of vertexes on which nulls are rejected by the
	// filters.
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
	var cols []*expression.UnresolvedColumn
	var nullAccepting []sql.Expression
	transform.InspectExpr(e.filter, func(e sql.Expression) bool {
		switch e := e.(type) {
		case *expression.UnresolvedColumn:
			cols = append(cols, e)
		case *expression.NullSafeEquals, *expression.NullSafeGreaterThan, *expression.NullSafeLessThan,
			*expression.NullSafeGreaterThanOrEqual, *expression.NullSafeLessThanOrEqual, *expression.IsNull:
			nullAccepting = append(nullAccepting, e)
		default:
		}
		return false
	})
	e.freeVars = cols

	// null rejection is all vertex set - rejecting vertex set
	e.nullRejectedRels = e.nullRejectingTables(nullAccepting, allNames, allV)
	//SES is vertexSet of all tables referenced in cols
	e.calcSES(cols, allNames)
	// use CD-C to expand dependency sets for operators
	// front load preventing applicable operators that would push crossjoins
	e.calcTES(edges)

	fmt.Sprint(e.tes)
}

func (e *edge) nullRejectingTables(nullAccepting []sql.Expression, allNames []string, allV vertexSet) vertexSet {
	var nullableSet vertexSet
	for _, e := range nullAccepting {
		transform.InspectExpr(e, func(e sql.Expression) bool {
			if e, ok := e.(*expression.UnresolvedColumn); ok {
				for i, n := range allNames {
					if n == e.Name() {
						nullableSet.add(vertexIndex(i))
					}
				}
			}
			return true
		})
	}
	return allV.difference(nullableSet)
}

// calcSES updates the syntactic eligibility set for an edge. An SES
// represents all tables this edge's filter requires as input.
func (e *edge) calcSES(cols []*expression.UnresolvedColumn, allNames []string) {
	var ses vertexSet
	for _, e := range cols {
		for i, n := range allNames {
			if n == e.Name() {
				ses.add(vertexIndex(i))
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

	// regular CD-C algorithm
	// iterate left edges
	for idx, ok := e.op.leftEdges.Next(0); ok; idx, ok = e.op.leftEdges.Next(idx + 1) {
		if e.op.leftVertices.isSubsetOf(e.tes) {
			// Fast path to break out early: the TES includes all relations from the
			// left input.
			break
		}
		child := &edges[idx]
		if !e.assoc(child) {
			// The edges are not associative, so add a conflict rule mapping from the
			// right input relations of the child to its left input relations.
			rule := conflictRule{from: child.op.rightVertices}
			if child.op.leftVertices.intersects(child.ses) {
				// A less restrictive conflict rule can be added in this case.
				rule.to = child.op.leftVertices.intersection(child.ses)
			} else {
				rule.to = child.op.leftVertices
			}
			e.addRule(rule)
		}
		if !e.lAsscom(child) {
			// Left-asscom does not hold, so add a conflict rule mapping from the
			// left input relations of the child to its right input relations.
			rule := conflictRule{from: child.op.leftVertices}
			if child.op.rightVertices.intersects(child.ses) {
				// A less restrictive conflict rule can be added in this case.
				rule.to = child.op.rightVertices.intersection(child.ses)
			} else {
				rule.to = child.op.rightVertices
			}
			e.addRule(rule)
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

// asscom checks whether the associate is applicable
// to a binary operator tree. We consider 1) generating cross joins,
// and 2) the left/right operator join types for this specific transform.
// The below is a valid association that generates no crossjoin:
//
//	(e2 op_a_12 e1) op_b_13 e3
//	=>
//	e2 op_a_12 (e1 op_b_13 e3)
func (e *edge) assoc(edgeB *edge) bool {
	if edgeB.ses.intersects(e.op.leftVertices) || e.ses.intersects(edgeB.op.rightVertices) {
		// associating two operators can estrange the distant relation.
		// for example:
		//   (e2 op_a_12 e1) op_b_13 e3
		//   =>
		//   e2 op_a_12 (e1 op_b_13 e3)
		// The first operator, a, takes explicit dependencies on e1 and e2.
		// The second operator, b, takes explicit dependencies on e1 ans e3.
		// Associating these two will isolate e2 from op_b for the downward
		// transform, and e3 from op_a on the upward transform, both of which
		// are valid. The same is not true for the transform below:
		//   (e2 op_a_12 e1) op_b_32 e3
		//   =>
		//   e2 op_a_12 (e1 op_b_32 e3)
		// Isolating e2 from op_b makes op_b degenerate, producing a cross join.
		return false
	}
	return checkProperty(assocTable, e, edgeB)
}

// lAsscom checks whether the left-associate+commute is applicable
// to a binary operator tree. We consider 1) generating cross joins,
// and 2) the left/right operator join types for this specific transform.
// For example:
//
//	(e1 op_a_12 e2) op_b_13 e3
//	=>
//	(e1 op_b_13 e3) op_a_12 e2
func (e *edge) lAsscom(edgeB *edge) bool {
	if edgeB.ses.intersects(e.op.rightVertices) || e.ses.intersects(edgeB.op.rightVertices) {
		// Associating two operators can estrange the distant relation.
		// For example:
		//	(e1 op_a_12 e2) op_b_23 e3
		//	=>
		//	(e1 op_b_23 e3) op_a_12 e2
		// Isolating e2 from op_b makes op_b degenerate, producing a cross join.
		return false
	}
	return checkProperty(leftAsscomTable, e, edgeB)
}

// rAsscom checks whether the left-associate+commute is applicable
// to a binary operator tree. We consider 1) generating cross joins,
// and 2) the left/right operator join types for this specific transform.
// For example:
//
//	e3 op_b_13 (e1 op_a_12 e2)
//	=>
//	e2 op_a_12 (e1 op_b_13 e3)
func (e *edge) rAsscom(edgeB *edge) bool {
	if edgeB.ses.intersects(e.op.leftVertices) || e.ses.intersects(edgeB.op.rightVertices) {
		// Associating two operators can estrange the distant relation.
		// For example:
		//	e3 op_b_23 (e1 op_a_12 e3)
		//	=>
		//	e2 op_a_12 (e1 op_b_23 e3)
		// Isolating e2 from op_b makes op_b degenerate, producing a cross join.
		return false
	}
	return checkProperty(leftAsscomTable, e, edgeB)
}

func (e *edge) commute() bool {
	switch e.op.joinType {
	case InnerJoinType, CrossJoinType:
		return true
	default:
		return false
	}

}
