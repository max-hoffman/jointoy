package jointoy

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestJoinOrderBuilder(t *testing.T) {
	childSchema := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "i", Type: sql.Int64, Nullable: true},
		{Name: "s", Type: sql.Text, Nullable: true},
	})
	child := memory.NewTable("test", childSchema, nil)

	//TODO need transform tests
	//  - make sure we get comm, assoc, lassoc, rassoc
	// TODO need preventative tests
	//  - block crossjoins for each transform
	//  - make sure TES blocks invalid transforms for each
	//  - make sure we don't commute LEFT JOIN or JSON_TABLE

	tests := []struct {
		in   sql.Node
		name string
	}{
		{
			name: "starter",
			in: plan.NewInnerJoin(
				plan.NewInnerJoin(
					plan.NewInnerJoin(
						plan.NewTableAlias(
							"a",
							plan.NewResolvedTable(child, nil, nil),
						),
						plan.NewTableAlias(
							"b",
							plan.NewResolvedTable(child, nil, nil),
						),
						expression.NewEquals(
							expression.NewGetFieldWithTable(0, sql.Int64, "a", "i", false),
							expression.NewGetFieldWithTable(0, sql.Int64, "b", "i", false),
						),
					),
					plan.NewTableAlias(
						"c",
						plan.NewResolvedTable(child, nil, nil),
					), expression.NewEquals(
						expression.NewGetFieldWithTable(0, sql.Int64, "b", "i", false),
						expression.NewGetFieldWithTable(0, sql.Int64, "c", "i", false)),
				),
				plan.NewTableAlias(
					"d",
					plan.NewResolvedTable(child, nil, nil),
				),
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "c", "i", false),
					expression.NewGetFieldWithTable(0, sql.Int64, "d", "i", false)),
			),
		},
	}

	for _, tt := range tests {
		j := newJoinOrderBuilder()
		j.reorderJoin(tt.in)

		// TODO verify SES

		// TODO verify TES

		// TODO verify null rejecting

		// TODO add dummy memo, collect all plans
		// check valid and invalid

		// TODO verify optimal plan
	}
}

func TestJoinOrderBuilder_populateSubgraph2(t *testing.T) {
	childSchema := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "i", Type: sql.Int64, Nullable: true},
		{Name: "s", Type: sql.Text, Nullable: true},
	})
	child := memory.NewTable("test", childSchema, nil)

	tests := []struct {
		name     string
		join     sql.Node
		expEdges []edge
	}{
		{
			name: "cross join",
			join: plan.NewCrossJoin(
				tableNode(child, "a"),
				plan.NewInnerJoin(
					tableNode(child, "b"),
					plan.NewLeftJoin(
						tableNode(child, "c"),
						tableNode(child, "d"),
						newEq("c.x=d.x"),
					),
					newEq("b.y=d.y"),
				),
			),
			expEdges: []edge{
				newEdge2(LeftJoinType, "0011", "0011", "0010", "0001", nil, newEq("c.x=d.x"), ""),  // C x D
				newEdge2(InnerJoinType, "0101", "0111", "0100", "0011", nil, newEq("b.y=d.y"), ""), // B x (CD)
				newEdge2(CrossJoinType, "0000", "1111", "1000", "0111", nil, nil, ""),              // A x (BCD)
			},
		},
		{
			name: "right deep left join",
			join: plan.NewInnerJoin(
				tableNode(child, "a"),
				plan.NewInnerJoin(
					tableNode(child, "b"),
					plan.NewLeftJoin(
						tableNode(child, "c"),
						tableNode(child, "d"),
						newEq("c.x=d.x"),
					),
					newEq("b.y=d.y"),
				),
				newEq("a.z=b.z"),
			),
			expEdges: []edge{
				newEdge2(LeftJoinType, "0011", "0011", "0010", "0001", nil, newEq("c.x=d.x"), ""),                                                                     // C x D
				newEdge2(InnerJoinType, "0101", "0111", "0100", "0011", nil, newEq("b.y=d.y"), ""),                                                                    // B x (CD)
				newEdge2(InnerJoinType, "1100", "1100", "1000", "0111", []conflictRule{{from: newVertexSet("0001"), to: newVertexSet("0010")}}, newEq("a.z=b.z"), ""), // A x (BCD)
			},
		},
		{
			name: "bushy left joins",
			join: plan.NewLeftJoin(
				plan.NewLeftJoin(
					tableNode(child, "a"),
					tableNode(child, "b"),
					newEq("a.x=b.x"),
				),
				plan.NewLeftJoin(
					tableNode(child, "c"),
					tableNode(child, "d"),
					newEq("c.x=d.x"),
				),
				newEq("b.y=c.y"),
			),
			expEdges: []edge{
				newEdge2(LeftJoinType, "1100", "1100", "1000", "0100", nil, newEq("a.x=b.x"), ""), // A x B
				newEdge2(LeftJoinType, "0011", "0011", "0010", "0001", nil, newEq("c.x=d.x"), ""), // C x D
				newEdge2(LeftJoinType, "0110", "1111", "1100", "0011", nil, newEq("b.y=c.y"), ""), // (AB) x (CD)
			},
		},
		{
			// SELECT *
			// FROM (SELECT * FROM A INNER JOIN B ON True)
			// FULL JOIN (SELECT * FROM C INNER JOIN D ON True)
			// ON A.x = C.x
			name: "degenerate inner join",
			join: NewFullJoin(
				plan.NewInnerJoin(
					tableNode(child, "a"),
					tableNode(child, "b"),
					expression.NewLiteral(true, sql.Boolean),
				),
				plan.NewInnerJoin(
					tableNode(child, "c"),
					tableNode(child, "d"),
					expression.NewLiteral(true, sql.Boolean),
				),
				newEq("a.x=c.x"),
			),
			expEdges: []edge{
				newEdge2(InnerJoinType, "0000", "1100", "1000", "0100", nil, expression.NewLiteral(true, sql.Boolean), ""), // A x B
				newEdge2(InnerJoinType, "0000", "0011", "0010", "0001", nil, expression.NewLiteral(true, sql.Boolean), ""), // C x D
				newEdge2(FullOuterJoinType, "1010", "1111", "1100", "0011", nil, newEq("a.x=c.x"), ""),                     // (AB) x (CD)
			},
		},
		{
			// SELECT * FROM A
			// WHERE EXISTS
			// (
			//   SELECT * FROM B
			//   LEFT JOIN C ON B.x = C.x
			//   WHERE A.y = B.y
			// )
			// note: left join is the right child
			name: "semi join",
			join: NewSemiJoin(
				plan.NewLeftJoin(
					tableNode(child, "b"),
					tableNode(child, "c"),
					newEq("b.x=c.x"),
				),
				tableNode(child, "a"),
				newEq("a.y=b.y"),
			),
			expEdges: []edge{
				newEdge2(LeftJoinType, "110", "110", "100", "010", nil, newEq("b.x=c.x"), ""), // B x C
				newEdge2(SemiJoinType, "101", "101", "110", "001", nil, newEq("a.y=b.y"), ""), // A x (BC)
			},
		},
		{
			name: "null rejecting",
			join: plan.NewCrossJoin(
				tableNode(child, "a"),
				plan.NewInnerJoin(
					tableNode(child, "b"),
					plan.NewLeftJoin(
						tableNode(child, "c"),
						tableNode(child, "d"),
						newEq("c.x=d.x"),
					),
					newEq("b.y=d.y"),
				),
			),
			expEdges: []edge{
				newEdge2(LeftJoinType, "0011", "0011", "0010", "0001", nil, newEq("c.x=d.x"), ""),  // C x D
				newEdge2(InnerJoinType, "0101", "0111", "0100", "0011", nil, newEq("b.y=d.y"), ""), // B x (CD)
				newEdge2(CrossJoinType, "0000", "1111", "1000", "0111", nil, nil, ""),              // A x (BCD)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newJoinOrderBuilder()
			b.populateSubgraph(tt.join)
			edgesEq(t, tt.expEdges, b.edges)
		})
	}
}

func tableNode(t sql.Table, name string) sql.Node {
	return plan.NewTableAlias(
		name,
		plan.NewResolvedTable(t, nil, nil),
	)
}

func newEq(eq string) sql.Expression {
	vars := strings.Split(eq, "=")
	if len(vars) > 2 {
		panic("invalid equal expression")
	}
	left := strings.Split(vars[0], ".")
	right := strings.Split(vars[1], ".")
	return expression.NewEquals(
		expression.NewGetFieldWithTable(0, sql.Int64, left[0], left[1], false),
		expression.NewGetFieldWithTable(0, sql.Int64, right[0], right[1], false),
	)
}

func TestAssociativeTransforms(t *testing.T) {
	// Sourced from Figure 3
	// each test has a reversible pair test which is a product of its transform
	validTests := []struct {
		name      string
		eA        *edge
		eB        *edge
		transform assocTransform
		rev       bool
	}{
		{
			name:      "assoc(a,b)",
			eA:        newEdge(InnerJoinType, "110", "010", "100"),
			eB:        newEdge(InnerJoinType, "101", "110", "001"),
			transform: assoc,
		},
		{
			name:      "assoc(b,a)",
			eA:        newEdge(InnerJoinType, "010", "101", "010"),
			eB:        newEdge(InnerJoinType, "101", "001", "100"),
			transform: assoc,
			rev:       true,
		},
		{
			name:      "r-asscom(a,b)",
			eA:        newEdge(InnerJoinType, "110", "010", "100"),
			eB:        newEdge(InnerJoinType, "101", "001", "110"),
			transform: rightAsscom,
		},
		{
			name:      "r-asscom(b,a)",
			eA:        newEdge(InnerJoinType, "110", "010", "101"),
			eB:        newEdge(InnerJoinType, "101", "001", "100"),
			transform: rightAsscom,
			rev:       true,
		},
		{
			name:      "l-asscom(a,b)",
			eA:        newEdge(InnerJoinType, "110", "100", "010"),
			eB:        newEdge(InnerJoinType, "101", "110", "001"),
			transform: leftAsscom,
		},
		{
			name:      "l-asscom(b,a)",
			eA:        newEdge(InnerJoinType, "110", "101", "010"),
			eB:        newEdge(InnerJoinType, "101", "100", "001"),
			transform: leftAsscom,
			rev:       true,
		},
		{
			name:      "assoc(a,b)",
			eA:        newEdge(InnerJoinType, "110", "010", "100"),
			eB:        newEdge(LeftJoinType, "101", "110", "001"),
			transform: assoc,
		},
		// l-asscom is OK with everything but full outerjoin w/ null rejecting A(e1).
		// Refer to rule table.
		{
			name:      "l-asscom(a,b)",
			eA:        newEdge(LeftJoinType, "110", "100", "010"),
			eB:        newEdge(InnerJoinType, "101", "110", "001"),
			transform: leftAsscom,
		},
		{
			name:      "l-asscom(b,a)",
			eA:        newEdge(LeftJoinType, "110", "101", "010"),
			eB:        newEdge(LeftJoinType, "101", "100", "001"),
			transform: leftAsscom,
			rev:       true,
		},
		// TODO special case operators
	}

	for _, tt := range validTests {
		t.Run(fmt.Sprintf("OK %s", tt.name), func(t *testing.T) {
			var res bool
			if tt.rev {
				res = tt.transform(tt.eB, tt.eA)
			} else {
				res = tt.transform(tt.eA, tt.eB)
			}
			require.True(t, res)
		})
	}

	invalidTests := []struct {
		name      string
		eA        *edge
		eB        *edge
		transform assocTransform
		rev       bool
	}{
		// most transforms are invalid, these are also from Figure 3
		{
			name:      "assoc(a,b)",
			eA:        newEdge(InnerJoinType, "110", "010", "100"),
			eB:        newEdge(InnerJoinType, "101", "001", "100"),
			transform: assoc,
		},
		{
			name:      "r-asscom(a,b)",
			eA:        newEdge(InnerJoinType, "110", "010", "100"),
			eB:        newEdge(InnerJoinType, "101", "100", "010"),
			transform: rightAsscom,
		},
		{
			name:      "l-asscom(a,b)",
			eA:        newEdge(InnerJoinType, "110", "010", "100"),
			eB:        newEdge(InnerJoinType, "101", "001", "100"),
			transform: leftAsscom,
		},
		// these are correct transforms with cross or inner joins, but invalid
		// with other operators
		{
			name:      "assoc(a,b)",
			eA:        newEdge(LeftJoinType, "110", "010", "100"),
			eB:        newEdge(InnerJoinType, "101", "110", "001"),
			transform: assoc,
		},
		{
			// this one depends on rejecting nulls on A(e2)
			name:      "left join assoc(b,a)",
			eA:        newEdge(LeftJoinType, "010", "101", "010"),
			eB:        newEdge(LeftJoinType, "101", "001", "100"),
			transform: assoc,
			rev:       true,
		},
		{
			name:      "left join r-asscom(a,b)",
			eA:        newEdge(LeftJoinType, "110", "010", "100"),
			eB:        newEdge(InnerJoinType, "101", "001", "110"),
			transform: rightAsscom,
		},
		{
			name:      "left join r-asscom(b,a)",
			eA:        newEdge(InnerJoinType, "110", "010", "101"),
			eB:        newEdge(LeftJoinType, "101", "001", "100"),
			transform: rightAsscom,
			rev:       true,
		},
		{
			name:      "left join l-asscom(a,b)",
			eA:        newEdge(FullOuterJoinType, "110", "100", "010"),
			eB:        newEdge(InnerJoinType, "101", "110", "001"),
			transform: leftAsscom,
		},
	}

	for _, tt := range invalidTests {
		t.Run(fmt.Sprintf("Invalid %s", tt.name), func(t *testing.T) {
			var res bool
			if tt.rev {
				res = tt.transform(tt.eB, tt.eA)
			} else {
				res = tt.transform(tt.eA, tt.eB)
			}
			require.False(t, res)
		})
	}
}

func newVertexSet(s string) vertexSet {
	v := vertexSet(0)
	for i, c := range s {
		if string(c) == "1" {
			v = v.add(uint64(i))
		}
	}
	return v
}

func newEdge(op JoinType, ses, leftV, rightV string) *edge {
	return &edge{
		op: &operator{
			joinType:      op,
			rightVertices: newVertexSet(rightV),
			leftVertices:  newVertexSet(leftV),
		},
		ses: newVertexSet(ses),
	}
}

func newEdge2(op JoinType, ses, tes, leftV, rightV string, rules []conflictRule, filter sql.Expression, nullRej string) edge {
	return edge{
		op: &operator{
			joinType:      op,
			rightVertices: newVertexSet(rightV),
			leftVertices:  newVertexSet(leftV),
		},
		ses:              newVertexSet(ses),
		tes:              newVertexSet(tes),
		rules:            rules,
		filter:           filter,
		nullRejectedRels: newVertexSet(nullRej),
	}
}

func edgesEq(t *testing.T, edges1, edges2 []edge) bool {
	if len(edges1) != len(edges2) {
		return false
	}
	for i := range edges1 {
		e1 := edges1[i]
		e2 := edges2[i]
		require.Equal(t, e1.op.joinType, e2.op.joinType)
		require.Equal(t, e1.op.leftVertices.String(), e2.op.leftVertices.String())
		require.Equal(t, e1.op.rightVertices.String(), e2.op.rightVertices.String())
		require.Equal(t, e1.filter, e2.filter)
		require.Equal(t, e1.nullRejectedRels, e2.nullRejectedRels)
		require.Equal(t, e1.tes, e2.tes)
		require.Equal(t, e1.ses, e2.ses)
		require.Equal(t, e1.rules, e2.rules)
	}
	return true
}
