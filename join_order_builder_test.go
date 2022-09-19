package jointoy

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/stretchr/testify/require"
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

func TestJoinOrderBuilder_populateSubgraph(t *testing.T) {
	//childSchema := sql.NewPrimaryKeySchema(sql.Schema{
	//	{Name: "i", Type: sql.Int64, Nullable: true},
	//	{Name: "s", Type: sql.Text, Nullable: true},
	//})
	//child := memory.NewTable("test", childSchema, nil)
	//
	//// make sure TES, SES, and rules are correct
	// also nullRejectedRels
	tests := []struct {
		q        string
		expTES   string
		expRules []conflictRule
	}{
		{
			q:      "select a from A left join B on a = b",
			expTES: "11",
		},
		{
			q:      "select a from A left join B on a = b",
			expTES: "11",
		},
		{
			q: `select x from xy
				join
				(ab left join uv on a = v)
				on x = b
				inner join pq on u = q`,
			expTES:   "",
			expRules: nil,
		},
	}
	ctx := sql.NewEmptyContext()
	for _, tt := range tests {
		t.Run(tt.q, func(t *testing.T) {
			n, err := parse.Parse(ctx, tt.q)
			require.NoError(t, err)
			var join sql.Node
			transform.Inspect(n, func(n sql.Node) bool {
				if join != nil {
					return false
				}
				switch n := n.(type) {
				case plan.JoinNode, *plan.CrossJoin:
					join = n
					return false
				default:
					return true
				}
			})
			b := newJoinOrderBuilder()
			b.populateSubgraph(join)
			require.Equal(t, tt.expTES, b.edges[len(b.edges)-1].tes.String())
		})
	}
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
