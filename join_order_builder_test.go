package jointoy

import (
	"bytes"
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
	tests := []struct {
		in    sql.Node
		name  string
		plans string
	}{
		{
			name: "inner joins",
			in: plan.NewInnerJoin(
				plan.NewInnerJoin(
					plan.NewInnerJoin(
						tableNode("a"),
						tableNode("b"),
						newEq("a.i = b.i"),
					),
					tableNode("c"),
					newEq("b.i = c.i"),
				),
				tableNode("d"),
				newEq("c.i = d.i"),
			),
			plans: `----
InnerJoin(a.i  =  b.i)
 ├─ Table(a)
 └─ Table(b)
----
InnerJoin(a.i  =  b.i)
 ├─ Table(b)
 └─ Table(a)
----
InnerJoin(b.i  =  c.i)
 ├─ Table(b)
 └─ Table(c)
----
InnerJoin(b.i  =  c.i)
 ├─ Table(c)
 └─ Table(b)
----
InnerJoin(a.i  =  b.i)
 ├─ Table(a)
 └─ InnerJoin(b.i  =  c.i)
     ├─ Table(c)
     └─ Table(b)
----
InnerJoin(a.i  =  b.i)
 ├─ InnerJoin(b.i  =  c.i)
 │   ├─ Table(c)
 │   └─ Table(b)
 └─ Table(a)
----
InnerJoin(b.i  =  c.i)
 ├─ InnerJoin(a.i  =  b.i)
 │   ├─ Table(b)
 │   └─ Table(a)
 └─ Table(c)
----
InnerJoin(b.i  =  c.i)
 ├─ Table(c)
 └─ InnerJoin(a.i  =  b.i)
     ├─ Table(b)
     └─ Table(a)
----
InnerJoin(c.i  =  d.i)
 ├─ Table(c)
 └─ Table(d)
----
InnerJoin(c.i  =  d.i)
 ├─ Table(d)
 └─ Table(c)
----
InnerJoin(b.i  =  c.i)
 ├─ Table(b)
 └─ InnerJoin(c.i  =  d.i)
     ├─ Table(d)
     └─ Table(c)
----
InnerJoin(b.i  =  c.i)
 ├─ InnerJoin(c.i  =  d.i)
 │   ├─ Table(d)
 │   └─ Table(c)
 └─ Table(b)
----
InnerJoin(c.i  =  d.i)
 ├─ InnerJoin(b.i  =  c.i)
 │   ├─ Table(c)
 │   └─ Table(b)
 └─ Table(d)
----
InnerJoin(c.i  =  d.i)
 ├─ Table(d)
 └─ InnerJoin(b.i  =  c.i)
     ├─ Table(c)
     └─ Table(b)
----
InnerJoin(a.i  =  b.i)
 ├─ Table(a)
 └─ InnerJoin(c.i  =  d.i)
     ├─ Table(d)
     └─ InnerJoin(b.i  =  c.i)
         ├─ Table(c)
         └─ Table(b)
----
InnerJoin(a.i  =  b.i)
 ├─ InnerJoin(c.i  =  d.i)
 │   ├─ Table(d)
 │   └─ InnerJoin(b.i  =  c.i)
 │       ├─ Table(c)
 │       └─ Table(b)
 └─ Table(a)
----
InnerJoin(b.i  =  c.i)
 ├─ InnerJoin(a.i  =  b.i)
 │   ├─ Table(b)
 │   └─ Table(a)
 └─ InnerJoin(c.i  =  d.i)
     ├─ Table(d)
     └─ Table(c)
----
InnerJoin(b.i  =  c.i)
 ├─ InnerJoin(c.i  =  d.i)
 │   ├─ Table(d)
 │   └─ Table(c)
 └─ InnerJoin(a.i  =  b.i)
     ├─ Table(b)
     └─ Table(a)
----
InnerJoin(c.i  =  d.i)
 ├─ InnerJoin(b.i  =  c.i)
 │   ├─ Table(c)
 │   └─ InnerJoin(a.i  =  b.i)
 │       ├─ Table(b)
 │       └─ Table(a)
 └─ Table(d)
----
InnerJoin(c.i  =  d.i)
 ├─ Table(d)
 └─ InnerJoin(b.i  =  c.i)
     ├─ Table(c)
     └─ InnerJoin(a.i  =  b.i)
         ├─ Table(b)
         └─ Table(a)
`,
		},
		{
			name: "non-inner joins",
			in: plan.NewInnerJoin(
				plan.NewInnerJoin(
					plan.NewLeftJoin(
						tableNode("a"),
						tableNode("b"),
						newEq("a.i = b.i"),
					),
					plan.NewLeftJoin(
						NewFullJoin(
							tableNode("c"),
							tableNode("d"),
							newEq("c.i = d.i"),
						),
						tableNode("e"),
						newEq("c.i = e.i"),
					),
					newEq("a.i = e.i"),
				),
				plan.NewInnerJoin(
					tableNode("f"),
					tableNode("g"),
					newEq("f.i = g.i"),
				),
				newEq("e.i = g.i"),
			),
			plans: `----
LeftJoin(a.i  =  b.i)
 ├─ Table(a)
 └─ Table(b)
----
FullOuter(c.i  =  d.i)
 ├─ Table(c)
 └─ Table(d)
----
LeftJoin(c.i  =  e.i)
 ├─ FullOuter(c.i  =  d.i)
 │   ├─ Table(c)
 │   └─ Table(d)
 └─ Table(e)
----
InnerJoin(a.i  =  e.i)
 ├─ Table(a)
 └─ LeftJoin(c.i  =  e.i)
     ├─ FullOuter(c.i  =  d.i)
     │   ├─ Table(c)
     │   └─ Table(d)
     └─ Table(e)
----
InnerJoin(a.i  =  e.i)
 ├─ LeftJoin(c.i  =  e.i)
 │   ├─ FullOuter(c.i  =  d.i)
 │   │   ├─ Table(c)
 │   │   └─ Table(d)
 │   └─ Table(e)
 └─ Table(a)
----
LeftJoin(a.i  =  b.i)
 ├─ InnerJoin(a.i  =  e.i)
 │   ├─ LeftJoin(c.i  =  e.i)
 │   │   ├─ FullOuter(c.i  =  d.i)
 │   │   │   ├─ Table(c)
 │   │   │   └─ Table(d)
 │   │   └─ Table(e)
 │   └─ Table(a)
 └─ Table(b)
----
InnerJoin(a.i  =  e.i)
 ├─ LeftJoin(a.i  =  b.i)
 │   ├─ Table(a)
 │   └─ Table(b)
 └─ LeftJoin(c.i  =  e.i)
     ├─ FullOuter(c.i  =  d.i)
     │   ├─ Table(c)
     │   └─ Table(d)
     └─ Table(e)
----
InnerJoin(a.i  =  e.i)
 ├─ LeftJoin(c.i  =  e.i)
 │   ├─ FullOuter(c.i  =  d.i)
 │   │   ├─ Table(c)
 │   │   └─ Table(d)
 │   └─ Table(e)
 └─ LeftJoin(a.i  =  b.i)
     ├─ Table(a)
     └─ Table(b)
----
InnerJoin(f.i  =  g.i)
 ├─ Table(f)
 └─ Table(g)
----
InnerJoin(f.i  =  g.i)
 ├─ Table(g)
 └─ Table(f)
----
InnerJoin(e.i  =  g.i)
 ├─ LeftJoin(c.i  =  e.i)
 │   ├─ FullOuter(c.i  =  d.i)
 │   │   ├─ Table(c)
 │   │   └─ Table(d)
 │   └─ Table(e)
 └─ InnerJoin(f.i  =  g.i)
     ├─ Table(g)
     └─ Table(f)
----
InnerJoin(e.i  =  g.i)
 ├─ InnerJoin(f.i  =  g.i)
 │   ├─ Table(g)
 │   └─ Table(f)
 └─ LeftJoin(c.i  =  e.i)
     ├─ FullOuter(c.i  =  d.i)
     │   ├─ Table(c)
     │   └─ Table(d)
     └─ Table(e)
----
InnerJoin(a.i  =  e.i)
 ├─ Table(a)
 └─ InnerJoin(e.i  =  g.i)
     ├─ InnerJoin(f.i  =  g.i)
     │   ├─ Table(g)
     │   └─ Table(f)
     └─ LeftJoin(c.i  =  e.i)
         ├─ FullOuter(c.i  =  d.i)
         │   ├─ Table(c)
         │   └─ Table(d)
         └─ Table(e)
----
InnerJoin(a.i  =  e.i)
 ├─ InnerJoin(e.i  =  g.i)
 │   ├─ InnerJoin(f.i  =  g.i)
 │   │   ├─ Table(g)
 │   │   └─ Table(f)
 │   └─ LeftJoin(c.i  =  e.i)
 │       ├─ FullOuter(c.i  =  d.i)
 │       │   ├─ Table(c)
 │       │   └─ Table(d)
 │       └─ Table(e)
 └─ Table(a)
----
InnerJoin(e.i  =  g.i)
 ├─ InnerJoin(a.i  =  e.i)
 │   ├─ LeftJoin(c.i  =  e.i)
 │   │   ├─ FullOuter(c.i  =  d.i)
 │   │   │   ├─ Table(c)
 │   │   │   └─ Table(d)
 │   │   └─ Table(e)
 │   └─ Table(a)
 └─ InnerJoin(f.i  =  g.i)
     ├─ Table(g)
     └─ Table(f)
----
InnerJoin(e.i  =  g.i)
 ├─ InnerJoin(f.i  =  g.i)
 │   ├─ Table(g)
 │   └─ Table(f)
 └─ InnerJoin(a.i  =  e.i)
     ├─ LeftJoin(c.i  =  e.i)
     │   ├─ FullOuter(c.i  =  d.i)
     │   │   ├─ Table(c)
     │   │   └─ Table(d)
     │   └─ Table(e)
     └─ Table(a)
----
LeftJoin(a.i  =  b.i)
 ├─ InnerJoin(e.i  =  g.i)
 │   ├─ InnerJoin(f.i  =  g.i)
 │   │   ├─ Table(g)
 │   │   └─ Table(f)
 │   └─ InnerJoin(a.i  =  e.i)
 │       ├─ LeftJoin(c.i  =  e.i)
 │       │   ├─ FullOuter(c.i  =  d.i)
 │       │   │   ├─ Table(c)
 │       │   │   └─ Table(d)
 │       │   └─ Table(e)
 │       └─ Table(a)
 └─ Table(b)
----
InnerJoin(a.i  =  e.i)
 ├─ LeftJoin(a.i  =  b.i)
 │   ├─ Table(a)
 │   └─ Table(b)
 └─ InnerJoin(e.i  =  g.i)
     ├─ InnerJoin(f.i  =  g.i)
     │   ├─ Table(g)
     │   └─ Table(f)
     └─ LeftJoin(c.i  =  e.i)
         ├─ FullOuter(c.i  =  d.i)
         │   ├─ Table(c)
         │   └─ Table(d)
         └─ Table(e)
----
InnerJoin(a.i  =  e.i)
 ├─ InnerJoin(e.i  =  g.i)
 │   ├─ InnerJoin(f.i  =  g.i)
 │   │   ├─ Table(g)
 │   │   └─ Table(f)
 │   └─ LeftJoin(c.i  =  e.i)
 │       ├─ FullOuter(c.i  =  d.i)
 │       │   ├─ Table(c)
 │       │   └─ Table(d)
 │       └─ Table(e)
 └─ LeftJoin(a.i  =  b.i)
     ├─ Table(a)
     └─ Table(b)
----
InnerJoin(e.i  =  g.i)
 ├─ InnerJoin(a.i  =  e.i)
 │   ├─ LeftJoin(c.i  =  e.i)
 │   │   ├─ FullOuter(c.i  =  d.i)
 │   │   │   ├─ Table(c)
 │   │   │   └─ Table(d)
 │   │   └─ Table(e)
 │   └─ LeftJoin(a.i  =  b.i)
 │       ├─ Table(a)
 │       └─ Table(b)
 └─ InnerJoin(f.i  =  g.i)
     ├─ Table(g)
     └─ Table(f)
----
InnerJoin(e.i  =  g.i)
 ├─ InnerJoin(f.i  =  g.i)
 │   ├─ Table(g)
 │   └─ Table(f)
 └─ InnerJoin(a.i  =  e.i)
     ├─ LeftJoin(c.i  =  e.i)
     │   ├─ FullOuter(c.i  =  d.i)
     │   │   ├─ Table(c)
     │   │   └─ Table(d)
     │   └─ Table(e)
     └─ LeftJoin(a.i  =  b.i)
         ├─ Table(a)
         └─ Table(b)
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &testMemo{b: &bytes.Buffer{}}
			j := newJoinOrderBuilder(m)
			j.reorderJoin(tt.in)
			fmt.Println(m.plans())
			require.Equal(t, tt.plans, m.plans())
		})
	}
}

type testMemo struct {
	b *bytes.Buffer
	i int
}

func (m *testMemo) plans() string {
	return m.b.String()
}

func (m *testMemo) memoize() {
	//TODO implement me
	panic("implement me")
}

func (m *testMemo) memoizeJoin(
	op JoinType,
	left sql.Node,
	right sql.Node,
	joinFilter []sql.Expression,
	selFilter []sql.Expression,
) sql.Node {
	n := constructJoin(op, left, right, joinFilter, selFilter)
	m.b.WriteString("----\n")
	m.b.WriteString(n.String())
	m.i++
	return n
}

func TestJoinOrderBuilder_populateSubgraph2(t *testing.T) {
	tests := []struct {
		name     string
		join     sql.Node
		expEdges []edge
	}{
		{
			name: "cross join",
			join: plan.NewCrossJoin(
				tableNode("a"),
				plan.NewInnerJoin(
					tableNode("b"),
					plan.NewLeftJoin(
						tableNode("c"),
						tableNode("d"),
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
				tableNode("a"),
				plan.NewInnerJoin(
					tableNode("b"),
					plan.NewLeftJoin(
						tableNode("c"),
						tableNode("d"),
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
					tableNode("a"),
					tableNode("b"),
					newEq("a.x=b.x"),
				),
				plan.NewLeftJoin(
					tableNode("c"),
					tableNode("d"),
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
					tableNode("a"),
					tableNode("b"),
					expression.NewLiteral(true, sql.Boolean),
				),
				plan.NewInnerJoin(
					tableNode("c"),
					tableNode("d"),
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
					tableNode("b"),
					tableNode("c"),
					newEq("b.x=c.x"),
				),
				tableNode("a"),
				newEq("a.y=b.y"),
			),
			expEdges: []edge{
				newEdge2(LeftJoinType, "110", "110", "100", "010", nil, newEq("b.x=c.x"), ""), // B x C
				newEdge2(SemiJoinType, "101", "101", "110", "001", nil, newEq("a.y=b.y"), ""), // A x (BC)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newJoinOrderBuilder(&memo{})
			b.populateSubgraph(tt.join)
			edgesEq(t, tt.expEdges, b.edges)
		})
	}
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

var childSchema = sql.NewPrimaryKeySchema(sql.Schema{
	{Name: "i", Type: sql.Int64, Nullable: true},
	{Name: "s", Type: sql.Text, Nullable: true},
})

func tableNode(name string) sql.Node {
	t := memory.NewTable(name, childSchema, nil)
	return plan.NewResolvedTable(t, nil, nil)
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
