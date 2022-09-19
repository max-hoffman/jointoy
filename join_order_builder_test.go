package jointoy

import (
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
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
