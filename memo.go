package jointoy

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// table index options and dependencies
// creating and updating group plans
// if two plans equal, linked list in group
// convert memo to plan tree
type memo struct {
}

// todo track lowest cost plan for group
func (m *memo) memoize() {

}

func (m *memo) memoizeJoin(
	op JoinType,
	left sql.Node,
	right sql.Node,
	joinFilter []sql.Expression,
	selFilter []sql.Expression,
) sql.Node {
	switch op {
	case InnerJoinType:
		filter := joinFilter[0]
		for _, e := range joinFilter[1:] {
			filter = expression.NewAnd(filter, e)
		}
		return plan.NewInnerJoin(left, right, filter)
	case LeftJoinType:
	case CrossJoinType:
	default:
		panic("unknown join type")
	}
	return nil
}
