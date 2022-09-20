package jointoy

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

type Memo interface {
	memoize()
	memoizeJoin(JoinType, sql.Node, sql.Node, []sql.Expression, []sql.Expression) sql.Node
}

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
	return constructJoin(op, left, right, joinFilter, selFilter)
}

func constructJoin(op JoinType,
	left sql.Node,
	right sql.Node,
	joinFilter []sql.Expression,
	selFilter []sql.Expression,
) sql.Node {
	var filter sql.Expression
	if len(joinFilter) > 0 {
		filter = joinFilter[0]
		for _, e := range joinFilter[1:] {
			filter = expression.NewAnd(filter, e)
		}
	}
	var join sql.Node
	switch op {
	case InnerJoinType:
		join = plan.NewInnerJoin(left, right, filter)
	case LeftJoinType:
		join = plan.NewLeftJoin(left, right, filter)
	case CrossJoinType:
		join = plan.NewCrossJoin(left, right)
	case FullOuterJoinType:
		join = NewFullJoin(left, right, filter)
	case SemiJoinType:
		join = NewSemiJoin(left, right, filter)
	case AntiJoinType:
		join = NewAntiJoin(left, right, filter)
	default:
		panic("unknown join type")
	}
	if len(selFilter) > 0 {
		filter = selFilter[0]
		for _, e := range selFilter[1:] {
			filter = expression.NewAnd(filter, e)
		}
		return plan.NewFilter(filter, join)
	}
	return join
}
