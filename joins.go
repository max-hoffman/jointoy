package jointoy

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

type joinBase struct {
	left   sql.Node
	right  sql.Node
	filter sql.Expression
	op     JoinType
}

func (f *joinBase) Expressions() []sql.Expression {
	return []sql.Expression{f.filter}
}

func (f *joinBase) Left() sql.Node {
	return f.left
}

func (f *joinBase) Right() sql.Node {
	return f.right
}

func (f *joinBase) JoinCond() sql.Expression {
	return f.filter
}

func (f *joinBase) Comment() string {
	//TODO implement me
	panic("implement me")
}

func (f *joinBase) Resolved() bool {
	//TODO implement me
	panic("implement me")
}

func (f *joinBase) Schema() sql.Schema {
	//TODO implement me
	panic("implement me")
}

func (f *joinBase) Children() []sql.Node {
	return []sql.Node{f.left, f.right}
}

func (f *joinBase) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	//TODO implement me
	panic("implement me")
}
func (f *joinBase) JoinType() plan.JoinType {
	panic("iplement me")
}

func (f *joinBase) WithExpressions(expression ...sql.Expression) (sql.Node, error) {
	ret := *f
	ret.filter = expression[0]
	return &ret, nil
}

func (f *joinBase) WithScopeLen(i int) plan.JoinNode {
	//TODO implement me
	panic("implement me")
}

func (f *joinBase) WithMultipassMode() plan.JoinNode {
	//TODO implement me
	panic("implement me")
}

func (f *joinBase) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	//TODO implement me
	panic("implement me")
}

func (f *joinBase) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NewFullJoin(children[0], children[1], f.filter), nil
}

func (f *joinBase) String() string {
	//TODO implement me
	panic("implement me")
}

var _ sql.Node = (*FullJoin)(nil)
var _ plan.JoinNode = (*FullJoin)(nil)
var _ sql.Expressioner = (*FullJoin)(nil)

func NewFullJoin(left, right sql.Node, filter sql.Expression) *FullJoin {
	return &FullJoin{
		&joinBase{op: FullOuterJoinType, left: left, right: right, filter: filter},
	}
}

type FullJoin struct {
	*joinBase
}

func (f *FullJoin) JoinType() plan.JoinType {
	panic("iplement me")
}

func (f *FullJoin) WithExpressions(expression ...sql.Expression) (sql.Node, error) {
	ret := *f
	ret.filter = expression[0]
	return &ret, nil
}

func (f *FullJoin) WithScopeLen(i int) plan.JoinNode {
	//TODO implement me
	panic("implement me")
}

func (f *FullJoin) WithMultipassMode() plan.JoinNode {
	//TODO implement me
	panic("implement me")
}

func (f *FullJoin) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	//TODO implement me
	panic("implement me")
}

func (f *FullJoin) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NewFullJoin(children[0], children[1], f.filter), nil
}

func (f *FullJoin) String() string {
	//TODO implement me
	panic("implement me")
}

var _ sql.Node = (*SemiJoin)(nil)
var _ plan.JoinNode = (*SemiJoin)(nil)
var _ sql.Expressioner = (*SemiJoin)(nil)

func NewSemiJoin(left, right sql.Node, filter sql.Expression) *SemiJoin {
	return &SemiJoin{
		&joinBase{op: SemiJoinType, left: left, right: right, filter: filter},
	}
}

type SemiJoin struct {
	*joinBase
}

func (j *SemiJoin) JoinType() plan.JoinType {
	panic("iplement me")
}

func (j *SemiJoin) WithExpressions(expression ...sql.Expression) (sql.Node, error) {
	ret := *j
	ret.filter = expression[0]
	return &ret, nil
}

func (j *SemiJoin) WithScopeLen(i int) plan.JoinNode {
	//TODO implement me
	panic("implement me")
}

func (j *SemiJoin) WithMultipassMode() plan.JoinNode {
	//TODO implement me
	panic("implement me")
}

func (j *SemiJoin) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	//TODO implement me
	panic("implement me")
}

func (j *SemiJoin) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NewSemiJoin(children[0], children[1], j.filter), nil
}

func (j *SemiJoin) String() string {
	//TODO implement me
	panic("implement me")
}

var _ sql.Node = (*AntiJoin)(nil)
var _ plan.JoinNode = (*AntiJoin)(nil)
var _ sql.Expressioner = (*AntiJoin)(nil)

func NewAntiJoin(left, right sql.Node, filter sql.Expression) *AntiJoin {
	return &AntiJoin{
		&joinBase{op: AntiJoinType, left: left, right: right, filter: filter},
	}
}

type AntiJoin struct {
	*joinBase
}

func (j *AntiJoin) JoinType() plan.JoinType {
	panic("iplement me")
}

func (j *AntiJoin) WithExpressions(expression ...sql.Expression) (sql.Node, error) {
	ret := *j
	ret.filter = expression[0]
	return &ret, nil
}

func (j *AntiJoin) WithScopeLen(i int) plan.JoinNode {
	//TODO implement me
	panic("implement me")
}

func (j *AntiJoin) WithMultipassMode() plan.JoinNode {
	//TODO implement me
	panic("implement me")
}

func (j *AntiJoin) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	//TODO implement me
	panic("implement me")
}

func (j *AntiJoin) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NewAntiJoin(children[0], children[1], j.filter), nil
}

func (j *AntiJoin) String() string {
	//TODO implement me
	panic("implement me")
}

type GroupJoin struct{}

func (g GroupJoin) Resolved() bool {
	//TODO implement me
	panic("implement me")
}

func (g GroupJoin) String() string {
	//TODO implement me
	panic("implement me")
}

func (g GroupJoin) Schema() sql.Schema {
	//TODO implement me
	panic("implement me")
}

func (g GroupJoin) Children() []sql.Node {
	//TODO implement me
	panic("implement me")
}

func (g GroupJoin) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	//TODO implement me
	panic("implement me")
}

func (g GroupJoin) WithChildren(children ...sql.Node) (sql.Node, error) {
	//TODO implement me
	panic("implement me")
}

func (g GroupJoin) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	//TODO implement me
	panic("implement me")
}

var _ sql.Node = (*GroupJoin)(nil)
