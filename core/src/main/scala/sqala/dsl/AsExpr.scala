package sqala.dsl

trait AsExpr[T]:
    def asExprs(item: T): List[Expr[?]]

object AsExpr:
    given exprAsExpr[T]: AsExpr[Expr[T]] with
        override def asExprs(item: Expr[T]): List[Expr[?]] = item :: Nil
    
    given exprSubtypingAsExpr[E <: Expr[?]]: AsExpr[E] with
        override def asExprs(item: E): List[Expr[?]] = item :: Nil

    given tupleAsExpr[H, T <: Tuple](using h: AsExpr[H], t: AsExpr[T]): AsExpr[H *: T] with
        override def asExprs(item: H *: T): List[Expr[?]] = h.asExprs(item.head) ++ t.asExprs(item.tail)

    given emptyTupleAsExpr: AsExpr[EmptyTuple] with
        override def asExprs(item: EmptyTuple): List[Expr[?]] = Nil