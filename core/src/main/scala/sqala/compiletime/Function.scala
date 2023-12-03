package sqala.compiletime

def count: Agg[Long, EmptyTuple] = Agg("COUNT", Nil, false, Nil)

def count[TableNames <: Tuple](expr: Expr[?, TableNames]): Agg[Long, TableNames] = 
    Agg("COUNT", expr :: Nil, false, Nil)

def countDistinct[TableNames <: Tuple](expr: Expr[?, TableNames]): Agg[Long, TableNames] = 
    Agg("COUNT", expr :: Nil, true, Nil)

def sum[T: ExprNumber, TableNames <: Tuple](expr: Expr[T, TableNames]): Agg[Option[BigDecimal], TableNames] = Agg("SUM", expr :: Nil, false, Nil)

def avg[T: ExprNumber, TableNames <: Tuple](expr: Expr[T, TableNames]): Agg[Option[BigDecimal], TableNames] =
    Agg("SUM", expr :: Nil, false, Nil)

def max[T, TableNames <: Tuple](expr: Expr[T, TableNames]): Agg[WrapToOption[T], TableNames] =
    Agg("MAX", expr :: Nil, false, Nil)

def min[T, TableNames <: Tuple](expr: Expr[T, TableNames]): Agg[WrapToOption[T], TableNames] =
    Agg("MIN", expr :: Nil, false, Nil)

def rank: Agg[Option[Long], EmptyTuple] = Agg("RANK", Nil, false, Nil)

def denseRank: Agg[Option[Long], EmptyTuple] = Agg("DENSE_RANK", Nil, false, Nil)

def rowNumber: Agg[Option[Long], EmptyTuple] = Agg("ROW_NUMBER", Nil, false, Nil)

def lag[T: AsSqlExpr, TableNames <: Tuple](expr: Expr[T, TableNames], offset: Int = 1, default: WrapToOption[T] = None): Agg[WrapToOption[T], TableNames] =
    val defaultExpr = default match
        case Some(n) => Literal(n)
        case _ => Null
    Agg("LAG", expr :: Literal(offset) :: defaultExpr :: Nil, false, Nil)

def lead[T: AsSqlExpr, TableNames <: Tuple](expr: Expr[T, TableNames], offset: Int = 1, default: WrapToOption[T] = None): Agg[WrapToOption[T], TableNames] =
    val defaultExpr = default match
        case Some(n) => Literal(n)
        case _ => Null
    Agg("LEAD", expr :: Literal(offset) :: defaultExpr :: Nil, false, Nil)