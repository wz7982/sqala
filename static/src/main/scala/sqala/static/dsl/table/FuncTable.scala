package sqala.static.dsl.table

import sqala.ast.table.SqlTable
import sqala.metadata.TableMetaData
import sqala.static.dsl.*

/**
 * A table function reference, representing a table-valued function
 * such as `unnest`.
 */
final case class FromFunc[T, K[_ <: Int] <: ExprKind, OKS <: Tuple, CL <: Int](
    private[sqala] val __aliasName__ : String,
    private[sqala] val __metaData__ : TableMetaData,
    private[sqala] val __sqlTable__ : SqlTable.Func
) extends AnyTable