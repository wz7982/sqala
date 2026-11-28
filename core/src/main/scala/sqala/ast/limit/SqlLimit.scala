package sqala.ast.limit

import sqala.ast.expr.SqlExpr

/**
 * A limit or offset clause.
 *
 * Renders as `[OFFSET n [ROW|ROWS]] [FETCH FIRST|NEXT expr [PERCENT] ROW|ROWS ONLY|WITH TIES]`.
 */
case class SqlLimit(offset: Option[SqlExpr], fetch: Option[SqlFetch])

/**
 * An enumeration of row count units for `FETCH`.
 */
enum SqlFetchUnit:
    /**
     * Row count.
     *
     * Renders as `ROW|ROWS`.
     */
    case RowCount

    /**
     * Percentage.
     *
     * Renders as `PERCENT`.
     */
    case Percentage

/**
 * A mode specification for the `FETCH` clause.
 */
enum SqlFetchMode:
    /**
     * Fetch exactly the requested number of rows.
     *
     * Renders as `ONLY`.
     */
    case Only

    /**
     * Fetch additional rows that tie with the last row.
     *
     * Renders as `WITH TIES`.
     */
    case WithTies

/**
 * A `FETCH` clause.
 *
 * Renders as `FETCH FIRST|NEXT expr [PERCENT] ROW|ROWS ONLY|WITH TIES`.
 */
case class SqlFetch(limit: SqlExpr, unit: SqlFetchUnit, mode: SqlFetchMode)