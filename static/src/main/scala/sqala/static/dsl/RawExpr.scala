package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.ast.token.SqlUnsafeCustomToken
import sqala.metadata.AsSqlExpr

import scala.collection.mutable

/**
 * A raw SQL expression fragment with interpolated values, expressions,
 * or subqueries. Values are automatically escaped.
 */
final case class RawExpr[L <: Int](private val tokens: List[SqlUnsafeCustomToken]):
    /**
     * Declares the expression's result type.
     *
     * {{{
     * rawExpr"MATCH(${p.title}) AGAINST(${word})".as[Boolean]
     * }}}
     */
    def as[T: AsSqlExpr as a](using QueryContext[L]): Expr[Wrap[T, Option], Value] =
        Expr(SqlExpr.UnsafeCustom(tokens))

object RawExpr:
    def apply[L <: Int](words: List[String], instances: List[AsExpr[?, ?]], args: List[Any]): RawExpr[L] =
        val exprs = instances.zip(args).map((i, e) => i.asInstanceOf[AsExpr[Any, ?]].asExpr(e).asSqlExpr)
        val tokens = new mutable.ListBuffer[SqlUnsafeCustomToken]
        val wordIterator = words.iterator
        val exprIterator = exprs.iterator
        val firstWord = wordIterator.next().trim
        firstWord.split(" ").filter(_.nonEmpty).foreach(w => tokens.append(SqlUnsafeCustomToken.Keyword(w)))
        while wordIterator.hasNext do
            if exprIterator.hasNext then
                val nextExpr = exprIterator.next()
                tokens.append(SqlUnsafeCustomToken.Expr(nextExpr))
            val nextWord = wordIterator.next().trim
            nextWord.split(" ").filter(_.nonEmpty).foreach(w => tokens.append(SqlUnsafeCustomToken.Keyword(w)))
        new RawExpr(tokens.toList)