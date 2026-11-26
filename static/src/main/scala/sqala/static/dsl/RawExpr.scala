package sqala.static.dsl

import sqala.ir.{IrExpr, IrUnsafeToken}
import sqala.metadata.AsSqlExpr

import scala.collection.mutable

/**
 * A raw SQL expression fragment with interpolated values, expressions,
 * or subqueries. Values are automatically escaped.
 */
final class RawExpr[L <: Int](private val tokens: List[IrUnsafeToken]):
    /**
     * Declares the expression's result type.
     *
     * {{{
     * rawExpr"MATCH(${p.title}) AGAINST(${word})".as[Boolean]
     * }}}
     */
    def as[T: AsSqlExpr as a](using QueryContext[L]): Expr[Wrap[T, Option], Composite[EmptyTuple]] =
        Expr(IrExpr.UnsafeCustom(tokens))

object RawExpr:
    def apply[L <: Int](words: List[String], exprs: List[IrExpr]): RawExpr[L] =
        val tokens = new mutable.ListBuffer[IrUnsafeToken]
        val wordIterator = words.iterator
        val exprIterator = exprs.iterator
        val firstWord = wordIterator.next().trim
        firstWord.split(" ").filter(_.nonEmpty).foreach(w => tokens.append(IrUnsafeToken.Keyword(w)))
        while wordIterator.hasNext do
            if exprIterator.hasNext then
                val nextExpr = exprIterator.next()
                tokens.append(IrUnsafeToken.Expr(nextExpr))
            val nextWord = wordIterator.next().trim
            nextWord.split(" ").filter(_.nonEmpty).foreach(w => tokens.append(IrUnsafeToken.Keyword(w)))
        new RawExpr(tokens.toList)