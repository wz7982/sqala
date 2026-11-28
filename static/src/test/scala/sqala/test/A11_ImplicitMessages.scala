package sqala.test

import sqala.static.dsl.*
import sqala.static.dsl.table.*

/**
 * implicitNotFound messages of the static DSL conversion type classes.
 * Every summon targets a type with no given instance, so the compiler
 * must surface the trait's own annotation message. Exhaustive type
 * classes are checked positively instead: their summons must compile.
 */
class A11_ImplicitMessages extends munit.FunSuite:
    private def check(errors: String, msg: String): Unit =
        assert(errors.contains(msg), s"expected message: $msg")

    test("expression and comparison conversions report their messages"):
        check(compileErrors("summon[AsExpr[Thread, 0]]"), "This value cannot be converted to a query expression.")
        check(compileErrors("summon[AsComparison[Thread, 0]]"), "This value cannot be converted to a comparison operand.")

    test("grouping and partition conversions report their messages"):
        check(compileErrors("summon[AsGroupItem[Thread, 0]]"), "This value cannot be converted to a grouping expression.")
        check(compileErrors("summon[AsGroup[Thread, 0]]"), "This value cannot be converted to a grouping expression.")
        check(compileErrors("summon[AsPartitionItem[Thread, 0]]"), "This value cannot be converted to a partition expression.")
        check(compileErrors("summon[AsPartition[Thread, 0]]"), "This value cannot be converted to a partition expression.")

    test("sort conversions report their messages"):
        check(compileErrors("summon[AsSortItem[Thread, ManyRows, 0]]"), "This value cannot be converted to a sort expression.")
        check(compileErrors("summon[AsSort[Thread, ManyRows, 0]]"), "This value cannot be converted to a sort expression.")
        check(compileErrors("summon[AsGroupedSortItem[Thread, 0]]"), "This value cannot be converted to a sort expression.")
        check(compileErrors("summon[AsGroupedSort[Thread, 0]]"), "This value cannot be converted to a sort expression.")
        check(compileErrors("summon[AsDistinctSortItem[Thread, 0]]"), "This value cannot be converted to a sort expression.")
        check(compileErrors("summon[AsDistinctSort[Thread, 0]]"), "This value cannot be converted to a sort expression.")
        check(compileErrors("summon[AsOverSortItem[Thread, 0]]"), "This value cannot be converted to a sort expression.")
        check(compileErrors("summon[AsOverSort[Thread, 0]]"), "This value cannot be converted to a sort expression.")

    test("projection conversions report their messages"):
        check(compileErrors("summon[sqala.static.dsl.statement.query.AsMapItem[Thread, 0]]"), "This value cannot be converted to a projection expression.")
        check(compileErrors("summon[sqala.static.dsl.statement.query.AsMap[Thread, 0]]"), "This value cannot be converted to a projection expression.")

    test("table conversions report their messages"):
        check(compileErrors("summon[AsTable[Thread, 0]]"), "This value cannot be converted to a table.")
        check(compileErrors("summon[AsTableParam[Thread, 0]]"), "This value cannot be converted to a table parameter.")
        check(compileErrors("summon[AsLateralTable[Thread, 0]]"), "This value cannot be converted to a lateral table.")

    test("insert and multidimensional grouping conversions report their messages"):
        check(compileErrors("summon[sqala.static.dsl.statement.dml.AsInsertColumns[Thread]]"), "This value cannot be converted to an insert column list.")
        check(compileErrors("summon[sqala.static.dsl.statement.query.AsGroupingItem[Thread]]"), "This value cannot be converted to a grouping item.")
        check(compileErrors("summon[sqala.static.dsl.statement.query.AsMultidimensionalGrouping[Thread]]"), "This value cannot be converted to a multidimensional grouping.")

    test("result and nullable conversions report their messages"):
        check(compileErrors("summon[Result[Thread]]"), "This projection cannot be converted to a query result type.")
        check(compileErrors("summon[ToOption[Thread]]"), "This value cannot be converted to a nullable result.")

    test("TransformKind reports its conversion message"):
        check(compileErrors("summon[TransformKind[Thread, Distinct, 0]]"), "This value cannot be converted to an expression.")

    test("outer join operands report the join message"):
        check(compileErrors("summon[LeftJoin[Thread, Thread]]"), "This value cannot be used as a join operand.")
        check(compileErrors("summon[RightJoin[Thread, Thread]]"), "This value cannot be used as a join operand.")
        check(compileErrors("summon[FullJoin[Thread, Thread]]"), "This value cannot be used as a join operand.")

    test("exhaustive type classes compile for any type"):
        summon[Join[Int, Int]]
        summon[ToTuple[Int]]
        summon[TakeSize[Int]]
        summon[IsKind[Value, Value]]
        summon[KindToTuple[Value]]
        summon[CombineKindTuple[EmptyTuple, EmptyTuple]]
        summon[TransformKind[Expr[Int, Value], Distinct, 0]]