package sqala.static.ext.dsl

import sqala.static.dsl.*

import scala.compiletime.error
import scala.util.NotGiven

/**
 * Validates that a kind is a single column of the source table, at the
 * current query context level `CL`.
 */
trait CanInRecognizePartition[K <: ExprKind, CL <: Int]

object CanInRecognizePartition:
    given column[CL <: Int]: CanInRecognizePartition[Column[CL], CL]()

    inline given fallback[K <: ExprKind, CL <: Int](using
        NotGiven[CanInRecognizePartition[K, CL]]
    ): CanInRecognizePartition[K, CL] =
        error("Only columns of the source table can be used in matchRecognize partitionBy.")

/**
 * Validates that a kind is a single column of the source table, at the
 * current query context level `CL`.
 */
trait CanInRecognizeSort[K <: ExprKind, CL <: Int]

object CanInRecognizeSort:
    given column[CL <: Int]: CanInRecognizeSort[Column[CL], CL]()

    inline given fallback[K <: ExprKind, CL <: Int](using
        NotGiven[CanInRecognizeSort[K, CL]]
    ): CanInRecognizeSort[K, CL] =
        error("Only columns of the source table can be used in matchRecognize sortBy.")

/**
 * Validates that a kind tuple can appear in a `define` clause of
 * `matchRecognize`. `CL` is the current query context level.
 */
trait CanInRecognizeDefine[KS <: Tuple, CL <: Int]

object CanInRecognizeDefine:
    given validate[KS <: Tuple, CL <: Int](using
        hc: HasKind[KS, Column[?]],
        nc: hc.R =:= false,
        hw: HasKind[KS, Window[?]],
        nw: hw.R =:= false,
        hu: HasKind[KS, UngroupedColumn[?]],
        nu: hu.R =:= false,
        oks: ExcludeCurrentLevelColumn[KS, CL],
        e: oks.R =:= EmptyTuple
    ): CanInRecognizeDefine[KS, CL]()

    inline given fallback[KS <: Tuple, CL <: Int](using
        NotGiven[CanInRecognizeDefine[KS, CL]]
    ): CanInRecognizeDefine[KS, CL] =
        error("Only pattern variables and aggregate functions over them can be used in matchRecognize define.")

/**
 * Validates that a kind tuple can appear in a `measures` clause of
 * `matchRecognize`.
 */
trait CanInRecognizeMeasures[KS <: Tuple, CL <: Int]

object CanInRecognizeMeasures:
    given validate[KS <: Tuple, CL <: Int](using
        hc: HasKind[KS, Column[?]],
        nc: hc.R =:= false,
        hw: HasKind[KS, Window[?]],
        nw: hw.R =:= false,
        hu: HasKind[KS, UngroupedColumn[?]],
        nu: hu.R =:= false,
        oks: ExcludeCurrentLevelColumn[KS, CL],
        e: oks.R =:= EmptyTuple
    ): CanInRecognizeMeasures[KS, CL]()

    inline given fallback[KS <: Tuple, CL <: Int](using
        NotGiven[CanInRecognizeMeasures[KS, CL]]
    ): CanInRecognizeMeasures[KS, CL] =
        error("Measures expressions accept pattern variables and aggregate functions over them.")