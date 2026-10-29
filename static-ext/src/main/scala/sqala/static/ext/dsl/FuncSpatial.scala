package sqala.static.ext.dsl

import sqala.static.dsl.QueryContext
import sqala.static.dsl.AsExpr
import sqala.metadata.SqlString
import sqala.metadata.SqlNumber
import sqala.static.dsl.CombineKind
import sqala.metadata.Geometry
import sqala.static.dsl.Expr
import sqala.ir.IrExpr
import sqala.static.dsl.KindToTuple
import sqala.static.dsl.Composite
import sqala.metadata.SqlGeometry
import sqala.static.dsl.ValidateArgType

/**
 * Constructs a geometry from WKT text with an SRID. Maps to
 * `ST_GeomFromText(text, srid)`. Part of the ISO/IEC 13249
 * spatial standard.
 *
 * {{{
 * from(Place).map(p => stGeomFromText(p.wkt, 4326))
 * }}}
 */
def stGeomFromText[A, S, CL <: Int](x: A, srid: S)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    as: AsExpr[S, CL],
    s: ValidateArgType[SqlString, aa.R, "string"],
    n: ValidateArgType[SqlNumber, as.R, "numeric"],
    c: CombineKind[aa.K, as.K]
): Expr[Option[Geometry], c.R] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "ST_GeomFromText",
            aa.asExpr(x).expr :: as.asExpr(srid).expr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Converts a geometry to WKT text. Maps to `ST_AsText(expr)`.
 * Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).map(p => stAsText(p.location))
 * }}}
 */
def stAsText[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    g: ValidateArgType[SqlGeometry, a.R, "geometry"],
    kt: KindToTuple[a.K]
): Expr[Option[String], Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "ST_AsText",
            a.asExpr(x).expr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Converts a geometry to GeoJSON text. Maps to
 * `ST_AsGeoJson(expr)`. Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).map(p => stAsGeoJson(p.location))
 * }}}
 */
def stAsGeoJson[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    g: ValidateArgType[SqlGeometry, a.R, "geometry"],
    kt: KindToTuple[a.K]
): Expr[Option[String], Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "ST_AsGeoJson",
            a.asExpr(x).expr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Returns the geometry type name. Maps to
 * `ST_GeometryType(expr)`. Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).map(p => stGeometryType(p.location))
 * }}}
 */
def stGeometryType[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    g: ValidateArgType[SqlGeometry, a.R, "geometry"],
    kt: KindToTuple[a.K]
): Expr[Option[String], Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "ST_GeometryType",
            a.asExpr(x).expr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Returns the X coordinate of a point. Maps to `ST_X(expr)`.
 * Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).map(p => stX(p.location))
 * }}}
 */
def stX[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    g: ValidateArgType[SqlGeometry, a.R, "geometry"],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "ST_X",
            a.asExpr(x).expr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Returns the Y coordinate of a point. Maps to `ST_Y(expr)`.
 * Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).map(p => stY(p.location))
 * }}}
 */
def stY[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    g: ValidateArgType[SqlGeometry, a.R, "geometry"],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "ST_Y",
            a.asExpr(x).expr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the area of a geometry. Maps to `ST_Area(expr)`.
 * Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).map(p => stArea(p.location))
 * }}}
 */
def stArea[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    g: ValidateArgType[SqlGeometry, a.R, "geometry"],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "ST_Area",
            a.asExpr(x).expr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the length of a geometry. Maps to `ST_Length(expr)`.
 * Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).map(p => stLength(p.location))
 * }}}
 */
def stLength[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    g: ValidateArgType[SqlGeometry, a.R, "geometry"],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "ST_Length",
            a.asExpr(x).expr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Tests whether one geometry contains another. Maps to
 * `ST_Contains(x, y)`. Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).filter(p => stContains(p.location, p.location))
 * }}}
 */
def stContains[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    ga: ValidateArgType[SqlGeometry, aa.R, "geometry"],
    gb: ValidateArgType[SqlGeometry, ab.R, "geometry"],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[Boolean], c.R] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "ST_Contains",
            aa.asExpr(x).expr :: ab.asExpr(y).expr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Tests whether one geometry is within another. Maps to
 * `ST_Within(x, y)`. Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).filter(p => stWithin(p.location, p.location))
 * }}}
 */
def stWithin[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    ga: ValidateArgType[SqlGeometry, aa.R, "geometry"],
    gb: ValidateArgType[SqlGeometry, ab.R, "geometry"],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[Boolean], c.R] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "ST_Within",
            aa.asExpr(x).expr :: ab.asExpr(y).expr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Tests whether two geometries intersect. Maps to
 * `ST_Intersects(x, y)`. Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).filter(p => stIntersects(p.location, p.location))
 * }}}
 */
def stIntersects[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    ga: ValidateArgType[SqlGeometry, aa.R, "geometry"],
    gb: ValidateArgType[SqlGeometry, ab.R, "geometry"],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[Boolean], c.R] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "ST_Intersects",
            aa.asExpr(x).expr :: ab.asExpr(y).expr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Tests whether two geometries touch. Maps to
 * `ST_Touches(x, y)`. Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).filter(p => stTouches(p.location, p.location))
 * }}}
 */
def stTouches[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    ga: ValidateArgType[SqlGeometry, aa.R, "geometry"],
    gb: ValidateArgType[SqlGeometry, ab.R, "geometry"],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[Boolean], c.R] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "ST_Touches",
            aa.asExpr(x).expr :: ab.asExpr(y).expr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Tests whether two geometries overlap. Maps to
 * `ST_Overlaps(x, y)`. Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).filter(p => stOverlaps(p.location, p.location))
 * }}}
 */
def stOverlaps[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    ga: ValidateArgType[SqlGeometry, aa.R, "geometry"],
    gb: ValidateArgType[SqlGeometry, ab.R, "geometry"],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[Boolean], c.R] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "ST_Overlaps",
            aa.asExpr(x).expr :: ab.asExpr(y).expr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Tests whether two geometries cross. Maps to
 * `ST_Crosses(x, y)`. Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).filter(p => stCrosses(p.location, p.location))
 * }}}
 */
def stCrosses[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    ga: ValidateArgType[SqlGeometry, aa.R, "geometry"],
    gb: ValidateArgType[SqlGeometry, ab.R, "geometry"],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[Boolean], c.R] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "ST_Crosses",
            aa.asExpr(x).expr :: ab.asExpr(y).expr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Tests whether two geometries are disjoint. Maps to
 * `ST_Disjoint(x, y)`. Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).filter(p => stDisjoint(p.location, p.location))
 * }}}
 */
def stDisjoint[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    ga: ValidateArgType[SqlGeometry, aa.R, "geometry"],
    gb: ValidateArgType[SqlGeometry, ab.R, "geometry"],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[Boolean], c.R] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "ST_Disjoint",
            aa.asExpr(x).expr :: ab.asExpr(y).expr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the intersection of two geometries. Maps to
 * `ST_Intersection(x, y)`. Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).map(p => stIntersection(p.location, p.location))
 * }}}
 */
def stIntersection[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    ga: ValidateArgType[SqlGeometry, aa.R, "geometry"],
    gb: ValidateArgType[SqlGeometry, ab.R, "geometry"],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[Boolean], c.R] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "ST_Intersection",
            aa.asExpr(x).expr :: ab.asExpr(y).expr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the union of two geometries. Maps to
 * `ST_Union(x, y)`. Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).map(p => stUnion(p.location, p.location))
 * }}}
 */
def stUnion[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    ga: ValidateArgType[SqlGeometry, aa.R, "geometry"],
    gb: ValidateArgType[SqlGeometry, ab.R, "geometry"],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[Geometry], c.R] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "ST_Union",
            aa.asExpr(x).expr :: ab.asExpr(y).expr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the difference of two geometries. Maps to
 * `ST_Difference(x, y)`. Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).map(p => stDifference(p.location, p.location))
 * }}}
 */
def stDifference[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    ga: ValidateArgType[SqlGeometry, aa.R, "geometry"],
    gb: ValidateArgType[SqlGeometry, ab.R, "geometry"],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[Geometry], c.R] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "ST_Difference",
            aa.asExpr(x).expr :: ab.asExpr(y).expr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the symmetric difference of two geometries. Maps to
 * `ST_SymDifference(x, y)`. Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).map(p => stSymDifference(p.location, p.location))
 * }}}
 */
def stSymDifference[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    ga: ValidateArgType[SqlGeometry, aa.R, "geometry"],
    gb: ValidateArgType[SqlGeometry, ab.R, "geometry"],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[Geometry], c.R] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "ST_SymDifference",
            aa.asExpr(x).expr :: ab.asExpr(y).expr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the distance between two geometries. Maps to
 * `ST_Distance(x, y)`. Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).map(p => stDistance(p.location, p.location))
 * }}}
 */
def stDistance[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    ga: ValidateArgType[SqlGeometry, aa.R, "geometry"],
    gb: ValidateArgType[SqlGeometry, ab.R, "geometry"],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[BigDecimal], c.R] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "ST_Distance",
            aa.asExpr(x).expr :: ab.asExpr(y).expr :: Nil,
            Nil,
            Nil,
            None
        )
    )