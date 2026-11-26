package sqala.test

import sqala.static.dsl.*
import sqala.test.StaticFixture.expect

/**
 * How types flow through query composition: a subquery used as a value
 * becomes optional, a named projection used as a table source keeps its
 * column types, an outer join makes the nullable side optional, and a set
 * operation widens column by column.
 */
class T4_QueryResult extends munit.FunSuite:
    test("a scalar subquery used as a value becomes optional"):
        from(User).map(u => expect[Option[Int]](from(Post).filter(p => p.userId === u.id).map(p => max(p.viewCount)).asExpr))
        from(User).map(u => expect[Option[Int]](from(Post).filter(p => p.userId === u.id).map(p => p.viewCount).take(1).asExpr))
        from(Wide).map(_ => expect[Option[Long]](from(Wide).map(x => x.score).take(1).asExpr))
        from(User).map(u => expect[Option[String]](from(Post).filter(p => p.userId === u.id).map(p => p.title).take(1).asExpr))

    test("a named projection used as a table source keeps its column types"):
        from(from(User).map(u => (id = u.id, name = u.name))).map(t => expect[Int](t.id))
        from(from(User).map(u => (id = u.id, name = u.name))).map(t => expect[String](t.name))
        from(from(Wide).map(w => (score = w.score))).map(t => expect[Option[Long]](t.score))
        from(from(Nums).map(n => (v = n.i + n.l))).map(t => expect[Long](t.v))
        from(from(Nums).map(n => (v = coalesce(n.oi, 0)))).map(t => expect[Int](t.v))

    test("a single field projection keeps its field name as a table source"):
        from(from(User).map(u => (name = u.name))).map(t => expect[String](t.name))

    test("a set operation widens column by column"):
        from(from(Nums).map(n => (v = n.i)).union(from(Nums).map(n => (v = n.l)))).map(t => expect[Long](t.v))
        from(from(Nums).map(n => (v = n.i)).union(from(Nums).map(n => (v = n.bd)))).map(t => expect[BigDecimal](t.v))
        from(from(Nums).map(n => (v = n.i)).union(from(Nums).map(n => (v = n.oi)))).map(t => expect[Option[Int]](t.v))
        from(from(Nums).map(n => (a = n.i, b = n.f)).union(from(Nums).map(n => (a = n.l, b = n.d))))
            .map(t => (expect[Long](t.a), expect[Double](t.b)))

    test("an outer join makes the nullable side optional"):
        from(User.leftJoin(Post).on((u, p) => u.id === p.userId)).map((_, p) => expect[Option[String]](p.title))
        from(User.leftJoin(Post).on((u, p) => u.id === p.userId)).map((u, _) => expect[Int](u.id))
        from(User.rightJoin(Post).on((u, p) => u.id === p.userId)).map((u, _) => expect[Option[Int]](u.id))
        from(User.fullJoin(Post).on((u, p) => u.id === p.userId)).map((u, p) => (expect[Option[Int]](u.id), expect[Option[String]](p.title)))
        from(User.join(Post).on((u, p) => u.id === p.userId)).map((_, p) => expect[String](p.title))

    test("an outer join keeps an already optional column single-wrapped"):
        from(Wide.leftJoin(Wide).on((a, b) => a.id === b.id)).map((_, b) => expect[Option[Long]](b.score))

    test("a grouping key keeps its type and aggregates stay optional"):
        from(Post).groupBy(p => p.userId).map((g, _) => expect[Int](g))
        from(Post).groupBy(p => p.userId).map((_, _) => expect[Long](count()))
        from(Post).groupBy(p => p.userId).map((_, p) => expect[Option[BigDecimal]](sum(p.viewCount)))
        from(Post).groupBy(p => p.userId).map((_, p) => expect[Option[Int]](max(p.viewCount)))
        from(Post).groupBy(p => (cid = p.userId)).map((g, _) => expect[Int](g.cid))

    test("the recursive level counter is an int"):
        from(Node).connectBy((n, prior) => n.parentId === prior.id).startWith(n => n.parentId.isEmpty)
            .map((_, l) => expect[Int](l))
        from(Node).connectBy((n, prior) => n.parentId === prior.id).startWith(n => n.parentId.isEmpty)
            .map((_, l) => expect[Int](l + 1))

    test("a quantified or existence test yields a boolean"):
        from(User).map(u => expect[Boolean](exists(from(Post).filter(p => p.userId === u.id).map(p => p.id))))
        from(User).map(u => expect[Boolean](u.id.in(from(Post).map(p => p.userId))))
        from(User).map(u => expect[Boolean](u.id === any(from(Post).map(p => p.userId))))
        from(User).map(u => expect[Boolean](u.id > all(from(Post).map(p => p.userId))))
        from(User).map(u => expect[Boolean](u.id.in(List(1, 2, 3))))

    test("an optional operand makes a quantified test optional"):
        from(Wide).map(w => expect[Option[Boolean]](w.score.in(from(Nums).map(n => n.oi))))
        from(Wide).map(w => expect[Option[Boolean]](w.score === any(from(Nums).map(n => n.oi))))
        from(Wide).map(w => expect[Option[Boolean]](w.score.in(List(1L, 2L))))

    test("a lateral join keeps the projected column types"):
        from(User.joinLateral(u => from(Post).filter(p => p.userId === u.id).map(p => (t = p.title))).on((_, _) => true))
            .map((_, p) => expect[String](p.t))
        from(User.leftJoinLateral(u => from(Post).filter(p => p.userId === u.id).map(p => (t = p.title))).on((_, _) => true))
            .map((_, p) => expect[Option[String]](p.t))

    test("projection modifiers keep the result type"):
        from(User).mapDistinct(u => expect[String](u.name))
        from(User).map(u => expect[Int](u.id)).sortBy(u => u.id).take(10).drop(5)
        from(Nums).map(n => expect[Option[BigDecimal]](sum(n.i).filter(n.i > 0)))