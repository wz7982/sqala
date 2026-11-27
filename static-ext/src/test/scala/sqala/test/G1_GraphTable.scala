package sqala.test

import sqala.static.dsl.*
import sqala.static.ext.dsl.*
import sqala.test.ExtFixture.*

/**
 * `GRAPH_TABLE`: `vertex`, `edge`, `createGraph`, the `is` label
 * binder, every pattern symbol combination, edge quantifiers, edge
 * and match filters, and the output columns.
 */
class G1_GraphTable extends munit.FunSuite:
    val friendGraph = createGraph("friend_graph")(
        person = vertex[Person]("person"),
        friends = edge[Friendship]("friendship")
    )

    test("graphTable prints the expected sql"):
        assertEquals(
            sql(from(graphTable(friendGraph)(g =>
                g.matching(
                    ("a" is g.person) |-|
                    ("f" is g.friends.filter(f => f.personAId > 1)) |->|
                    ("b" is g.person)
                ).filter(p => p.a.id > 1)
                .columns(p => (personA = p.a.name, personB = p.b.name, since = p.f.since))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t4"."c1" AS "c1",""",
                """    "t4"."c2" AS "c2",""",
                """    "t4"."c3" AS "c3"""",
                "FROM",
                "    GRAPH_TABLE(",
                """        "friend_graph"""",
                "        MATCH",
                """            ("t1" IS "person") -["t2" IS "friendship" WHERE "t2"."person_a_id" > 1]-> ("t3" IS "person")""",
                "        WHERE",
                """            "t1"."id" > 1""",
                "        COLUMNS(",
                """            "t1"."name" AS "c1",""",
                """            "t3"."name" AS "c2",""",
                """            "t2"."since" AS "c3"""",
                "        )",
                """    ) AS "t4""""
            )
        )

    test("dash-left pattern symbols print the expected sql"):
        assertEquals(
            sql(from(graphTable(friendGraph)(g =>
                g.matching(("a" is g.person) |-| ("f" is g.friends) |-| ("b" is g.person))
                .columns(p => (x = p.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t4"."c1" AS "c1"""",
                "FROM",
                "    GRAPH_TABLE(",
                """        "friend_graph"""",
                "        MATCH",
                """            ("t1" IS "person") -["t2" IS "friendship"]- ("t3" IS "person")""",
                "        COLUMNS(",
                """            "t1"."id" AS "c1"""",
                "        )",
                """    ) AS "t4""""
            )
        )
        assertEquals(
            sql(from(graphTable(friendGraph)(g =>
                g.matching(("a" is g.person) |-| ("f" is g.friends) |~| ("b" is g.person))
                .columns(p => (x = p.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t4"."c1" AS "c1"""",
                "FROM",
                "    GRAPH_TABLE(",
                """        "friend_graph"""",
                "        MATCH",
                """            ("t1" IS "person") -["t2" IS "friendship"]~ ("t3" IS "person")""",
                "        COLUMNS(",
                """            "t1"."id" AS "c1"""",
                "        )",
                """    ) AS "t4""""
            )
        )
        assertEquals(
            sql(from(graphTable(friendGraph)(g =>
                g.matching(("a" is g.person) |-| ("f" is g.friends) |->| ("b" is g.person))
                .columns(p => (x = p.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t4"."c1" AS "c1"""",
                "FROM",
                "    GRAPH_TABLE(",
                """        "friend_graph"""",
                "        MATCH",
                """            ("t1" IS "person") -["t2" IS "friendship"]-> ("t3" IS "person")""",
                "        COLUMNS(",
                """            "t1"."id" AS "c1"""",
                "        )",
                """    ) AS "t4""""
            )
        )
        assertEquals(
            sql(from(graphTable(friendGraph)(g =>
                g.matching(("a" is g.person) |-| ("f" is g.friends) |~>| ("b" is g.person))
                .columns(p => (x = p.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t4"."c1" AS "c1"""",
                "FROM",
                "    GRAPH_TABLE(",
                """        "friend_graph"""",
                "        MATCH",
                """            ("t1" IS "person") -["t2" IS "friendship"]~> ("t3" IS "person")""",
                "        COLUMNS(",
                """            "t1"."id" AS "c1"""",
                "        )",
                """    ) AS "t4""""
            )
        )

    test("tilde-left pattern symbols print the expected sql"):
        assertEquals(
            sql(from(graphTable(friendGraph)(g =>
                g.matching(("a" is g.person) |~| ("f" is g.friends) |-| ("b" is g.person))
                .columns(p => (x = p.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t4"."c1" AS "c1"""",
                "FROM",
                "    GRAPH_TABLE(",
                """        "friend_graph"""",
                "        MATCH",
                """            ("t1" IS "person") ~["t2" IS "friendship"]- ("t3" IS "person")""",
                "        COLUMNS(",
                """            "t1"."id" AS "c1"""",
                "        )",
                """    ) AS "t4""""
            )
        )
        assertEquals(
            sql(from(graphTable(friendGraph)(g =>
                g.matching(("a" is g.person) |~| ("f" is g.friends) |~| ("b" is g.person))
                .columns(p => (x = p.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t4"."c1" AS "c1"""",
                "FROM",
                "    GRAPH_TABLE(",
                """        "friend_graph"""",
                "        MATCH",
                """            ("t1" IS "person") ~["t2" IS "friendship"]~ ("t3" IS "person")""",
                "        COLUMNS(",
                """            "t1"."id" AS "c1"""",
                "        )",
                """    ) AS "t4""""
            )
        )
        assertEquals(
            sql(from(graphTable(friendGraph)(g =>
                g.matching(("a" is g.person) |~| ("f" is g.friends) |->| ("b" is g.person))
                .columns(p => (x = p.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t4"."c1" AS "c1"""",
                "FROM",
                "    GRAPH_TABLE(",
                """        "friend_graph"""",
                "        MATCH",
                """            ("t1" IS "person") ~["t2" IS "friendship"]-> ("t3" IS "person")""",
                "        COLUMNS(",
                """            "t1"."id" AS "c1"""",
                "        )",
                """    ) AS "t4""""
            )
        )
        assertEquals(
            sql(from(graphTable(friendGraph)(g =>
                g.matching(("a" is g.person) |~| ("f" is g.friends) |~>| ("b" is g.person))
                .columns(p => (x = p.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t4"."c1" AS "c1"""",
                "FROM",
                "    GRAPH_TABLE(",
                """        "friend_graph"""",
                "        MATCH",
                """            ("t1" IS "person") ~["t2" IS "friendship"]~> ("t3" IS "person")""",
                "        COLUMNS(",
                """            "t1"."id" AS "c1"""",
                "        )",
                """    ) AS "t4""""
            )
        )

    test("left-arrow pattern symbols print the expected sql"):
        assertEquals(
            sql(from(graphTable(friendGraph)(g =>
                g.matching(("a" is g.person) |<-| ("f" is g.friends) |-| ("b" is g.person))
                .columns(p => (x = p.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t4"."c1" AS "c1"""",
                "FROM",
                "    GRAPH_TABLE(",
                """        "friend_graph"""",
                "        MATCH",
                """            ("t1" IS "person") <-["t2" IS "friendship"]- ("t3" IS "person")""",
                "        COLUMNS(",
                """            "t1"."id" AS "c1"""",
                "        )",
                """    ) AS "t4""""
            )
        )
        assertEquals(
            sql(from(graphTable(friendGraph)(g =>
                g.matching(("a" is g.person) |<-| ("f" is g.friends) |~| ("b" is g.person))
                .columns(p => (x = p.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t4"."c1" AS "c1"""",
                "FROM",
                "    GRAPH_TABLE(",
                """        "friend_graph"""",
                "        MATCH",
                """            ("t1" IS "person") <-["t2" IS "friendship"]~ ("t3" IS "person")""",
                "        COLUMNS(",
                """            "t1"."id" AS "c1"""",
                "        )",
                """    ) AS "t4""""
            )
        )
        assertEquals(
            sql(from(graphTable(friendGraph)(g =>
                g.matching(("a" is g.person) |<-| ("f" is g.friends) |->| ("b" is g.person))
                .columns(p => (x = p.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t4"."c1" AS "c1"""",
                "FROM",
                "    GRAPH_TABLE(",
                """        "friend_graph"""",
                "        MATCH",
                """            ("t1" IS "person") <-["t2" IS "friendship"]-> ("t3" IS "person")""",
                "        COLUMNS(",
                """            "t1"."id" AS "c1"""",
                "        )",
                """    ) AS "t4""""
            )
        )
        assertEquals(
            sql(from(graphTable(friendGraph)(g =>
                g.matching(("a" is g.person) |<-| ("f" is g.friends) |~>| ("b" is g.person))
                .columns(p => (x = p.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t4"."c1" AS "c1"""",
                "FROM",
                "    GRAPH_TABLE(",
                """        "friend_graph"""",
                "        MATCH",
                """            ("t1" IS "person") <-["t2" IS "friendship"]~> ("t3" IS "person")""",
                "        COLUMNS(",
                """            "t1"."id" AS "c1"""",
                "        )",
                """    ) AS "t4""""
            )
        )

    test("left-tilde-arrow pattern symbols print the expected sql"):
        assertEquals(
            sql(from(graphTable(friendGraph)(g =>
                g.matching(("a" is g.person) |<~| ("f" is g.friends) |-| ("b" is g.person))
                .columns(p => (x = p.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t4"."c1" AS "c1"""",
                "FROM",
                "    GRAPH_TABLE(",
                """        "friend_graph"""",
                "        MATCH",
                """            ("t1" IS "person") <~["t2" IS "friendship"]- ("t3" IS "person")""",
                "        COLUMNS(",
                """            "t1"."id" AS "c1"""",
                "        )",
                """    ) AS "t4""""
            )
        )
        assertEquals(
            sql(from(graphTable(friendGraph)(g =>
                g.matching(("a" is g.person) |<~| ("f" is g.friends) |~| ("b" is g.person))
                .columns(p => (x = p.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t4"."c1" AS "c1"""",
                "FROM",
                "    GRAPH_TABLE(",
                """        "friend_graph"""",
                "        MATCH",
                """            ("t1" IS "person") <~["t2" IS "friendship"]~ ("t3" IS "person")""",
                "        COLUMNS(",
                """            "t1"."id" AS "c1"""",
                "        )",
                """    ) AS "t4""""
            )
        )
        assertEquals(
            sql(from(graphTable(friendGraph)(g =>
                g.matching(("a" is g.person) |<~| ("f" is g.friends) |->| ("b" is g.person))
                .columns(p => (x = p.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t4"."c1" AS "c1"""",
                "FROM",
                "    GRAPH_TABLE(",
                """        "friend_graph"""",
                "        MATCH",
                """            ("t1" IS "person") <~["t2" IS "friendship"]-> ("t3" IS "person")""",
                "        COLUMNS(",
                """            "t1"."id" AS "c1"""",
                "        )",
                """    ) AS "t4""""
            )
        )
        assertEquals(
            sql(from(graphTable(friendGraph)(g =>
                g.matching(("a" is g.person) |<~| ("f" is g.friends) |~>| ("b" is g.person))
                .columns(p => (x = p.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t4"."c1" AS "c1"""",
                "FROM",
                "    GRAPH_TABLE(",
                """        "friend_graph"""",
                "        MATCH",
                """            ("t1" IS "person") <~["t2" IS "friendship"]~> ("t3" IS "person")""",
                "        COLUMNS(",
                """            "t1"."id" AS "c1"""",
                "        )",
                """    ) AS "t4""""
            )
        )

    test("edge quantifiers print the expected sql"):
        assertEquals(
            sql(from(graphTable(friendGraph)(g =>
                g.matching(("a" is g.person) |-| ("f" is g.friends.*) |->| ("b" is g.person))
                .columns(p => (x = p.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t4"."c1" AS "c1"""",
                "FROM",
                "    GRAPH_TABLE(",
                """        "friend_graph"""",
                "        MATCH",
                """            ("t1" IS "person") -["t2" IS "friendship"]->* ("t3" IS "person")""",
                "        COLUMNS(",
                """            "t1"."id" AS "c1"""",
                "        )",
                """    ) AS "t4""""
            )
        )
        assertEquals(
            sql(from(graphTable(friendGraph)(g =>
                g.matching(("a" is g.person) |-| ("f" is g.friends.+) |->| ("b" is g.person))
                .columns(p => (x = p.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t4"."c1" AS "c1"""",
                "FROM",
                "    GRAPH_TABLE(",
                """        "friend_graph"""",
                "        MATCH",
                """            ("t1" IS "person") -["t2" IS "friendship"]->+ ("t3" IS "person")""",
                "        COLUMNS(",
                """            "t1"."id" AS "c1"""",
                "        )",
                """    ) AS "t4""""
            )
        )
        assertEquals(
            sql(from(graphTable(friendGraph)(g =>
                g.matching(("a" is g.person) |-| ("f" is g.friends.?) |->| ("b" is g.person))
                .columns(p => (x = p.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t4"."c1" AS "c1"""",
                "FROM",
                "    GRAPH_TABLE(",
                """        "friend_graph"""",
                "        MATCH",
                """            ("t1" IS "person") -["t2" IS "friendship"]->? ("t3" IS "person")""",
                "        COLUMNS(",
                """            "t1"."id" AS "c1"""",
                "        )",
                """    ) AS "t4""""
            )
        )
        assertEquals(
            sql(from(graphTable(friendGraph)(g =>
                g.matching(("a" is g.person) |-| ("f" is g.friends.between(1, 10)) |->| ("b" is g.person))
                .columns(p => (x = p.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t4"."c1" AS "c1"""",
                "FROM",
                "    GRAPH_TABLE(",
                """        "friend_graph"""",
                "        MATCH",
                """            ("t1" IS "person") -["t2" IS "friendship"]->{1,10} ("t3" IS "person")""",
                "        COLUMNS(",
                """            "t1"."id" AS "c1"""",
                "        )",
                """    ) AS "t4""""
            )
        )
        assertEquals(
            sql(from(graphTable(friendGraph)(g =>
                g.matching(("a" is g.person) |-| ("f" is g.friends.least(3)) |->| ("b" is g.person))
                .columns(p => (x = p.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t4"."c1" AS "c1"""",
                "FROM",
                "    GRAPH_TABLE(",
                """        "friend_graph"""",
                "        MATCH",
                """            ("t1" IS "person") -["t2" IS "friendship"]->{3,} ("t3" IS "person")""",
                "        COLUMNS(",
                """            "t1"."id" AS "c1"""",
                "        )",
                """    ) AS "t4""""
            )
        )
        assertEquals(
            sql(from(graphTable(friendGraph)(g =>
                g.matching(("a" is g.person) |-| ("f" is g.friends.most(10)) |->| ("b" is g.person))
                .columns(p => (x = p.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t4"."c1" AS "c1"""",
                "FROM",
                "    GRAPH_TABLE(",
                """        "friend_graph"""",
                "        MATCH",
                """            ("t1" IS "person") -["t2" IS "friendship"]->{,10} ("t3" IS "person")""",
                "        COLUMNS(",
                """            "t1"."id" AS "c1"""",
                "        )",
                """    ) AS "t4""""
            )
        )
        assertEquals(
            sql(from(graphTable(friendGraph)(g =>
                g.matching(("a" is g.person) |-| ("f" is g.friends.at(3)) |->| ("b" is g.person))
                .columns(p => (x = p.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t4"."c1" AS "c1"""",
                "FROM",
                "    GRAPH_TABLE(",
                """        "friend_graph"""",
                "        MATCH",
                """            ("t1" IS "person") -["t2" IS "friendship"]->{3} ("t3" IS "person")""",
                "        COLUMNS(",
                """            "t1"."id" AS "c1"""",
                "        )",
                """    ) AS "t4""""
            )
        )

    test("graph columns keep their field types"):
        assertEquals(
            compileErrors("""from(graphTable(friendGraph)(g =>
                g.matching(("a" is g.person) |-| ("f" is g.friends) |->| ("b" is g.person))
                .columns(p => (personA = p.a.name, since = p.f.since, oid = p.b.id))
            )).map(r => (expect[String](r.personA), expect[Long](r.since), expect[Int](r.oid)))"""),
            ""
        )

    test("matching rejects an unknown label"):
        assert(compileErrors("""from(graphTable(friendGraph)(g =>
            g.matching(("a" is g.unknown) |-| ("f" is g.friends) |->| ("b" is g.person))
            .columns(p => (x = p.a.id))
        )).map(r => r)""").contains("value unknown is not a member"))

    test("an edge filter requires a boolean condition"):
        assert(compileErrors("""from(graphTable(friendGraph)(g =>
            g.matching(("a" is g.person) |-| ("f" is g.friends.filter(f => f.personAId)) |->| ("b" is g.person))
            .columns(p => (x = p.a.id))
        )).map(r => r)""").contains("The arguments type should be boolean, but got scala.Int."))
    test("lateral graphTable columns can be selected in the outer query"):
        assertEquals(
            compileErrors("""from(Person.crossJoinLateral(p => graphTable(friendGraph)(g =>
                g.matching(("a" is g.person) |-| ("f" is g.friends) |->| ("b" is g.person))
                .columns(p2 => (x = p2.a.id))
            ))).map((p, r) => (p.id, r.x))"""),
            ""
        )

    test("lateral graphTable prints the expected sql"):
        assertEquals(
            sql(from(Person.crossJoinLateral(_ => graphTable(friendGraph)(g =>
                g.matching(("a" is g.person) |-| ("f" is g.friends) |->| ("b" is g.person))
                .columns(p2 => (x = p2.a.id))
            ))).map((p, r) => (p.id, r.x))),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1",""",
                """    "t5"."c1" AS "c2"""",
                "FROM",
                """    "person" AS "t1"""",
                "    CROSS JOIN LATERAL GRAPH_TABLE(",
                """        "friend_graph"""",
                "        MATCH",
                """            ("t2" IS "person") -["t3" IS "friendship"]-> ("t4" IS "person")""",
                "        COLUMNS(",
                """            "t2"."id" AS "c1"""",
                "        )",
                """    ) AS "t5""""
            )
        )
