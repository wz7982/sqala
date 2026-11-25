package sqala.test

import scala.language.unsafeNulls
import scala.reflect.ClassTag

/**
 * Exhaustiveness guard for enum fixtures.
 *
 * The case list is read from the compiled enum, so adding an enum case without
 * registering a fixture fails the test run instead of silently shrinking coverage.
 *
 * Reflection rather than a macro: a macro defined in the same compilation run as
 * its call site suspends the unit, and the -Wsafe-init checker then crashes on
 * the suspended run (NoDenotation.owner in avoidPrivateLeaks) on every clean build.
 */
object Coverage:
    def casesOf[E](using ct: ClassTag[E]): Set[String] =
        val cls = ct.runtimeClass
        val companion = Class.forName(cls.getName + "$")

        val singletons = companion.getDeclaredFields
            .filter(_.getType == cls)
            .map(_.getName)
            .toSet

        val parameterized = cls.getDeclaredClasses
            .map(_.getSimpleName)
            .filterNot(_.contains("$"))
            .toSet

        singletons ++ parameterized

    def assertCoverage[E](using ct: ClassTag[E])(names: String*): Unit =
        val name = ct.runtimeClass.getSimpleName
        val cases = casesOf[E]

        if cases.isEmpty then
            throw AssertionError(s"$name has no enum cases; assertCoverage expects an enum type.")

        val known = names.toSet
        val missing = cases.diff(known).toList.sorted
        val unknown = known.diff(cases).toList.sorted

        if missing.nonEmpty || unknown.nonEmpty then
            val parts = List(
                Option.when(missing.nonEmpty)(s"missing cases: ${missing.mkString(", ")}"),
                Option.when(unknown.nonEmpty)(s"unknown names: ${unknown.mkString(", ")}")
            ).flatten

            throw AssertionError(s"$name fixture coverage mismatch - ${parts.mkString("; ")}.")