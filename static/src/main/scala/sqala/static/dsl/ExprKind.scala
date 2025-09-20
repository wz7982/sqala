package sqala.static.dsl

sealed trait ExprKind
final class Field extends ExprKind
final class Operation extends ExprKind
final class Value extends ExprKind
final class ValueOperation extends ExprKind
final class Agg extends ExprKind
final class AggOperation extends ExprKind
final class Window extends ExprKind
final class WindowOver extends ExprKind
final class WindowOverEmpty extends ExprKind
final class WindowOverAgg extends ExprKind
final class Grouped extends ExprKind
final class WithoutGrouping extends ExprKind
final class Match extends ExprKind
final class Distinct extends ExprKind