package sqala.dynamic.dsl

import sqala.ast.order.SqlOrderingItem

final case class Order(private[sqala] val order: SqlOrderingItem)