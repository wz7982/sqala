package sqala.dynamic.dsl

import sqala.ast.statement.SqlSelectItem

final case class SelectItem(private[sqala] val item: SqlSelectItem):
    def ast: SqlSelectItem =
        item