package sqala.dsl

import sqala.dsl.macros.TableMacro

trait AsTable[T]:
    type R

object AsTable:
    transparent inline given derived[T]: AsTable[T] = ${ TableMacro.asTableMacro[T] }