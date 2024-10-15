package sqala.optimizer.logic

import sqala.optimizer.Query
import sqala.util.*

object LogicOptimizer:
    def optimize(query: Query): Query = 
        query |>
        PullUpSubQuery.pullUpAnySubLink |>
        PullUpSubQuery.pullUpSubQuery