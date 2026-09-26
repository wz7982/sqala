import sqala.static.dsl.*
import sqala.metadata.PostgresqlDialect

import java.time.LocalDateTime


case class StockPrice(
    stockSymbol: String,
    tradeTime: LocalDateTime,
    price: BigDecimal
)


object Test:
    def main(args: Array[String]): Unit =



    
        val q =
            from:
                StockPrice.matchRecognize: s =>
                    // 分组和排序，类似窗口函数
                    s.partitionBy(s.stockSymbol)
                    .sortBy(s.tradeTime)
                    // 行匹配模式 支持oneRowPerMatch和allRowsPerMatch
                    .oneRowPerMatch
                    // 预定义标签，使用字面量类型创建，后续会为模式生成该名称的字段，以做到类型安全
                    .predefine[("start", "down", "bottom", "up")]
                    // 标签的定义，参数是命名元组，名称需要与预定义中对应
                    .define: d =>
                        (
                            start = false,
                            // prev方法引用上一个匹配到的值，scala支持导航函数prev、next、first、last
                            down = d.down.price < prev(d.down.price),
                            bottom = d.bottom.price < prev(d.bottom.price) && next(d.bottom.price) > d.bottom.price,
                            up = d.up.price > prev(d.up.price)
                        )
                    // 定义模式，SQL中使用空格连接两个模式，sqala使用~，匹配或使用|
                    // permute(p1, p2, p3) 对应PERMUTE(p1, p2, p3)
                    // exclusion(p) 对应{- p -}
                    // 量词支持+ * ?
                    // least(n) 对应{n,}
                    // most(n) 对应{,n}
                    // between(m, n) 对应{m, n}
                    // at(n) 对应{n}
                    .pattern(d => d.start ~ d.down.+ ~ d.bottom ~ d.up.+)
                    // 支持afterMatchSkipToNextRow、afterMatchSkipPastLastRow、afterMatchSkipToFirst、afterMatchSkipToLast、afterMatchSkipTo(p)
                    .afterMatchSkipTo(d => d.up)
                    // 定义度量，即MATCH_RECOGNIZE表最后返回的字段，使用命名元组
                    .measures: d =>
                        (
                            startTime = d.start.tradeTime,
                            bottomTime = d.bottom.tradeTime,
                            // 支持RUNNING和FINAL获取模式
                            endTime = finalized(last(d.up.tradeTime)),
                            // 获取匹配到的编号使用matchNumber方法
                            matchNum = matchNumber(),
                            // 获取匹配到的标签名称使用classifier方法
                            matchVar = classifier()
                        )


        println(q.sql(PostgresqlDialect, true))