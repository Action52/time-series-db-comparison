from(bucket: "advdb")
|> range(start: 2022-11-21T00:00:00.000Z, stop: 2022-11-22T00:00:00.000Z)
|> filter(fn: (r) => r["_measurement"] == "findata")
|> filter(fn: (r) => r._field == "TradeSize" or r._field == "TradePrice")
|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
|> map(
    fn: (r) => ({ 
        r with
        TotalSP: (r.TradeSize * r.TradePrice),
    }),
)
|> reduce(
fn: (r, accumulator) => ({
    sum1: r.TradeSize + accumulator.sum1,
    sum2: r.TotalSP + accumulator.sum2,   

}),identity: {sum1: 0.0, sum2: 0.0},)
|> map(
    fn: (r) => ({ 
        r with
        Volume: r.sum2/r.sum1,
    }),)
|> group(columns: ["Id"], mode:"by")
|> limit(n:1)
|> yield()
