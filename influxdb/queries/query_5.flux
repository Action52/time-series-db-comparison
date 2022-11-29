import "experimental"

from(bucket: "advdb")
|> range(start: 2022-11-21T00:00:00.000Z, stop: 2022-11-21T03:00:00.000Z)
|> filter(fn: (r) => r["_measurement"] == "findata")
|> filter(fn: (r) => r["SIC"] =~ /COMPUTER%*/)
|> aggregateWindow(every: 1h, fn:count)
|> limit(n:3)
|> group(columns: ["ID", "_measurement"], mode:"by")
|> top(n:10, columns: ["_value"])
|> yield(name: "Query 5")
