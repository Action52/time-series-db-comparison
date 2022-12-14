import "experimental"

from(bucket: "advdb")
|> range(start: 2022-11-21T08:00:00.000Z, stop: 2022-11-21T11:00:00.000Z)
|> filter(fn: (r) => r["_measurement"] == "findata")
|> filter(fn: (r) => r["SIC"] =~ /COMPUTER%*/)
|> aggregateWindow(every: 3h, fn:count)
|> limit(n:3)
|> group(columns: ["ID", "_measurement"], mode:"by")
|> yield(name: "Query 5")
