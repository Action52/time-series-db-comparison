from(bucket: "advdb")
  |> range(start: 2022-11-21T08:00:00.000Z, stop: 2022-11-21T11:00:00.000Z)
  |> filter(fn: (r) => r["_measurement"] == "findata")
  |> filter(fn: (r) => r["Id"] =~ /Security_5%*/)
  |> aggregateWindow(every: 3h, fn: mean, createEmpty: true)
  |> yield(name: "Query 1")
  