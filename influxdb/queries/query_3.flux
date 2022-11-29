import "join"
import "experimental"

yesteday = from(bucket: "advdb")
|> range(start: 2022-11-17T00:00:00.000Z, stop: 2022-11-17T23:59:59.999Z)
|> filter(fn: (r) => r._measurement == "findata")
|> filter(fn: (r) => r._field == "Id" or r._field == "TradePrice")
|> group(columns: ["Id"])
|> last()

today = from(bucket: "advdb")
|> range(start: 2022-11-18T00:00:00.000Z, stop: 2022-11-19T23:59:59.999Z)
|> filter(fn: (r) => r._measurement == "findata")
|> filter(fn: (r) => r._field == "Id" or r._field == "TradePrice")
|> group(columns: ["Id"])
|> last()

join.full(
    left: yesteday |> group(),
    right: today |> group(),
    on: (l, r) => l.Id == r.Id,
    as: (l, r) => {
        yesterday = if exists l._value then l._value else 0.0
        today = if exists r._value then r._value else 0.0

        return {_start: l._start, _stop: l._stop, _time: l._time, Id: l.Id, Loss: (today*100.0)/yesterday, yesteday: yesterday, today: today}
    },
)
|> top(n: 10, columns: ["Loss"])
|> yield()
