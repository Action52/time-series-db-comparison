a: select TimeStamp:last TimeStamp where not null TradePrice, TradePrice: last TradePrice where not null TradePrice by Id from priceenum2 where TradeDate=2022.11.03
b: select TimeStamp_final: last TimeStamp where not null TradePrice, TradePrice_final:last TradePrice where not null TradePrice by Id  from priceenum2 where TradeDate=2022.11.04
c: a^b
select [10;>(TradePrice_final-TradePrice)%TradePrice] Id, loss:(TradePrice_final-TradePrice)%TradePrice from c