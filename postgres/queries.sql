-- 1.
SELECT *
FROM ticks
WHERE Id IN (SELECT Id From base LIMIT 100)
  AND TradeDate = '2022.11.21'
  AND ts >= '9:00:00.00'
  AND ts < '12:00:00.00';

--2.
SELECT SUM(TradePrice * TradeSize) / SUM(TradeSize) AS volume_weighted_price
FROM ticks
WHERE Id='Security_1'
  AND TradeDate='2022.11.21'
  AND ts >= '9:00:00.00'
  AND ts < '12:00:00.00';

--3.
WITH closingpricetoday AS(
  SELECT DISTINCT Id, FIRST_VALUE(TradePrice) OVER (PARTITION BY Id ORDER BY ts DESC) AS TradePrice
  FROM ticks
  WHERE TradeDate='2022.11.22'
    AND TradePrice > 0.0
), closingpricepreviousday AS(
  SELECT DISTINCT Id, FIRST_VALUE(TradePrice) OVER (PARTITION BY Id ORDER BY ts DESC) AS TradePrice
  FROM ticks
  WHERE TradeDate='2022.11.23'
    AND TradePrice > 0.0
)
SELECT
  closingpricetoday.Id,
  --closingpricetoday.TradePrice, closingpricepreviousday.TradePrice,
  (closingpricetoday.TradePrice
        -closingpricepreviousday.TradePrice)*100/closingpricepreviousday.TradePrice
  AS loss_percentage
FROM closingpricetoday JOIN closingpricepreviousday USING(Id)
ORDER BY loss_percentage
LIMIT 10;

--4.
select Id, sum(TradeSize) TradeCumulative
from ticks
where TradeDate = '2022.11.03'
group by Id
order by sum(TradeSize) DESC
limit 10;

--5.
select B.id, count(*) TradeCumulative
from ticks T inner join base B on(T.id= B.id)
where B.sic= 'COMPUTERS'
group by B.id
order by count(*) DESC
limit 1;

--6.
WITH
LAStB (id, bidprice, lASttime) AS
(SELECT id, bidprice, ROW_NUMBER() OVER 
(PARTITION BY id ORDER BY tradedate,Ts DESC) AS rown FROM ticks WHERE bidprice is not null),
LAStA (id,ASkprice, lASttime) AS (SELECT id, ASkprice, ROW_NUMBER() OVER 
(PARTITION BY id ORDER BY tradedate,Ts DESC) rown FROM ticks WHERE ASkprice is not null),
allids (id, rank) AS
(SELECT a.id, rank() OVER (ORDER BY (2*(b.ASkprice-a.bidprice) / (b.ASkprice+a.bidprice)) DESC)
FROM LAStB a, LAStA b
WHERE a.id=b.id AND a.lASttime=1 AND b.lASttime=1)
SELECT id
FROM allids
WHERE rank < 11;