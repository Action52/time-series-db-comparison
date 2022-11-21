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