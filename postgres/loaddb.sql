COPY base(Id, Ex, Descr, SIC, Cu)
FROM '/Users/alfredo.leon/Desktop/findata/data/scale_1000/tick_base_file_no_spaces.csv' DELIMITER '|' CSV HEADER;

COPY ticks(Id, SeqNo, TradeDate, Ts, TradePrice, TradeSize, AskPrice, AskSize, BidPrice, BidSize, Type_)
FROM '/Users/alfredo.leon/Desktop/findata/data/scale_1000/tick_price_file_no_spaces.csv'
DELIMITER '|' CSV HEADER;