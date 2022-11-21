DROP TABLE IF EXISTS ticks;
DROP TABLE IF EXISTS base;
CREATE TABLE IF NOT EXISTS base(
  Id    varchar(40) PRIMARY KEY NOT NULL,
  Ex    varchar(5),
  Descr varchar(260),
  SIC   varchar(30),
  Cu    varchar(30)
);
CREATE TABLE IF NOT EXISTS ticks(
  TickID      serial PRIMARY KEY,
  Id          varchar(40) NOT NULL ,
  SeqNo       bigint,
  TradeDate   date,
  Ts          time,
  TradePrice  float,
  TradeSize   float,
  AskPrice    float,
  AskSize     float,
  BidPrice    float,
  BidSize     float,
  Type_       char(3),
  CONSTRAINT fk_security_id
    FOREIGN KEY(Id) REFERENCES base(Id)
);