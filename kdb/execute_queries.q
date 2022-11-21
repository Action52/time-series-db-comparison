files:.Q.opt .z.x;
show tickfile:files[`tickfile];
/ Load price tick file
tick:("SIDTFIFIFIS"; enlist"|")0:`:/Users/alfredo.leon/Desktop/findata/data/scale_1000/tick_price_file_no_spaces.csv;
/ Create enumeration for table (this is required to create a splayed table and then a partitioned table)
tickenum: .Q.en[`:../data/test/raw/tickprice/] tick;
/ Save table
/ rsave `tickenum

/ Load base tick file
base:("SSSSS"; enlist"|")0:`:/Users/alfredo.leon/Desktop/findata/data/scale_1000/tick_base_file_no_spaces.csv;
/ Create enumeration for table (this is required to create a splayed table and then a partitioned table)
baseenum: .Q.en[`:../data/test/raw/basetick/] base;
/ Save table
/rsave `baseenum

/ Query 1
secs : select[101] Id from tickenum;
securities: (exec Id from secs);  / This is a list
show select from tickenum where Id in securities, TradeDate=2022.11.21, TimeStamp within 09:00:00.00 12:00:00.00;

/ Query 2
select sum(TradePrice * TradeSize) % sum(TradeSize) from tickenum where Id=`Security_1, TradeDate=2022.11.21, TimeStamp within 09:00:00.00 12:00:00.00
/ Query 3
show select[10] from
    `LossPercentage xasc `Id`LossPercentage xcol
    select Id, (ClosingPriceToday-ClosingPricePreviousDay)*100%ClosingPricePreviousDay from(
        `Id`ClosingPriceToday`ClosingPricePreviousDay xcol
        (
            (`Id`ClosingPriceToday xcol select first TradePrice by Id from (`TimeStamp xdesc
                select Id, TradePrice, TimeStamp
                from tickenum
                where TradeDate=2022.11.22, TradePrice>0.0)) ij
            (`Id`ClosingPricePreviousDay xcol select first TradePrice by Id from (`TimeStamp xdesc
                select Id, TradePrice, TimeStamp
                from tickenum
                where TradeDate=2022.11.23, TradePrice>0.0))
        )
    );

exit 0;