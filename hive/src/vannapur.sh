
DROP TABLE IF EXISTS stock_vol;

SHOW DATABASES;

CREATE TABLE stock_vol(
date STRING,
open FLOAT,
high FLOAT,
low FLOAT,
close FLOAT,
volume INT,
adjClose FLOAT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '$1/*.csv' OVERWRITE INTO TABLE stock_vol;

SHOW DATABASES;

CREATE TABLE A AS select INPUT__FILE__NAME,split(date, '-')[0] as year,split(date, '-')[1] as month,split(date, '-')[2] as day,adjclose from stock_vol where date !='Date';

CREATE TABLE MM_DAY AS select INPUT__FILE__NAME as file,year as year,month as month,MAX(day) AS lastday,MIN(day) as firstday from A GROUP BY INPUT__FILE__NAME,year,month;

CREATE TABLE MAXPRICE AS Select A.input__file__name as file,A.month as month,A.year as year,A.adjclose as val from A join MM_DAY on A.input__file__name = MM_DAY.file AND A.month = MM_DAY.month AND A.year = MM_DAY.year where A.day = MM_DAY.lastday;

CREATE TABLE MINPRICE AS Select A.input__file__name as file,A.month as month,A.year as year,A.adjclose as val from A join MM_DAY on A.input__file__name = MM_DAY.file AND A.month = MM_DAY.month AND A.year = MM_DAY.year where A.day = MM_DAY.firstday;

DROP TABLE A;

DROP TABLE MM_DAY;

CREATE TABLE OCVALS AS Select MAXPRICE.file as file, MAXPRICE.year as year, MAXPRICE.month as month, MAXPRICE.val as last, MINPRICE.val as first from MAXPRICE join MINPRICE on MAXPRICE.file=MINPRICE.file AND MAXPRICE.year=MINPRICE.year AND MAXPRICE.month=MINPRICE.month;


DROP TABLE MAXPRICE;
DROP TABLE MINPRICE;

CREATE TABLE XIVALUE AS SELECT file, year, month, (last-first)/first as x_i from OCVALS;

DROP TABLE OCVALS;

CREATE TABLE T3 AS Select XIVALUE.file as file, AVG(x_i) as x_bar from XIVALUE GROUP BY XIVALUE.file;

CREATE TABLE XIXBAR AS SELECT XIVALUE.file,XIVALUE.year,XIVALUE.month, XIVALUE.x_i, t3.x_bar from t3 join XIVALUE on t3.file = XIVALUE.file;

DROP TABLE XIVALUE;
DROP TABLE T3;

CREATE TABLE vol AS select XIXBAR.file as stock, sqrt(SUM((x_i-x_bar)*(x_i-x_bar))/(Count(*)-1)) as volatility from XIXBAR GROUP by file;

DROP TABLE XIXBAR;
select * from vol where volatility IS NOT NULL AND volatility >0.0 ORDER BY volatility DESC LIMIT 10;

select * from vol where volatility IS NOT NULL AND volatility >0.0 ORDER BY volatility ASC LIMIT 10;