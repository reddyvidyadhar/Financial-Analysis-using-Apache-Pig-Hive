REGISTER ./myUDFS.jar;
REGISTER ./Vol.jar;
A = LOAD 'hdfs:///pigdata/*.csv' USING PigStorage(',','-tagFile') AS (fname,date,open,high,low,close,volume,adj_close);
B = FILTER A BY date!='Date';
C = FOREACH B GENERATE $0 AS fname,$1 AS date,$7 AS adj_close;
D = FOREACH C GENERATE fname, FLATTEN(STRSPLIT(date,'-')), adj_close;
E = FOREACH D GENERATE $0 AS name, $1 AS year,$2 AS month, $3 AS day, $4 AS val;
F = GROUP E BY (name,year,month);
G = FOREACH F GENERATE myUDFS.XI($1) AS TEMP:chararray;
H = FOREACH G GENERATE FLATTEN(STRSPLIT(TEMP,'\t')) as (FNAME, XIVAL);				
I = GROUP H BY FNAME;
J = FOREACH I GENERATE Vol.COMPUTE($1) AS TEMP:chararray;
K = FOREACH J GENERATE FLATTEN(STRSPLIT(TEMP,'\t')) as (FN,VOLATILITY);		
FILT = FILTER K by FN!='null';
L = FOREACH FILT GENERATE FN, (double)VOLATILITY;
M = ORDER L BY VOLATILITY ASC;
O = ORDER M BY VOLATILITY DESC;
P = LIMIT M 10;
Q = LIMIT O 10;
RESULT = UNION P,Q;
STORE RESULT INTO 'hdfs:///pigdata/hw3_out';