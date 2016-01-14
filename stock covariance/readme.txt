NYSE Data has the stock exchange data for different stocks, price fluctuations are collected with time, this use case is for finding the covariance among stocks.

load data into HDFS 
    /* copies the data to HDFS to nyse folder */
	hdfs dfs -put '/nyse_stock.csv' /nyse/

Schema Creation:

	create table nyse (exchange String,stock_symbol String,stock_date String,stock_price_open double, stock_price_high double, 
	stock_price_low double, stock_price_close double, stock_volume double, stock_price_adj_close double)
	row format delimited fields terminated by ",";
	
Loading Data:

	load data inpath '/nyse/nyse_stock.csv' into table nyse;
	
Covariance query:

	select a.STOCK_SYMBOL, b.STOCK_SYMBOL, month(a.STOCK_DATE),
		(AVG(a.STOCK_PRICE_HIGH*b.STOCK_PRICE_HIGH) - (AVG(a.STOCK_PRICE_HIGH)*AVG(b.STOCK_PRICE_HIGH)))
	from nyse a join nyse b on a.STOCK_DATE=b.STOCK_DATE
	where a.STOCK_SYMBOL<b.STOCK_SYMBOL and year(a.STOCK_DATE)=2008
	group by a.STOCK_SYMBOL, b. STOCK_SYMBOL, month(a.STOCK_DATE);
	
