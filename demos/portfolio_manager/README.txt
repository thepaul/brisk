Brisk Portfolio Manager Demo
============================

The portfolio manager demonstrates a hybrid workflow: live changing data(OLTP)
and backend analytics(OLAP).

Our use case is a finance application where customers are actively creating and
managing their portfolios of stocks.

The OLTP Side:
  Each portfolio contains a list of stocks, the number of shares purchased, 
and the price at which they were purchased.

  In our system there is a continuous stream of live price data flowing in
simulating an active market.  Each portfolio can be priced to get the 
overall value based on the current market prices and the percent gain 
or loss since the purchase price.

  At the same time we maintain historical market data every stock, this being
the end-of-day price for each stock going back in time.

  With this historical data we can also the historical performance of each
portfolio.

For this demo the data is all simulated by inserting data into our cassandra
side using the ./bin/pricer tool which creates portfolios, live prices and
historical prices.

The OLAP Side:
  When people value a portfolio they don't just look at price information, 
they want to understand how "risky" their portfolio is.  In this demo we 
ask the question: 

   "What is the most money each portfolio has lost historically if it was held
   it for 10 days?"

Using Brisk and Hive we can easily calculate this value for each portfolio and
insert the result back into cassandra for users to see.  Now users have a much
better understanding of the potential losses they would incur by holding onto
their portfolio.
