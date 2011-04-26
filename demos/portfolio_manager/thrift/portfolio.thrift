namespace java com.datastax.demo.portfolio

struct Stock
{
    1: string ticker,
    2: double price,
}

struct Portfolio 
{
    1: string name,
    2: list<Stock> constituents,
    3: double basis,
    4: double price,
    5: double largest_10day_loss,
    6: string largest_10day_loss_date
}


service PortfolioMgr
{    
    list<Portfolio> get_portfolios(1:string start_token, 2:i32 limit),
}