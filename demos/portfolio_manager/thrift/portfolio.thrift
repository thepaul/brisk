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
    3: double price
}


struct LeaderBoard
{
    1: list<Portfolio> low_var,
    2: list<Portfolio> high_var
}

service PortfolioMgr
{    
    LeaderBoard get_leaderboard(),

    list<Portfolio> get_portfolios(1:string start_token, 2:i32 limit),
    
    //Portfolio get_portfolio(1:string name),   
    //void add_portfolio(1:string name),
    //void del_portfolio(1:string name),
    
    //void add_constituent(1:string portfolio_name, 2:string ticker),
    //void del_constituent(1:string portfolio_name, 2:string ticker)
}