import numpy as np
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline import CustomFactor, Pipeline
from quantopian.pipeline.factors import SimpleMovingAverage
from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline.data import morningstar

class Piotroski(CustomFactor):
    inputs = [
        morningstar.income_statement.gross_profit,     morningstar.operation_ratios.roa,
        morningstar.cash_flow_statement.operating_cash_flow,
        morningstar.cash_flow_statement.cash_flow_from_continuing_operating_activities,
        
        morningstar.operation_ratios.long_term_debt_equity_ratio,
        morningstar.operation_ratios.current_ratio,
        morningstar.valuation.shares_outstanding,
        morningstar.income_statement.net_income,
        
        morningstar.operation_ratios.gross_margin,
        morningstar.operation_ratios.assets_turnover,
    ]
    window_length = 22
   
    def compute(self, today, assets, out, net_income, roa, gross_profit, cash_flow, cash_flow_from_ops, long_term_debt_ratio, current_ratio, shares_outstanding, gross_margin, assets_turnover):
         profit = ((net_income[0] > 0).astype(int) +
                  (roa[0] > 0).astype(int) +
                  (cash_flow[0] > 0).astype(int) +
                  (cash_flow_from_ops[0] < net_income[0]).astype(int))     
         leverage = ((long_term_debt_ratio[-1] > long_term_debt_ratio[0]).astype(int) + 
                (current_ratio[-1] < current_ratio[0]).astype(int) +
                (shares_outstanding[-1] >= shares_outstanding[0]).astype(int))
                                
         operating = ((gross_margin[-1] < net_income[0]).astype(int) +
                       (assets_turnover[-1] < assets_turnover[0]).astype(int))
         out[:] = profit + leverage + operating

def initialize(context):

    pipe = Pipeline()
    pipe = attach_pipeline(pipe, name='piotroski')
    
    piotroski = Piotroski()
    
    market_cap = morningstar.valuation.market_cap > 5e7
    
    pipe.add(piotroski, 'piotroski')
    pipe.set_screen((piotroski >= 7) & market_cap)
    context.is_month_end = False
    schedule_function(set_month_end, date_rules.month_end(1)) 
    schedule_function(trade_long, date_rules.month_end(), time_rules.market_open())
    # schedule_function(trade_short, date_rules.month_end(), time_rules.market_open())
    schedule_function(trade, date_rules.month_end(), time_rules.market_close())

def set_month_end(context, data):
    context.is_month_end = True
    
def before_trading_start(context, data):
    if context.is_month_end:
        context.results = pipeline_output('piotroski')
        context.long_stocks = context.results.sort_values('piotroski', ascending=False).head(10)
        # context.short_stocks = context.results.sort_values('piotroski', ascending=True).head(10)
   
def trade_long(context, data):
    for stock in context.long_stocks.index:
        if data.can_trade(stock):
            order_target_percent(stock, .1)

def trade_short(context, data):
    for stock in context.short_stocks.index:
        if data.can_trade(stock):
            order_target_percent(stock, -.1)


def trade(context, data):
    print "------------------"
    print context.long_stocks 
    print context.short_stocks
     
    for stock in context.portfolio.positions:
        if stock not in context.long_stocks.index and stock not in context.short_stocks.index:
         if stock not in context.long_stocks.index:
            order_target_percent(stock, 0)
    context.is_month_end = False
