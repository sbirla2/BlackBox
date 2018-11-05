import numpy as np
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline import CustomFactor, Pipeline
from quantopian.pipeline.factors import SimpleMovingAverage
from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline.data import morningstar

class Piotroski(CustomFactor):
    inputs = [
        morningstar.operation_ratios.roa,
        morningstar.cash_flow_statement.operating_cash_flow,
        morningstar.cash_flow_statement.cash_flow_from_continuing_operating_activities,
        
        morningstar.operation_ratios.long_term_debt_equity_ratio,
        morningstar.operation_ratios.current_ratio,
        morningstar.valuation.shares_outstanding,
        
        morningstar.operation_ratios.gross_margin,
        morningstar.operation_ratios.assets_turnover,
    ]
    window_length = 92
    
    def compute(self, today, assets, out, roa, cash_flow, cash_flow_from_ops, long_term_debt_ratio, current_ratio, shares_outstanding, gross_margin, assets_turnover):
        profit = ((roa[-1] > 0).astype(int) +
            (cash_flow[-1] > 0).astype(int) +
            (roa[-1] > roa[0]).astype(int) +
            (cash_flow_from_ops[-1] > roa[-1]).astype(int))
        
        leverage = ((long_term_debt_ratio[-1] < long_term_debt_ratio[0]).astype(int) +
            (current_ratio[-1] > current_ratio[0]).astype(int) + 
            (shares_outstanding[-1] <= shares_outstanding[0]).astype(int))
        
        operating = ((gross_margin[-1] > gross_margin[0]).astype(int) +
            (assets_turnover[-1] > assets_turnover[0]).astype(int))
        
        out[:] = profit + leverage + operating
        
def initialize(context):

    pipe = Pipeline()
    pipe = attach_pipeline(pipe, name='piotroski')
    
    piotroski = Piotroski()
    
    pipe.add(piotroski, 'piotroski')
    pipe.set_screen(piotroski >= 7 && piotroski<=3)
    context.is_month_end = False
    schedule_function(set_month_end, date_rules.month_end(1)) 
    schedule_function(trade, date_rules.month_end(), time_rules.market_close())

def set_month_end(context, data):
    context.is_month_end = True
    
def before_trading_start(context, data):
    if context.is_month_end:
        context.results = pipeline_output('piotroski')
        context.long_stocks = context.results.sort_values('piotroski', ascending=False).head(10)
        context.short_stocks = context.results.sort_values('piotroski', ascending=True).head(10)
        context.total_piotroski = context.long_stocks.piotroski.sum()
        context.piotroski_weight = context.long_stocks.piotroski/context.total_piotroski
        update_universe(context.long_stocks.index)
   

def trade(context, data):
    valid_stocks = set(data.keys()).intersection(set(context.long_stocks.index))
    for stock in valid_stocks:
        order_target_percent(stock, context.piotroski_weight[stock])
    for stock in context.portfolio.positions:
        if stock not in valid_stocks:
            order_target_percent(stock, 0)
    context.is_month_end = False
