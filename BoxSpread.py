import numpy as np
import math
import scipy.stats
from scipy.stats import norm

def initialize(context):
    context.stock = sid(24)   
    context.cash = 1000000
    context.optiondict = []
    context.strikePercent = .9
    

def before_trading_start(context, data):
        price = data.history(context.stock,'price',1,'1d')
        
        for item in context.optiondict:
            #moving closer to expiration date
            item[2] = item[2] - float(1)/21
            
            #last day before expiration
            if item[2] <= 0:
            
            #execute option if it is price > strike
                if item[1] < price:
                    context.cash = context.cash + price - item[1]
                    
                context.optiondict.remove(item)    
        
        #value of portfolio = cash on hand + fair market value of all options
        context.portfolioValue = context.cash
        for item in context.optiondict:
            context.portfolioValue = context.portfolioValue + optionPrice(context, data, item[1], item[2])
                  

#change to schedule function, much easier to do
def handle_data(context, data):
     buyOption(context, data, 0, 0)
               
    
#need to change call and put functions into a time check
#instead of using black scholes 


def buyOption(context, data, strike, time):
    if strike == 0:
         strike = data.history(context.stock,'price',1,'1d').mean() 
            
    #set to 1 month        
    if time == 0:
        time = 1
           
    if context.cash > optionPrice(context, data, 0, 0):
        context.cash = context.cash - optionPrice(context, data, 0, 0)
        order(context.stock, optionPrice(context, data, 0, 0))
    
    #dictionary to keep track of strike prices and maybe later on other things(rankings of the most undervalued options)
    temp = []
    temp.append(context.stock.symbol)
    temp.append(strike)
    temp.append(float(time))
    context.optiondict.append(temp)

    return   
    
def sellOption(context, index, data):
    if index < len(context.optiondict):
        context.cash = context.cash + optionPrice(context, data, context.optiondict[index][1], context.optiondict[index][2])
       
    if(context.time == 1):
        order(context.stock, 0)

        context.optiondict.pop(index)

    return    
      
#helper function to see if the price is undervalued 

def optionPrice(context, data, strike, time):
    
    price = data.history(context.stock,'price',1,'1d').mean()
    
                    
   #if no strike is specified set to strikePercent*price             
    if strike == 0:
        strike = price*context.strikePercent
                
    price_history = data.history(context.stock, "price", bar_count=5,         frequency="1d")
    sdev = price_history.std()/price
    rf = .0122
    
    #if no time was specified set price to 1 month            
    if time == 0:
        time = 1
    
    return call(price,strike,rf,sdev,time)
    
def d1(price,strike,rf,sdev,time):
    return (np.log(price/strike)+(rf+0.5*sdev**2)*time)/(sdev*math.sqrt(time))
    
def d2(price,strike,rf,sdev,time):
    d1 = (np.log(price/strike)+(rf+0.5*sdev**2)*time)/(sdev*math.sqrt(time))
    return (d1 - (sdev * math.sqrt(time)))

def call(price,strike,rf,sdev,time):
            return (price*norm.cdf((d1(price,strike,rf,sdev,time))) - strike*math.exp(-rf*time)*norm.cdf((d2(price,strike,rf,sdev,time))))
