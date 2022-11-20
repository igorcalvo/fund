# import pandas as pd
# from bs4 import BeautifulSoup
# import re
# from selenium import webdriver
# import chromedriver_binary
# import string
# pd.options.display.float_format = '{:.0f}'.format
#
# is_link = 'https://finance.yahoo.com/quote/AAPL/financials?p=AAPL'
# driver = webdriver.Chrome()
# driver.get(is_link)
# html = driver.execute_script('return document.body.innerHTML;')
# soup = BeautifulSoup(html,'lxml')



# import json
# from yahoofinancials import YahooFinancials
#
# ticker = 'tots3.SA'
# yahoo_financials = YahooFinancials(ticker)
# result = yahoo_financials.get_financial_stmts('quarterly', 'income')
# # print(result)
# print(json.dumps(result, indent=4))



from yahoo_fin import stock_info as si
import yfinance as yf
import pandas as pd
# import matplotlib.pyplot as plt
# import pandas_datareader
from IPython.display import display

# pd.set_option('display.max_columns', None)
#
# income_statement = si.get_income_statement("viva3.sa", yearly=True)
# display(income_statement)
#
# from yahoofinance import IncomeStatement
# req = IncomeStatement('AAPL')




# from yahooquery import Ticker
# import json
#
# aapl = Ticker('tots3.sa')
#
# aapl.summary_detail
#
# print(aapl)
# print(aapl.summary_detail)
# print(json.dumps(aapl.key_stats, indent=4))