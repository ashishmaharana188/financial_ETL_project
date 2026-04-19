import yfinance as yf
import pandas as pd

msft = yf.Ticker("MSFT")

dfy = msft.get_income_stmt(as_dict=False, pretty=False, freq="yearly")
print("Print Yearly:", "\n", dfy, "\n")

dfq = msft.get_income_stmt(as_dict=False, pretty=False, freq="quarterly")
print("Print Quarterly:", "\n", dfq, "\n")
