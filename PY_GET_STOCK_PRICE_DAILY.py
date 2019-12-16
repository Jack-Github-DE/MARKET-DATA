'''
# This line only need to run once to install the package. 
# Once the package is installed then it's good to go

#!pip install yfinance

# I got the following error message:

#twisted 18.7.0 requires PyHamcrest>=1.9.0, which is not installed.
#You are using pip version 10.0.1, however version 19.3.1 is available.
#You should consider upgrading via the 'python -m pip install --upgrade pip' command.
'''

import yfinance as yf
import time
import datetime
import pandas as pd
import os

# Define a func to return head and tail of a df
def ends(df,x=1):
    dfends=df.head(x).append(df.tail(x))
    return dfends 


# Official site of SP500: https://us.spindices.com/indices/equity/sp-500; but they don't have the list to download
# The list is down loaded from WIKI: https://en.wikipedia.org/wiki/List_of_S%26P_500_companies


stocks =['A',	'AAL',	'AAP',	'AAPL',	'ABBV',	'ABC',	'ABMD',	'ABT',	'ACN',	'ADBE',
        'ADI',	'ADM',	'ADP',	'ADS',	'ADSK',	'AEE',	'AEP',	'AES',	'AFL',	'AGN',
        'AIG',	'AIV',	'AIZ',	'AJG',	'AKAM',	'ALB',	'ALGN',	'ALK',	'ALL',	'ALLE',
        'ALXN',	'AMAT',	'AMCR',	'AMD',	'AME',	'AMG',	'AMGN',	'AMP',	'AMT',	'AMZN',
        'ANET',	'ANSS',	'ANTM',	'AON',	'AOS',	'APA',	'APD',	'APH',	'APTV',	'ARE',
        'ARNC',	'ATO',	'ATVI',	'AVB',	'AVGO',	'AVY',	'AWK',	'AXP',	'AZO',	'BA',
        'BAC',	'BAX',	'BBY',	'BDX',	'BEN',	'BF.B',	'BIIB',	'BK',	'BKNG',	'BKR',
        'BLK',	'BLL',	'BMY',	'BR',	'BRK.B',	'BSX',	'BWA',	'BXP',	'C',	'CAG',
        'CAH',	'CAT',	'CB',	'CBOE',	'CBRE',	'CCI',	'CCL',	'CDNS',	'CDW',	'CE',
        'CERN',	'CF',	'CFG',	'CHD',	'CHRW',	'CHTR',	'CI',	'CINF',	'CL',	'CLX',
        'CMA',	'CMCSA',	'CME',	'CMG',	'CMI',	'CMS',	'CNC',	'CNP',	'COF',	'COG',
        'COO',	'COP',	'COST',	'COTY',	'CPB',	'CPRI',	'CPRT',	'CRM',	'CSCO',	'CSX',
        'CTAS',	'CTL',	'CTSH',	'CTVA',	'CTXS',	'CVS',	'CVX',	'CXO',	'D',	'DAL',
        'DD',	'DE',	'DFS',	'DG',	'DGX',	'DHI',	'DHR',	'DIS',	'DISCA',	'DISCK',
        'DISH',	'DLR',	'DLTR',	'DOV',	'DOW',	'DRE',	'DRI',	'DTE',	'DUK',	'DVA',
        'DVN',	'DXC',	'EA',	'EBAY',	'ECL',	'ED',	'EFX',	'EIX',	'EL',	'EMN',
        'EMR',	'EOG',	'EQIX',	'EQR',	'ES',	'ESS',	'ETFC',	'ETN',	'ETR',	'EVRG',
        'EW',	'EXC',	'EXPD',	'EXPE',	'EXR',	'F',	'FANG',	'FAST',	'FB',	'FBHS',
        'FCX',	'FDX',	'FE',	'FFIV',	'FIS',	'FISV',	'FITB',	'FLIR',	'FLS',	'FLT',
        'FMC',	'FOX',	'FOXA',	'FRC',	'FRT',	'FTI',	'FTNT',	'FTV',	'GD',	'GE',
        'GILD',	'GIS',	'GL',	'GLW',	'GM',	'GOOG',	'GOOGL',	'GPC',	'GPN',	'GPS',
        'GRMN',	'GS',	'GWW',	'HAL',	'HAS',	'HBAN',	'HBI',	'HCA',	'HD',	'HES',
        'HFC',	'HIG',	'HII',	'HLT',	'HOG',	'HOLX',	'HON',	'HP',	'HPE',	'HPQ',
        'HRB',	'HRL',	'HSIC',	'HST',	'HSY',	'HUM',	'IBM',	'ICE',	'IDXX',	'IEX',
        'IFF',	'ILMN',	'INCY',	'INFO',	'INTC',	'INTU',	'IP',	'IPG',	'IPGP',	'IQV',
        'IR',	'IRM',	'ISRG',	'IT',	'ITW',	'IVZ',	'JBHT',	'JCI',	'JEC',	'JKHY',
        'JNJ',	'JNPR',	'JPM',	'JWN',	'K',	'KEY',	'KEYS',	'KHC',	'KIM',	'KLAC',
        'KMB',	'KMI',	'KMX',	'KO',	'KR',	'KSS',	'KSU',	'L',	'LB',	'LDOS',
        'LEG',	'LEN',	'LH',	'LHX',	'LIN',	'LKQ',	'LLY',	'LMT',	'LNC',	'LNT',
        'LOW',	'LRCX',	'LUV',	'LVS',	'LW',	'LYB',	'M',	'MA',	'MAA',	'MAC',
        'MAR',	'MAS',	'MCD',	'MCHP',	'MCK',	'MCO',	'MDLZ',	'MDT',	'MET',	'MGM',
        'MHK',	'MKC',	'MKTX',	'MLM',	'MMC',	'MMM',	'MNST',	'MO',	'MOS',	'MPC',
        'MRK',	'MRO',	'MS',	'MSCI',	'MSFT',	'MSI',	'MTB',	'MTD',	'MU',	'MXIM',
        'MYL',	'NBL',	'NCLH',	'NDAQ',	'NEE',	'NEM',	'NFLX',	'NI',	'NKE',	'NLOK',
        'NLSN',	'NOC',	'NOV',	'NOW',	'NRG',	'NSC',	'NTAP',	'NTRS',	'NUE',	'NVDA',
        'NVR',	'NWL',	'NWS',	'NWSA',	'O',	'ODFL',	'OKE',	'OMC',	'ORCL',	'ORLY',
        'OXY',	'PAYX',	'PBCT',	'PCAR',	'PEAK',	'PEG',	'PEP',	'PFE',	'PFG',	'PG',
        'PGR',	'PH',	'PHM',	'PKG',	'PKI',	'PLD',	'PM',	'PNC',	'PNR',	'PNW',
        'PPG',	'PPL',	'PRGO',	'PRU',	'PSA',	'PSX',	'PVH',	'PWR',	'PXD',	'PYPL',
        'QCOM',	'QRVO',	'RCL',	'RE',	'REG',	'REGN',	'RF',	'RHI',	'RJF',	'RL',
        'RMD',	'ROK',	'ROL',	'ROP',	'ROST',	'RSG',	'RTN',	'SBAC',	'SBUX',	'SCHW',
        'SEE',	'SHW',	'SIVB',	'SJM',	'SLB',	'SLG',	'SNA',	'SNPS',	'SO',	'SPG',
        'SPGI',	'SRE',	'STT',	'STX',	'STZ',	'SWK',	'SWKS',	'SYF',	'SYK',	'SYY',
        'T',	'TAP',	'TDG',	'TEL',	'TFC',	'TFX',	'TGT',	'TIF',	'TJX',	'TMO',
        'TMUS',	'TPR',	'TRIP',	'TROW',	'TRV',	'TSCO',	'TSN',	'TTWO',	'TWTR',	'TXN',
        'TXT',	'UA',	'UAA',	'UAL',	'UDR',	'UHS',	'ULTA',	'UNH',	'UNM',	'UNP',
        'UPS',	'URI',	'USB',	'UTX',	'V',	'VAR',	'VFC',	'VIAC',	'VLO',	'VMC',
        'VNO',	'VRSK',	'VRSN',	'VRTX',	'VTR',	'VZ',	'WAB',	'WAT',	'WBA',	'WCG',
        'WDC',	'WEC',	'WELL',	'WFC',	'WHR',	'WLTW',	'WM',	'WMB',	'WMT',	'WRB',
        'WRK',	'WU',	'WY',	'WYNN',	'XEC',	'XEL',	'XLNX',	'XOM',	'XRAY',	'XRX',
        'XYL',	'YUM',	'ZBH',	'ZION',	'ZTS',  'SPY']


start = datetime.datetime(2015,1,1)
end = datetime.datetime(2020,1,1)

def get_symbol_price(symbols):

    stock_df=pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume','Symbol'])

    for ticker in symbols:

        ticker_data=yf.download(ticker,start,end)
        ticker_data['Symbol']=ticker
        ticker_data.reset_index(inplace=True)
        stock_df=stock_df.append(ticker_data).reset_index(drop=True)
        
    return stock_df 

df=get_symbol_price(stocks)
df.to_csv(os.path.join('C:/Users/FN.LN/Documents/Python/Data/Market', 'SP500_FIVE_YR_RAW.csv'),index=False)

# After saving the raw data as CSV, read it for inspection of NaN/Blanks
stock_raw_df=pd.read_csv(os.path.join('C:/Users/FN.LN/Documents/Market', 'SP500_FIVE_YR_RAW.csv'),sep=',')

# Count # of records by Symbol
symbol_count=stock_raw_df.groupby(['Symbol']).size().reset_index(name='Row Count')
stock_with_record_count=pd.merge(stock_raw_df,symbol_count, on='Symbol',how='left')

# A quick check showed there are problem raw data whose count is 1247, more than the correct one 1246
##print("Unique number of trading days: ", pd.unique(stock_with_record_count['Row Count'])) # this line works too
stock_with_record_count.groupby('Row Count')['Symbol'].nunique()

# for example, symbol M has date 2019-12-12 doubled with NaN
stock_with_record_count[stock_with_record_count['Symbol']=='M']

#For the counts very different from the right one (1246), their ticker will be dropped. 
#for the counts very close (1247), looks they have one more blank row, I will only drop that one blank row. 
#Don't worry about the order of the Date, when do the analysis, those symbols' dataframe will be sorted by date and joined by date later

# Now drop any row with column 'Open'= NaN 
stock_no_na=stock_raw_df.dropna(subset=['Open']).reset_index(drop=True)
stock_no_na[stock_no_na['Symbol']=='M'] # NaN dropped

# after dropping na, count # of rows again
symbol_count=stock_no_na.groupby(['Symbol']).size().reset_index(name='Row Count')
stock_no_na_count=pd.merge(stock_no_na,symbol_count, on='Symbol',how='left')
ends(stock_no_na_count)

# Now the 1247 count is gone
stock_no_na_count.groupby('Row Count')['Symbol'].nunique()

# Only select the symbols with count == 1246, i.e symbols with the correct # of trading days through out the tracking period
stock_cleaned=stock_no_na_count[stock_no_na_count['Row Count']==1246].reset_index(drop=True)
ends(stock_cleaned)

# below are the problem tickers that are excluded
prob_ticker=stock_no_na_count[stock_no_na_count['Row Count']!=1246].reset_index(drop=True)
pd.unique(prob_ticker['Symbol']).tolist()

# Before saving the cleaned data, sort by symbol and date
stock_cleaned_sorted=stock_cleaned.sort_values(['Symbol','Date'])
ends(stock_cleaned_sorted)

# Save cleaned and sorted data
stock_cleaned_sorted.to_csv(os.path.join('C:/Users/FN.LN/Documents/Market', 'SP500_FIVE_YR_FULL_PERIOD.csv'),sep=',', index=False)
