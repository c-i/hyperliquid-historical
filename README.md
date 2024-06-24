A CLI tool for downloading tick level historical price data from Hyperliquid exchange.  Additionally, there are options for decompressing downloaded lz4 files and converting decompressed files to CSV files.

Requirements: Python 3.10 or above, pip, boto3, lz4

To easily install the required modules, navigate to the directory containing the "requirements.txt" file in your console and use the following command:  `pip install -r requirements.txt` 

Downloaded and decompressed files for a given asset are stored in the /downloads/{asset}/ folder.  CSVs are stored in the /CSV/{asset}/ folder.

<br>

<h3>Usage</h3>

Basic command format: `python hyperliquid-historical.py [options]`


```
usage: hyperliquid-historical.py [-h] {global_settings,download,decompress,to_csv} ...

Retrieve historical tick level market data from Hyperliquid exchange

positional arguments:
  {global_settings,download,decompress,to_csv}
                        tool: download, decompress, to_csv
    download            Download historical market data
    decompress          Decompress downloaded lz4 data
    to_csv              Convert decompressed downloads into formatted CSV

options:
  -h, --help            show this help message and exit
```

<h4>Download</h4>

```
usage: hyperliquid-historical.py download [-h] [--all] [-sd Start date] [-sh Start hour] [-ed End date] [-eh End hour]
                                          Tickers [Tickers ...]

positional arguments:
  Tickers         Tickers of assets to be downloaded seperated by spaces. e.g. BTC ETH

options:
  -h, --help      show this help message and exit
  --all           Apply action to all available dates and times.
  -sd Start date  Starting date as one unbroken string formatted: YYYYMMDD. e.g. 20230916
  -sh Start hour  Hour of the starting day as an integer between 0 and 23. e.g. 9 Default: 0
  -ed End date    Ending date as one unbroken string formatted: YYYYMMDD. e.g. 20230916
  -eh End hour    Hour of the ending day as an integer between 0 and 23. e.g. 9 Default: 23
```

<h4>Decompress</h4>

```
usage: hyperliquid-historical.py decompress [-h] [--all] [-sd Start date] [-sh Start hour] [-ed End date]
                                            [-eh End hour]
                                            Tickers [Tickers ...]

positional arguments:
  Tickers         Tickers of assets to be downloaded seperated by spaces. e.g. BTC ETH

options:
  -h, --help      show this help message and exit
  --all           Apply action to all available dates and times.
  -sd Start date  Starting date as one unbroken string formatted: YYYYMMDD. e.g. 20230916
  -sh Start hour  Hour of the starting day as an integer between 0 and 23. e.g. 9 Default: 0
  -ed End date    Ending date as one unbroken string formatted: YYYYMMDD. e.g. 20230916
  -eh End hour    Hour of the ending day as an integer between 0 and 23. e.g. 9 Default: 23
```

<h4>Convert to CSV</h4>

```
usage: hyperliquid-historical.py to_csv [-h] [--all] [-sd Start date] [-sh Start hour] [-ed End date] [-eh End hour]
                                        Tickers [Tickers ...]

positional arguments:
  Tickers         Tickers of assets to be downloaded seperated by spaces. e.g. BTC ETH

options:
  -h, --help      show this help message and exit
  --all           Apply action to all available dates and times.
  -sd Start date  Starting date as one unbroken string formatted: YYYYMMDD. e.g. 20230916
  -sh Start hour  Hour of the starting day as an integer between 0 and 23. e.g. 9 Default: 0
  -ed End date    Ending date as one unbroken string formatted: YYYYMMDD. e.g. 20230916
  -eh End hour    Hour of the ending day as an integer between 0 and 23. e.g. 9 Default: 23
```

