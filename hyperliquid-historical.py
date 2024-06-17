import boto3
from botocore import UNSIGNED
from botocore.config import Config
import os
import argparse
from datetime import datetime, timedelta
import asyncio



DIR_PATH = os.path.dirname(os.path.realpath(__file__))
BUCKET = "hyperliquid-archive"

# s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
# s3.download_file('hyperliquid-archive', 'market_data/20230916/9/l2Book/SOL.lz4', f"{dir_path}/SOL.lz4")



def get_args():
    parser = argparse.ArgumentParser(description="Retrieve historical market data from Hyperliquid exchange")

    parser.add_argument("start_date", help="Starting date as one unbroken string formatted: YYYYMMDD.  e.g. 20230916")
    parser.add_argument("start_hour", help="Hour of the day as an integer between 0 and 23. e.g. 9")
    parser.add_argument("end_date", help="Ending date as one unbroken string formatted: YYYYMMDD.  e.g. 20230916")
    parser.add_argument("end_hour", help="Hour of the day as an integer between 0 and 23. e.g. 9")
    parser.add_argument("tickers", help="Tickers of assets to be downloaded seperated by spaces. e.g. BTC ETH", nargs="+")


    return parser.parse_args()




def make_date_list(start_date, end_date):
    start_date = datetime.strptime(start_date, '%Y%m%d')
    end_date = datetime.strptime(end_date, '%Y%m%d')
    
    date_list = []
    
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.strftime('%Y%m%d'))
        current_date += timedelta(days=1)
    
    return date_list




def make_date_hour_list(date_list, start_hour, end_hour):
    date_hour_list = []
    end_date = date_list[-1]
    hour = int(start_hour)
    end = 23
    for date in date_list:
        if date == end_date:
            end = int(end_hour)

        while hour <= end:
            date_hour = date + "/" + str(hour)
            date_hour_list.append(date_hour)
            hour += 1

        hour = 0

    return date_hour_list




async def download_object(s3, asset, date_hour):
    date_and_hour = date_hour.split("/")
    s3.download_file(BUCKET, f"market_data/{date_hour}/l2Book/{asset}.lz4", f"{DIR_PATH}/downloads/{asset}/{date_and_hour[0]}-{date_and_hour[1]}")




async def download_objects(s3, assets, date_hour_list):
    print("Downloading objects...")
    for asset in assets:
        await asyncio.gather(*[download_object(s3, asset, date_hour) for date_hour in date_hour_list])



def main():
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    args = get_args()

    if not os.path.isdir("downloads"):
        os.mkdir("downloads")

    for asset in args.tickers:
        if not os.path.isdir(f"downloads/{asset}"):
            os.mkdir(f"downloads/{asset}")

    date_list = make_date_list(args.start_date, args.end_date)
    date_hour_list = make_date_hour_list(date_list, args.start_hour, args.end_hour)

    loop = asyncio.new_event_loop()
    loop.run_until_complete(download_objects(s3, args.tickers, date_hour_list))
    loop.close()
    
    print("Done")
    


if __name__ == "__main__":
    main()
