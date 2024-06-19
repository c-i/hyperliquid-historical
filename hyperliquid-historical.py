import boto3
from botocore import UNSIGNED
from botocore.config import Config
import os
import argparse
from datetime import datetime, timedelta
import asyncio
import lz4.frame




DIR_PATH = os.path.dirname(os.path.realpath(__file__))
BUCKET = "hyperliquid-archive"

# s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
# s3.download_file('hyperliquid-archive', 'market_data/20230916/9/l2Book/SOL.lz4', f"{dir_path}/SOL.lz4")

# earliest date: 20230415/0/



def get_args():
    parser = argparse.ArgumentParser(description="Retrieve historical tick level market data from Hyperliquid exchange")
    subparser = parser.add_subparsers(dest="tool", required=True, help="tool: download, decompress, to_csv")

    global_parser = subparser.add_parser("global_settings", add_help=False)
    global_parser.add_argument("--all", help="Apply action to all available dates and times.", action="store_true", default=False)
    global_parser.add_argument("-sd", help="Starting date as one unbroken string formatted: YYYYMMDD.  e.g. 20230916")
    global_parser.add_argument("-sh", help="Hour of the starting day as an integer between 0 and 23. e.g. 9", type=int, default=0)
    global_parser.add_argument("-ed", help="Ending date as one unbroken string formatted: YYYYMMDD.  e.g. 20230916")
    global_parser.add_argument("-eh", help="Hour of the ending day as an integer between 0 and 23. e.g. 9", type=int, default=23)
    global_parser.add_argument("-t", help="Tickers of assets to be downloaded seperated by spaces. e.g. BTC ETH", nargs="+")

    download_parser = subparser.add_parser("download", help="Download historical market data", parents=[global_parser])
    decompress_parser = subparser.add_parser("decompress", help="Decompress downloaded lz4 data", parents=[global_parser])


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




def make_date_hour_list(date_list, start_hour, end_hour, delimiter="/"):
    date_hour_list = []
    end_date = date_list[-1]
    hour = start_hour
    end = 23
    for date in date_list:
        if date == end_date:
            end = end_hour

        while hour <= end:
            date_hour = date + delimiter + str(hour)
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




def decompress_files(assets, date_hour_list):
    pass




def main():
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    args = get_args()

    if not os.path.isdir("downloads"):
        os.mkdir("downloads")

    for asset in args.t:
        if not os.path.isdir(f"downloads/{asset}"):
            os.mkdir(f"downloads/{asset}")

    date_list = make_date_list(args.sd, args.ed)
    
    if args.tool == "download":
        date_hour_list = make_date_hour_list(date_list, args.sh, args.eh)
        loop = asyncio.new_event_loop()
        loop.run_until_complete(download_objects(s3, args.t, date_hour_list))
        loop.close()
        
        print("Done")

    if args.tool == "decompress":
        date_hour_list = make_date_hour_list(date_list, args.sh, args.eh, delimiter="-")

    
    


if __name__ == "__main__":
    main()
