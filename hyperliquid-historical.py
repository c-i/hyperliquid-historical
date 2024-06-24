import boto3
from botocore import UNSIGNED
from botocore.config import Config
import os
import argparse
from datetime import datetime, timedelta
import asyncio
import lz4.frame
from pathlib import Path
import csv
import json



# MUST USE PATHLIB INSTEAD
DIR_PATH = Path(__file__).parent
BUCKET = "hyperliquid-archive"
CSV_HEADER = ["datetime", "timestamp", "level", "price", "size", "number"]

# s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
# s3.download_file('hyperliquid-archive', 'market_data/20230916/9/l2Book/SOL.lz4', f"{dir_path}/SOL.lz4")

# earliest date: 20230415/0/



def get_args():
    parser = argparse.ArgumentParser(description="Retrieve historical tick level market data from Hyperliquid exchange")
    subparser = parser.add_subparsers(dest="tool", required=True, help="tool: download, decompress, to_csv")

    global_parser = subparser.add_parser("global_settings", add_help=False)
    global_parser.add_argument("t", metavar="Tickers", help="Tickers of assets to be downloaded seperated by spaces. e.g. BTC ETH", nargs="+")
    global_parser.add_argument("--all", help="Apply action to all available dates and times.", action="store_true", default=False)
    global_parser.add_argument("-sd", metavar="Start date", help="Starting date as one unbroken string formatted: YYYYMMDD.  e.g. 20230916")
    global_parser.add_argument("-sh", metavar="Start hour", help="Hour of the starting day as an integer between 0 and 23. e.g. 9  Default: 0", type=int, default=0)
    global_parser.add_argument("-ed", metavar="End date", help="Ending date as one unbroken string formatted: YYYYMMDD.  e.g. 20230916")
    global_parser.add_argument("-eh", metavar="End hour", help="Hour of the ending day as an integer between 0 and 23. e.g. 9  Default: 23", type=int, default=23)
    

    download_parser = subparser.add_parser("download", help="Download historical market data", parents=[global_parser])
    decompress_parser = subparser.add_parser("decompress", help="Decompress downloaded lz4 data", parents=[global_parser])
    to_csv_parser = subparser.add_parser("to_csv", help="Convert decompressed downloads into formatted CSV", parents=[global_parser])


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
    s3.download_file(BUCKET, f"market_data/{date_hour}/l2Book/{asset}.lz4", f"{DIR_PATH}/downloads/{asset}/{date_and_hour[0]}-{date_and_hour[1]}.lz4")




async def download_objects(s3, assets, date_hour_list):
    print(f"Downloading {len(date_hour_list)} objects...")
    for asset in assets:
        await asyncio.gather(*[download_object(s3, asset, date_hour) for date_hour in date_hour_list])




async def decompress_file(asset, date_hour):
    lz_file_path = DIR_PATH / "downloads" / asset / f"{date_hour}.lz4"
    file_path = DIR_PATH / "downloads" / asset / date_hour

    if not lz_file_path.is_file():
        print(f"decompress_file: file not found: {lz_file_path}")
        return

    with lz4.frame.open(lz_file_path, mode='r') as lzfile: 
        data = lzfile.read()
        with open(file_path, "wb") as file:
            file.write(data)




async def decompress_files(assets, date_hour_list):
    print(f"Decompressing {len(date_hour_list)} files...")
    for asset in assets:
        await asyncio.gather(*[decompress_file(asset, date_hour) for date_hour in date_hour_list])




def write_rows(csv_writer, line):
    rows = []
    entry = json.loads(line)
    date_time = entry["time"]
    timestamp = str(entry["raw"]["data"]["time"])
    all_orders = entry["raw"]["data"]["levels"]

    for i, order_level in enumerate(all_orders):
        level = str(i + 1)
        for order in order_level:
            price = order["px"]
            size = order["sz"]
            number = str(order["n"])
            
            rows.append([date_time, timestamp, level, price, size, number])

    for row in rows:
        csv_writer.writerow(row)





async def convert_file(asset, date_hour):
    file_path = DIR_PATH / "downloads" / asset / date_hour
    csv_path = DIR_PATH / "csv" / asset / f"{date_hour}.csv"
    
    with open(csv_path, "w", newline='') as csv_file:
        csv_writer = csv.writer(csv_file, dialect="excel")
        csv_writer.writerow(CSV_HEADER)

        with open(file_path) as file:
            for line in file:
                write_rows(csv_writer, line)




async def files_to_csv(assets, date_hour_list):
    print(f"Converting {len(date_hour_list)} files to CSV...")
    for asset in assets:
        await asyncio.gather(*[convert_file(asset, date_hour) for date_hour in date_hour_list])





def main():
    print(DIR_PATH)
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    args = get_args()

    downloads_path = DIR_PATH / "downloads"
    downloads_path.mkdir(exist_ok=True)

    csv_path = DIR_PATH / "csv"
    csv_path.mkdir(exist_ok=True)

    for asset in args.t:
        downloads_asset_path = downloads_path / asset
        downloads_asset_path.mkdir(exist_ok=True)
        csv_asset_path = csv_path / asset
        csv_asset_path.mkdir(exist_ok=True)

    date_list = make_date_list(args.sd, args.ed)
    loop = asyncio.new_event_loop()
    
    if args.tool == "download":
        date_hour_list = make_date_hour_list(date_list, args.sh, args.eh)
        loop.run_until_complete(download_objects(s3, args.t, date_hour_list))
        loop.close()

    if args.tool == "decompress":
        date_hour_list = make_date_hour_list(date_list, args.sh, args.eh, delimiter="-")
        loop.run_until_complete(decompress_files(args.t, date_hour_list))
        loop.close()

    if args.tool == "to_csv":
        date_hour_list = make_date_hour_list(date_list, args.sh, args.eh, delimiter="-")
        loop.run_until_complete(files_to_csv(args.t, date_hour_list))
        loop.close()


    print("Done")

    
    


if __name__ == "__main__":
    main()
