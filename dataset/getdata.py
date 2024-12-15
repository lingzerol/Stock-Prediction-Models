import akshare as ak
import time
import datetime
import os
import argparse
import tqdm

def get_akshare_stock_codes():
    stock_sector_detail_df = ak.stock_info_a_code_name()
    codes = []
    for _, row in stock_sector_detail_df.iterrows():
        codes.append(row["code"])
    return codes

def download_akshare_stock_histories(codes: list, granularity:str, save_path: str):
    os.makedirs(save_path, exist_ok=True)
    if granularity != "daily" and datetime.datetime.today().weekday() >= 5:
        print("today is not weekday")
        return
    now = datetime.datetime.now()
    date_str = now.strftime("%Y%m%d")
    start_date = now.strftime("%Y-%m-%d") + " 09:30:00"
    end_date = now.strftime("%Y-%m-%d") + " 15:00:00"
    for code in tqdm.tqdm(codes):
        for _ in range(3):
            try:
                os.makedirs(os.path.join(save_path, code), exist_ok=True)
                if granularity == "daily":
                    df = ak.stock_zh_a_hist(symbol=code, period=granularity, end_date=date_str, adjust="hfq")
                    columns = ["Date", "Code", "Open", "Close", "Hight", "Low", "Volumn", "Transaction_Volume", "Amplitude", "Chg", "Percentage_Change", "Turnover_Rate"]
                    df.columns = columns
                    df.to_csv(os.path.join(save_path, code, f"{code}-daily.csv"), index=False)
                else:
                    df = ak.stock_zh_a_hist_min_em(symbol=code, start_date=start_date, end_date=end_date, period="1", adjust="hfq")
                    columns = ["Time", "Open", "Close", "Hight", "Low", "Volumn", "Transaction_Volume", "Average"]
                    df.columns = columns
                    df.to_csv(os.path.join(save_path, code, f"{code}-{date_str}.csv"), index=False)
                time.sleep(0.1)
                break
            except Exception as e:
                print(e)
            time.sleep(0.1)

def main(args):
    if args.stock_type == "akshare":
        codes = get_akshare_stock_codes()
        download_akshare_stock_histories(codes, args.granularity, args.save_path)
    else:
        print(f"not support stoch price {args.stoch_price}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--save_path", default="./dataset/akshare", type=str, help="the path to save data")
    parser.add_argument("-t", "--stock_type", default="akshare", type=str, help="the type of stock", choices=["akshare"])
    parser.add_argument("-g", "--granularity", default="daily", type=str, help="the granularity of the stock data", choices=["daily", "minute"])
    args = parser.parse_args()
    main(args)