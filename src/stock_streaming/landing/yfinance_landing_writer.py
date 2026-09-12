import yfinance as yf
from pathlib import Path
from datetime import datetime, timezone

TICKERS = ["GOOG", "NVDA", "MSFT", "TSLA", "AMZN", "META", "AAPL"]
VOLUME_PATH = Path("/Volumes/stock_dev/dev_lamtszhong2014_yfinance/landing/")

def get_output_path() -> Path:
    now = datetime.now(timezone.utc)
    folder_name = now.strftime("%Y-%m-%d")
    filename = now.strftime("%Y%m%d_%H%M%S") + ".parquet"
    return VOLUME_PATH / folder_name / filename

def fetch_and_land() -> None:
    df = (
        yf.Tickers(tickers=TICKERS)
        .history(period="1d", interval="1m", progress=False, threads=True)
        .stack(future_stack=True)
        .reset_index()
        .assign(Datetime=lambda df: df["Datetime"].astype("datetime64[s, UTC]"))
    )

    if df.empty:
        print("fetch data is empty")
        return
    
    output_path = get_output_path()
    output_path.parent.mkdir(exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"land success on {output_path}")

if __name__ == "__main__":
    fetch_and_land()