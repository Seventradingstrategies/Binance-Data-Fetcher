import requests
import streamlit as st
import pandas as pd
import time
import base64
from datetime import datetime, timedelta, date

base_url = 'https://api.binance.com/api/v3/klines'


def fetch_data(symbol, interval, start_time=None, end_time=None):
    params = {
        'symbol': symbol,
        'interval': interval,
        'limit': 1000
    }

    if start_time:
        params['startTime'] = start_time
    if end_time:
        params['endTime'] = end_time

    response = requests.get(base_url, params=params)
    data = response.json()

    df = pd.DataFrame(data, columns=['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                                     'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume',
                                     'taker_buy_quote_asset_volume', 'ignore'])

    df = df[['open_time', 'open', 'high', 'low', 'close', 'volume']]
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')

    return df


def get_all_trading_pairs():
    pairs_url = "https://api.binance.com/api/v3/exchangeInfo"
    response = requests.get(pairs_url)
    symbols = response.json()["symbols"]
    trading_pairs = [symbol["symbol"] for symbol in symbols if symbol["status"] == "TRADING"]
    return trading_pairs


def get_pair_counts(all_pairs, base_currencies):
    counts = {}
    for base in base_currencies:
        counts[base] = sum(1 for pair in all_pairs if pair.endswith(base))
    return counts


def main():
    st.title("Binance Data Fetcher")

    all_pairs = get_all_trading_pairs()
    base_currencies = ["USDT", "BTC", "ETH", "BNB", "XRP"]
    pair_counts = get_pair_counts(all_pairs, base_currencies)

    base_currency_options = [f"{base} ({pair_counts[base]} pairs)" for base in base_currencies]
    selected_base_label = st.selectbox("Select Base Currency", base_currency_options)
    selected_base = selected_base_label.split(" ")[0]

    # Filter trading pairs based on selected base currency
    filtered_pairs = [pair for pair in all_pairs if pair.endswith(selected_base)]
    symbol = st.selectbox("Select Trading Pair", filtered_pairs)

    interval = st.selectbox("Select Interval", ["1m", "5m", "15m", "30m", "45m", "1h", "4h", "1d"], index=6)
    start_date = st.date_input("Select start date", date(2017, 1, 1))

    intervals_minutes = {
        "1m": 1, "5m": 5, "15m": 15, "30m": 30, "45m": 45, "1h": 60, "4h": 240, "1d": 1440
    }

    elapsed_mins = (datetime.now() - datetime.combine(start_date, datetime.min.time())).total_seconds() / 60
    total_required_points = elapsed_mins / intervals_minutes[interval]
    total_pages = int(total_required_points / 1000) + 1

    data_frames = []
    start_timestamp = int(start_date.strftime("%s")) * 1000

    progress_bar = st.progress(0)
    time_remaining_placeholder = st.empty()
    total_request_time = 0

    with st.spinner("Fetching Data..."):
        for i in range(total_pages):
            page_start_time = time.time()

            df_page = fetch_data(symbol, interval, start_time=start_timestamp)

            request_duration = time.time() - page_start_time
            total_request_time += request_duration
            average_request_time = total_request_time / (i + 1)
            estimated_time_remaining = average_request_time * (total_pages - i - 1)

            time_remaining_placeholder.text(f"Estimated time remaining: {estimated_time_remaining:.2f} seconds.")

            if df_page.empty:
                break

            start_timestamp = int(df_page.iloc[-1]['open_time'].timestamp() * 1000) + 1
            data_frames.append(df_page)

            progress = int((i + 1) / total_pages * 100)
            progress_bar.progress(progress)

            time.sleep(0.05)

    time_taken = time.time() - page_start_time
    df = pd.concat(data_frames, ignore_index=True)
    datasets_received = df.shape[0]

    st.write(f"Data fetched in {time_taken:.2f} seconds.")
    st.write(f"Received {datasets_received} data sets.")
    st.dataframe(df.tail(100000))

    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="binance_data.csv">Download CSV File</a>'
    st.markdown(href, unsafe_allow_html=True)

if __name__ == "__main__":
    main()