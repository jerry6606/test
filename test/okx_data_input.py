import requests
import json
import time
from datetime import datetime
import concurrent.futures
from tqdm import tqdm
import schedule
from collections import defaultdict

API_BASE_URL = "https://www.okx.com/api/v5/market/tickers?instType=SPOT"
OUTPUT_FILE = "okx_token_data.json"
ERROR_LOG_FILE = "okx_error_log.json"
UPDATE_INTERVAL = 600
MAX_WORKERS = 10
MAX_RETRIES = 3
TIMEOUT = 10
PRICE_CHANGE_THRESHOLD = 0.01  # Price change threshold (1%)

# Field name translation (empty since we don't need translation)
FIELD_NAME_TRANSLATION = {}


def translate_field_names(data):
    """Translate field names (no translation needed)"""
    return data


def calculate_price_change(ticker):
    """Calculate price change percentage"""
    try:
        last_price = float(ticker['last'])
        open_price = float(ticker['open24h'])
        if open_price == 0:
            return 0
        return (last_price - open_price) / open_price * 100
    except (KeyError, ValueError, TypeError):
        return 0


def fetch_okx_data(retry_count=0):
    """Fetch OKX token data with retry support"""
    try:
        response = requests.get(API_BASE_URL, timeout=TIMEOUT)
        response.raise_for_status()

        data = response.json()
        if data.get('code') == '0':
            # Add price change percentage field
            processed_data = []
            for ticker in data['data']:
                ticker['priceChangePercent'] = f"{calculate_price_change(ticker):.2f}%"
                processed_data.append(ticker)

            return {
                "status": "success",
                "data": processed_data,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "retries": retry_count
            }
        else:
            error_msg = f"API error: {data.get('msg', 'Unknown error')}"
            raise Exception(error_msg)

    except requests.exceptions.Timeout:
        if retry_count < MAX_RETRIES:
            print(f"Request timeout, retrying ({retry_count + 1}/{MAX_RETRIES})")
            return fetch_okx_data(retry_count + 1)
        raise Exception(f"Max retries reached ({MAX_RETRIES})")

    except Exception as e:
        error_type = type(e).__name__
        return {
            "status": "error",
            "message": f"{error_type}: {str(e)}",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "retries": retry_count
        }


def fetch_individual_token(ticker, retry_count=0):
    """Fetch detailed data for a single token"""
    try:
        return {
            "status": "success",
            "data": ticker,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    except Exception as e:
        if retry_count < MAX_RETRIES:
            return fetch_individual_token(ticker, retry_count + 1)
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }


def filter_changed_tokens(tickers):
    """Filter tokens with price changes exceeding threshold"""
    changed_tokens = []
    for ticker in tickers:
        try:
            price_change = float(ticker['priceChangePercent'].rstrip('%'))
            if abs(price_change) >= PRICE_CHANGE_THRESHOLD:
                changed_tokens.append(ticker)
        except (KeyError, ValueError):
            continue
    return changed_tokens


def analyze_errors(error_results):
    """Analyze and categorize errors"""
    error_stats = defaultdict(int)
    error_details = []

    for result in error_results:
        if 'message' in result:
            error_msg = result['message']
            error_type = error_msg.split(':')[0] if ':' in error_msg else error_msg
            error_stats[error_type] += 1
            error_details.append({
                "error": error_msg,
                "timestamp": result['timestamp'],
                "retries": result.get('retries', 0)
            })

    return error_stats, error_details


def fetch_all_tokens_with_details():
    """Batch fetch detailed data for all tokens"""
    main_data = fetch_okx_data()
    if main_data['status'] != 'success':
        return [], [main_data]

    tickers = main_data['data']
    print(f"\n[Starting fetch] Tokens: {len(tickers)} | Threads: {MAX_WORKERS}")
    start_time = time.time()

    changed_tickers = filter_changed_tokens(tickers)
    print(f"[Price change filter] Tokens changed >{PRICE_CHANGE_THRESHOLD}%: {len(changed_tickers)}")

    if not changed_tickers:
        print("No tokens with significant price changes detected")
        return [], []

    all_data = []
    error_results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_individual_token, ticker): ticker['instId'] for ticker in changed_tickers}

        progress = tqdm(concurrent.futures.as_completed(futures),
                        total=len(changed_tickers),
                        desc="Fetching details")

        for future in progress:
            result = future.result()
            if result['status'] == 'success':
                all_data.append(result['data'])
            else:
                error_results.append(result)

    if error_results:
        error_stats, error_details = analyze_errors(error_results)
        print("\n[Error statistics]")
        for err_type, count in error_stats.items():
            print(f"{err_type}: {count}")

        try:
            with open(ERROR_LOG_FILE, 'w', encoding='utf-8') as f:
                json.dump({
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "total_errors": len(error_results),
                    "error_stats": dict(error_stats),
                    "details": error_details
                }, f, indent=2, ensure_ascii=False)
            print(f"\nError log saved to {ERROR_LOG_FILE}")
        except Exception as e:
            print(f"Failed to save error log: {e}")

    elapsed = time.time() - start_time
    print(f"\n[Fetch complete] Success: {len(all_data)} | Failed: {len(error_results)} | Time: {elapsed:.2f}s")
    return all_data


def save_data(token_data):
    """Save data to file"""
    if not token_data:
        print("No valid data to save")
        return

    try:
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
                if not isinstance(existing_data, list):
                    existing_data = []
        except (FileNotFoundError, json.JSONDecodeError):
            existing_data = []

        existing_data.append({
            "batch_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_count": len(token_data),
            "price_change_threshold": f"{PRICE_CHANGE_THRESHOLD}%",
            "data": token_data
        })

        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, indent=2, ensure_ascii=False)

        print(f"Data saved to {OUTPUT_FILE} (New records: {len(token_data)})")
    except Exception as e:
        print(f"Error saving data: {e}")


def monitoring_job():
    """Scheduled job entry point"""
    print("\n" + "=" * 60)
    print(f"Running monitoring job @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    data = fetch_all_tokens_with_details()
    save_data(data)

    print("=" * 60 + "\n")


if __name__ == "__main__":
    monitoring_job()

    schedule.every(UPDATE_INTERVAL).seconds.do(monitoring_job)

    print(f"OKX token monitoring service started | Update interval: {UPDATE_INTERVAL}s | Price change threshold: {PRICE_CHANGE_THRESHOLD}%")
    print("Waiting for next execution... (Ctrl+C to exit)")

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nMonitoring service stopped")
