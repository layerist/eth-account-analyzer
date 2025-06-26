import argparse
import csv
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple, Optional, Dict

import requests
from requests.adapters import HTTPAdapter, Retry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Constants and config
@dataclass(frozen=True)
class Config:
    BASE_URL: str = "https://api.etherscan.io/api"
    WEI_TO_ETH: int = 10**18
    DEFAULT_CSV_FILENAME: str = "transactions.csv"
    DEFAULT_TRANSACTION_COUNT: int = 10
    TIMEOUT: int = 10  # seconds
    RETRIES: int = 3

# Create a session with retry logic
session = requests.Session()
retries = Retry(total=Config.RETRIES, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

def make_request(params: Dict[str, str]) -> Optional[Dict]:
    """Make a GET request to the Etherscan API with retries."""
    try:
        response = session.get(Config.BASE_URL, params=params, timeout=Config.TIMEOUT)
        response.raise_for_status()
        data = response.json()
        if data.get("status") != "1" or "result" not in data:
            logging.error(f"Etherscan API error: {data.get('message', 'Unknown error')} | Params: {params}")
            return None
        return data
    except requests.RequestException as e:
        logging.error(f"Request failed: {e} | Params: {params}")
        return None

def get_eth_balance(address: str, api_key: str) -> Optional[float]:
    """Get the ETH balance of a given address."""
    params = {
        "module": "account",
        "action": "balance",
        "address": address,
        "tag": "latest",
        "apikey": api_key
    }
    data = make_request(params)
    if data:
        try:
            return int(data["result"]) / Config.WEI_TO_ETH
        except (ValueError, KeyError):
            logging.exception("Failed to parse ETH balance.")
    return None

def get_eth_price(api_key: str) -> Optional[float]:
    """Get the current ETH price in USD."""
    params = {
        "module": "stats",
        "action": "ethprice",
        "apikey": api_key
    }
    data = make_request(params)
    if data:
        try:
            return float(data["result"]["ethusd"])
        except (ValueError, KeyError, TypeError):
            logging.exception("Failed to parse ETH price.")
    return None

def get_last_transactions(address: str, api_key: str, count: int) -> List[Dict]:
    """Fetch the most recent transactions for an address."""
    params = {
        "module": "account",
        "action": "txlist",
        "address": address,
        "startblock": "0",
        "endblock": "99999999",
        "sort": "desc",
        "apikey": api_key
    }
    data = make_request(params)
    if data:
        return data.get("result", [])[:count]
    return []

def calculate_transaction_totals(transactions: List[Dict], address: str) -> Tuple[float, float]:
    """Calculate total ETH sent and received by the address."""
    total_in = total_out = 0.0
    address = address.lower()

    for tx in transactions:
        try:
            value = int(tx.get("value", 0)) / Config.WEI_TO_ETH
            if tx.get("to", "").lower() == address:
                total_in += value
            elif tx.get("from", "").lower() == address:
                total_out += value
        except (ValueError, TypeError):
            logging.warning(f"Skipping transaction due to invalid value: {tx}")
            continue

    return total_in, total_out

def save_transactions_to_csv(transactions: List[Dict], filename: str) -> None:
    """Save transactions to a CSV file sorted by timestamp (descending)."""
    if not transactions:
        logging.warning("No transactions to save.")
        return

    fieldnames = ["hash", "blockNumber", "timeStamp", "from", "to", "value", "gas", "gasPrice"]

    try:
        # Sort by timeStamp descending
        transactions.sort(key=lambda x: int(x.get("timeStamp", 0)), reverse=True)

        with open(filename, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()

            for tx in transactions:
                try:
                    writer.writerow({
                        "hash": tx.get("hash", ""),
                        "blockNumber": tx.get("blockNumber", ""),
                        "timeStamp": datetime.utcfromtimestamp(int(tx.get("timeStamp", 0))).isoformat(),
                        "from": tx.get("from", ""),
                        "to": tx.get("to", ""),
                        "value": round(int(tx.get("value", 0)) / Config.WEI_TO_ETH, 8),
                        "gas": tx.get("gas", ""),
                        "gasPrice": tx.get("gasPrice", "")
                    })
                except Exception as e:
                    logging.warning(f"Skipping invalid transaction row: {e}")

        logging.info(f"{len(transactions)} transactions saved to '{filename}'")

    except IOError as e:
        logging.error(f"Failed to write CSV file '{filename}': {e}")

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Query Ethereum address balance, fetch recent transactions, and save to CSV."
    )
    parser.add_argument("address", help="Ethereum address to query")
    parser.add_argument("apikey", help="Your Etherscan API key")
    parser.add_argument("--count", type=int, default=Config.DEFAULT_TRANSACTION_COUNT,
                        help=f"Number of recent transactions to fetch (default: {Config.DEFAULT_TRANSACTION_COUNT})")
    parser.add_argument("--csv", default=Config.DEFAULT_CSV_FILENAME,
                        help=f"CSV filename to save transactions (default: '{Config.DEFAULT_CSV_FILENAME}')")

    args = parser.parse_args()

    logging.info(f"Querying Ethereum address: {args.address}")

    balance = get_eth_balance(args.address, args.apikey)
    if balance is not None:
        logging.info(f"ETH Balance: {balance:.4f} ETH")
    else:
        logging.error("Could not retrieve ETH balance.")

    price = get_eth_price(args.apikey)
    if price is not None:
        logging.info(f"Current ETH Price: ${price:.2f}")
    else:
        logging.error("Could not retrieve ETH price.")

    transactions = get_last_transactions(args.address, args.apikey, args.count)
    logging.info(f"Fetched {len(transactions)} transactions.")

    if transactions:
        total_in, total_out = calculate_transaction_totals(transactions, args.address)
        logging.info(f"Total Received: {total_in:.4f} ETH | Total Sent: {total_out:.4f} ETH")
        save_transactions_to_csv(transactions, args.csv)

if __name__ == "__main__":
    main()
