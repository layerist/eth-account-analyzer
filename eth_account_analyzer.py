import argparse
import csv
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple, Optional, Dict

import requests

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

@dataclass(frozen=True)
class Config:
    BASE_URL: str = "https://api.etherscan.io/api"
    WEI_TO_ETH: int = 10**18
    DEFAULT_CSV_FILENAME: str = "transactions.csv"
    DEFAULT_TRANSACTION_COUNT: int = 10
    TIMEOUT: int = 10  # seconds

def make_request(params: Dict[str, str]) -> Optional[Dict]:
    """Send a GET request to the Etherscan API and handle response."""
    try:
        response = requests.get(Config.BASE_URL, params=params, timeout=Config.TIMEOUT)
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
    """Return ETH balance of a given address."""
    params = {
        "module": "account",
        "action": "balance",
        "address": address,
        "tag": "latest",
        "apikey": api_key
    }
    data = make_request(params)
    if not data:
        return None
    try:
        return int(data["result"]) / Config.WEI_TO_ETH
    except (ValueError, KeyError):
        logging.exception("Error parsing ETH balance.")
        return None

def get_last_transactions(address: str, api_key: str, count: int) -> List[Dict]:
    """Return the latest transactions for the address."""
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
    return data.get("result", [])[:count] if data else []

def get_eth_price(api_key: str) -> Optional[float]:
    """Fetch current ETH price in USD."""
    params = {
        "module": "stats",
        "action": "ethprice",
        "apikey": api_key
    }
    data = make_request(params)
    try:
        return float(data["result"]["ethusd"]) if data else None
    except (ValueError, KeyError, TypeError):
        logging.exception("Error parsing ETH price.")
        return None

def calculate_transaction_totals(transactions: List[Dict], address: str) -> Tuple[float, float]:
    """Calculate incoming and outgoing ETH totals for a given address."""
    address = address.lower()
    total_in = total_out = 0.0

    for tx in transactions:
        try:
            value_eth = int(tx.get("value", 0)) / Config.WEI_TO_ETH
            if tx.get("to", "").lower() == address:
                total_in += value_eth
            elif tx.get("from", "").lower() == address:
                total_out += value_eth
        except (ValueError, TypeError):
            logging.warning("Skipping transaction with invalid value.")

    return total_in, total_out

def save_transactions_to_csv(transactions: List[Dict], filename: str) -> None:
    """Write transaction list to a CSV file."""
    if not transactions:
        logging.warning("No transactions to write.")
        return

    fieldnames = ["hash", "blockNumber", "timeStamp", "from", "to", "value", "gas", "gasPrice"]

    try:
        with open(filename, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
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
                    logging.warning(f"Skipping invalid transaction: {e}")

        logging.info(f"Saved {len(transactions)} transactions to '{filename}'")
    except IOError as e:
        logging.error(f"Could not write to file '{filename}': {e}")

def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch ETH address balance, recent transactions, and save to CSV.")
    parser.add_argument("address", help="Ethereum address to query")
    parser.add_argument("apikey", help="Etherscan API key")
    parser.add_argument("--count", type=int, default=Config.DEFAULT_TRANSACTION_COUNT, help="Number of recent transactions to fetch")
    parser.add_argument("--csv", default=Config.DEFAULT_CSV_FILENAME, help="Output CSV file name")
    args = parser.parse_args()

    address = args.address
    api_key = args.apikey

    logging.info(f"Processing Ethereum address: {address}")

    balance = get_eth_balance(address, api_key)
    if balance is not None:
        logging.info(f"ETH Balance: {balance:.4f} ETH")
    else:
        logging.error("Failed to retrieve ETH balance.")

    transactions = get_last_transactions(address, api_key, args.count)
    logging.info(f"Fetched {len(transactions)} transactions.")

    eth_price = get_eth_price(api_key)
    if eth_price is not None:
        logging.info(f"Current ETH Price: ${eth_price:.2f}")
    else:
        logging.error("Failed to retrieve ETH price.")

    if transactions:
        total_in, total_out = calculate_transaction_totals(transactions, address)
        logging.info(f"Total Incoming: {total_in:.4f} ETH | Total Outgoing: {total_out:.4f} ETH")
        save_transactions_to_csv(transactions, args.csv)

if __name__ == "__main__":
    main()
