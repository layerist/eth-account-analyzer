import requests
import csv
import logging
import argparse
from datetime import datetime
from typing import List, Tuple, Optional, Dict
from dataclasses import dataclass

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@dataclass(frozen=True)
class Config:
    BASE_URL: str = "https://api.etherscan.io/api"
    WEI_TO_ETH: int = 10**18
    DEFAULT_CSV_FILENAME: str = "transactions.csv"
    DEFAULT_TRANSACTION_COUNT: int = 10
    TIMEOUT: int = 10  # seconds

def make_request(params: Dict[str, str]) -> Optional[Dict]:
    """Make an HTTP GET request to the Etherscan API."""
    try:
        response = requests.get(Config.BASE_URL, params=params, timeout=Config.TIMEOUT)
        response.raise_for_status()
        data = response.json()

        if data.get("status") != "1" or "result" not in data:
            message = data.get("message", "Unknown error")
            logging.error(f"Etherscan API error: {message} | Params: {params}")
            return None

        return data
    except requests.RequestException as e:
        logging.error(f"HTTP request failed: {e} | Params: {params}")
        return None

def get_eth_balance(address: str, api_key: str) -> Optional[float]:
    """Get the ETH balance of an address."""
    data = make_request({
        "module": "account",
        "action": "balance",
        "address": address,
        "tag": "latest",
        "apikey": api_key,
    })
    try:
        return int(data["result"]) / Config.WEI_TO_ETH if data else None
    except (ValueError, KeyError, TypeError):
        logging.exception("Failed to parse ETH balance.")
        return None

def get_last_transactions(address: str, api_key: str, count: int = Config.DEFAULT_TRANSACTION_COUNT) -> List[Dict]:
    """Fetch the most recent transactions for a given address."""
    data = make_request({
        "module": "account",
        "action": "txlist",
        "address": address,
        "startblock": "0",
        "endblock": "99999999",
        "sort": "desc",
        "apikey": api_key,
    })
    return data.get("result", [])[:count] if data else []

def get_eth_price(api_key: str) -> Optional[float]:
    """Fetch the current ETH price in USD."""
    data = make_request({
        "module": "stats",
        "action": "ethprice",
        "apikey": api_key,
    })
    try:
        return float(data["result"]["ethusd"]) if data else None
    except (ValueError, KeyError, TypeError):
        logging.exception("Failed to parse ETH price.")
        return None

def calculate_transaction_totals(transactions: List[Dict], address: str) -> Tuple[float, float]:
    """Calculate incoming and outgoing ETH totals for the specified address."""
    address = address.lower()
    total_in = sum(int(tx["value"]) / Config.WEI_TO_ETH for tx in transactions if tx.get("to", "").lower() == address)
    total_out = sum(int(tx["value"]) / Config.WEI_TO_ETH for tx in transactions if tx.get("from", "").lower() == address)
    return total_in, total_out

def save_transactions_to_csv(transactions: List[Dict], filename: str) -> None:
    """Save a list of transactions to a CSV file."""
    if not transactions:
        logging.warning("No transactions to save.")
        return

    fieldnames = ["hash", "blockNumber", "timeStamp", "from", "to", "value", "gas", "gasPrice"]

    try:
        with open(filename, mode="w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for tx in transactions:
                try:
                    row = {}
                    for key in fieldnames:
                        if key == "timeStamp":
                            timestamp = int(tx.get("timeStamp", 0))
                            row[key] = datetime.utcfromtimestamp(timestamp).isoformat()
                        elif key == "value":
                            row[key] = round(int(tx.get("value", 0)) / Config.WEI_TO_ETH, 8)
                        else:
                            row[key] = tx.get(key, "")
                    writer.writerow(row)
                except Exception as e:
                    logging.warning(f"Skipping a transaction due to formatting error: {e}")

        logging.info(f"Transactions saved to '{filename}'")
    except IOError as e:
        logging.error(f"Failed to write CSV file: {e}")

def main() -> None:
    parser = argparse.ArgumentParser(description="Query Ethereum address data and recent transactions.")
    parser.add_argument("address", help="Ethereum address to query")
    parser.add_argument("apikey", help="Etherscan API key")
    parser.add_argument("--count", type=int, default=Config.DEFAULT_TRANSACTION_COUNT, help="Number of recent transactions to fetch")
    parser.add_argument("--csv", default=Config.DEFAULT_CSV_FILENAME, help="Output CSV file name")
    args = parser.parse_args()

    address = args.address
    api_key = args.apikey

    logging.info(f"Fetching data for Ethereum address: {address}")

    balance = get_eth_balance(address, api_key)
    if balance is not None:
        logging.info(f"ETH Balance: {balance:.4f} ETH")
    else:
        logging.error("Unable to retrieve ETH balance.")

    transactions = get_last_transactions(address, api_key, args.count)
    logging.info(f"Retrieved {len(transactions)} transactions.")

    eth_price = get_eth_price(api_key)
    if eth_price is not None:
        logging.info(f"Current ETH Price: ${eth_price:.2f}")
    else:
        logging.error("Unable to retrieve ETH price.")

    if transactions:
        total_in, total_out = calculate_transaction_totals(transactions, address)
        logging.info(f"Total Incoming: {total_in:.4f} ETH | Total Outgoing: {total_out:.4f} ETH")
        save_transactions_to_csv(transactions, args.csv)

if __name__ == "__main__":
    main()
