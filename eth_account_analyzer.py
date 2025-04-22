import requests
import csv
import logging
import argparse
from datetime import datetime
from typing import List, Tuple, Optional, Dict
from dataclasses import dataclass

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@dataclass(frozen=True)
class Config:
    BASE_URL: str = "https://api.etherscan.io/api"
    WEI_TO_ETH: int = 10**18
    DEFAULT_CSV_FILENAME: str = "transactions.csv"
    DEFAULT_TRANSACTION_COUNT: int = 10
    TIMEOUT: int = 10  # seconds

def make_request(params: Dict[str, str]) -> Optional[Dict]:
    """Send a request to the Etherscan API with error handling."""
    try:
        response = requests.get(Config.BASE_URL, params=params, timeout=Config.TIMEOUT)
        response.raise_for_status()
        data = response.json()

        if data.get("status") != "1" or "result" not in data:
            logging.error(f"Etherscan API error: {data.get('message', 'Unknown error')} | Params: {params}")
            return None
        return data
    except requests.RequestException as e:
        logging.error(f"HTTP request failed: {e} | Params: {params}")
        return None

def get_eth_balance(address: str, api_key: str) -> Optional[float]:
    """Return the ETH balance of an address."""
    data = make_request({
        "module": "account",
        "action": "balance",
        "address": address,
        "tag": "latest",
        "apikey": api_key,
    })
    try:
        return int(data["result"]) / Config.WEI_TO_ETH if data else None
    except (ValueError, KeyError):
        logging.error("Failed to parse balance from response.")
        return None

def get_last_transactions(address: str, api_key: str, count: int = Config.DEFAULT_TRANSACTION_COUNT) -> List[Dict]:
    """Return the most recent transactions for an address."""
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
    """Return the current ETH price in USD."""
    data = make_request({
        "module": "stats",
        "action": "ethprice",
        "apikey": api_key,
    })
    try:
        return float(data["result"]["ethusd"]) if data else None
    except (ValueError, KeyError):
        logging.error("Failed to parse ETH price from response.")
        return None

def calculate_transaction_totals(transactions: List[Dict], address: str) -> Tuple[float, float]:
    """Calculate incoming and outgoing ETH totals for the address."""
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
        with open(filename, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()

            for tx in transactions:
                try:
                    row = {
                        key: (
                            datetime.utcfromtimestamp(int(tx["timeStamp"])).isoformat()
                            if key == "timeStamp"
                            else int(tx["value"]) / Config.WEI_TO_ETH
                            if key == "value"
                            else tx.get(key, "")
                        )
                        for key in fieldnames
                    }
                    writer.writerow(row)
                except Exception as e:
                    logging.warning(f"Skipping transaction due to formatting error: {e}")

        logging.info(f"Transactions saved to '{filename}'")
    except IOError as e:
        logging.error(f"Failed to write to CSV file: {e}")

def main():
    parser = argparse.ArgumentParser(description="Fetch Ethereum account data and transactions.")
    parser.add_argument("address", help="Ethereum address to query")
    parser.add_argument("apikey", help="Etherscan API key")
    parser.add_argument("--count", type=int, default=Config.DEFAULT_TRANSACTION_COUNT, help="Number of transactions to retrieve")
    parser.add_argument("--csv", default=Config.DEFAULT_CSV_FILENAME, help="Output CSV file name")
    args = parser.parse_args()

    logging.info(f"Processing Ethereum address: {args.address}")

    balance = get_eth_balance(args.address, args.apikey)
    if balance is not None:
        logging.info(f"ETH Balance: {balance:.4f} ETH")
    else:
        logging.error("Could not retrieve ETH balance.")

    transactions = get_last_transactions(args.address, args.apikey, args.count)
    logging.info(f"Fetched {len(transactions)} transactions.")

    eth_price = get_eth_price(args.apikey)
    if eth_price is not None:
        logging.info(f"Current ETH Price: ${eth_price:.2f}")
    else:
        logging.error("Could not retrieve ETH price.")

    if transactions:
        total_in, total_out = calculate_transaction_totals(transactions, args.address)
        logging.info(f"Total Incoming: {total_in:.4f} ETH | Total Outgoing: {total_out:.4f} ETH")
        save_transactions_to_csv(transactions, args.csv)

if __name__ == "__main__":
    main()
