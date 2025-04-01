import requests
import csv
import logging
import argparse
from datetime import datetime
from typing import List, Tuple, Optional, Dict
from dataclasses import dataclass

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@dataclass
class Config:
    BASE_URL: str = "https://api.etherscan.io/api"
    WEI_TO_ETH: int = 10**18
    DEFAULT_CSV_FILENAME: str = "transactions.csv"
    DEFAULT_TRANSACTION_COUNT: int = 10
    TIMEOUT: int = 10  # seconds

def make_request(params: Dict[str, str]) -> Optional[Dict]:
    """Send a request to the Etherscan API and handle errors."""
    try:
        response = requests.get(Config.BASE_URL, params=params, timeout=Config.TIMEOUT)
        response.raise_for_status()
        data = response.json()
        
        if data.get("status") != "1":
            logging.error(f"API Error: {data.get('message', 'Unknown error')} - Params: {params}")
            return None
        return data
    except requests.RequestException as e:
        logging.error(f"Request failed: {e} - Params: {params}")
        return None

def get_eth_balance(address: str, api_key: str) -> Optional[float]:
    """Retrieve the Ethereum balance of the specified address."""
    data = make_request({
        "module": "account",
        "action": "balance",
        "address": address,
        "tag": "latest",
        "apikey": api_key,
    })
    return int(data["result"]) / Config.WEI_TO_ETH if data else None

def get_last_transactions(address: str, api_key: str, count: int = Config.DEFAULT_TRANSACTION_COUNT) -> List[Dict]:
    """Retrieve the most recent transactions for the specified address."""
    data = make_request({
        "module": "account",
        "action": "txlist",
        "address": address,
        "startblock": "0",
        "endblock": "99999999",
        "sort": "desc",
        "apikey": api_key,
    })
    transactions = data.get("result", []) if data else []
    return transactions[:count]  # Limit transactions

def get_eth_price(api_key: str) -> Optional[float]:
    """Retrieve the current Ethereum price in USD."""
    data = make_request({"module": "stats", "action": "ethprice", "apikey": api_key})
    return float(data["result"]["ethusd"]) if data else None

def calculate_transaction_totals(transactions: List[Dict], address: str) -> Tuple[float, float]:
    """Calculate total incoming and outgoing Ethereum for a given address."""
    address_lower = address.lower()
    total_incoming = sum(int(tx["value"]) / Config.WEI_TO_ETH for tx in transactions if tx.get("to", "").lower() == address_lower)
    total_outgoing = sum(int(tx["value"]) / Config.WEI_TO_ETH for tx in transactions if tx.get("from", "").lower() == address_lower)
    return total_incoming, total_outgoing

def save_transactions_to_csv(transactions: List[Dict], filename: str) -> None:
    """Save Ethereum transactions to a CSV file."""
    if not transactions:
        logging.warning("No transactions to save.")
        return

    try:
        with open(filename, mode="w", newline="", encoding="utf-8") as file:
            fieldnames = ["hash", "blockNumber", "timeStamp", "from", "to", "value", "gas", "gasPrice"]
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()

            for tx in transactions:
                tx["value"] = int(tx.get("value", 0)) / Config.WEI_TO_ETH
                tx["timeStamp"] = datetime.utcfromtimestamp(int(tx["timeStamp"])).isoformat()
                writer.writerow({key: tx.get(key, "") for key in fieldnames})

        logging.info(f"Transactions saved to {filename}")
    except IOError as e:
        logging.error(f"Error writing CSV file: {e}")

def main():
    parser = argparse.ArgumentParser(description="Fetch and analyze Ethereum transactions.")
    parser.add_argument("address", type=str, help="Ethereum address")
    parser.add_argument("apikey", type=str, help="Etherscan API key")
    parser.add_argument("--count", type=int, default=Config.DEFAULT_TRANSACTION_COUNT, help="Number of transactions to fetch")
    parser.add_argument("--csv", type=str, default=Config.DEFAULT_CSV_FILENAME, help="Output CSV file")
    args = parser.parse_args()

    logging.info(f"Fetching data for address: {args.address}")

    balance = get_eth_balance(args.address, args.apikey)
    logging.info(f"ETH Balance: {balance:.4f} ETH" if balance is not None else "Failed to fetch balance.")

    transactions = get_last_transactions(args.address, args.apikey, args.count)
    logging.info(f"Fetched {len(transactions)} transactions." if transactions else "No transactions found.")

    eth_price = get_eth_price(args.apikey)
    logging.info(f"Current ETH Price: ${eth_price:.2f}" if eth_price is not None else "Failed to fetch ETH price.")

    if transactions:
        total_incoming, total_outgoing = calculate_transaction_totals(transactions, args.address)
        logging.info(f"Total Incoming: {total_incoming:.4f} ETH, Total Outgoing: {total_outgoing:.4f} ETH")
        save_transactions_to_csv(transactions, args.csv)

if __name__ == "__main__":
    main()
