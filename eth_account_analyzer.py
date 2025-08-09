import argparse
import csv
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple, Optional, Dict, Any

import requests
from requests.adapters import HTTPAdapter, Retry


@dataclass(frozen=True)
class Config:
    """Application configuration constants."""
    BASE_URL: str = "https://api.etherscan.io/api"
    WEI_TO_ETH: int = 10 ** 18
    DEFAULT_CSV_FILENAME: str = "transactions.csv"
    DEFAULT_TRANSACTION_COUNT: int = 10
    TIMEOUT: int = 10
    RETRIES: int = 3


# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_session() -> requests.Session:
    """Create and configure a requests Session with retry logic."""
    session = requests.Session()
    retry_strategy = Retry(
        total=Config.RETRIES,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        backoff_factor=1
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    return session


session = get_session()


def make_request(params: Dict[str, str]) -> Optional[Dict[str, Any]]:
    """Make an HTTP GET request to the Etherscan API."""
    try:
        response = session.get(Config.BASE_URL, params=params, timeout=Config.TIMEOUT)
        response.raise_for_status()
        data = response.json()

        if data.get("status") != "1" or "result" not in data:
            logging.error(f"API error: {data.get('message', 'Unknown')} | Params: {params}")
            return None
        return data
    except (requests.RequestException, ValueError) as e:
        logging.error(f"Request failed: {e} | Params: {params}")
        return None


def get_eth_balance(address: str, api_key: str) -> Optional[float]:
    """Retrieve ETH balance for a given address."""
    params = {
        "module": "account",
        "action": "balance",
        "address": address,
        "tag": "latest",
        "apikey": api_key
    }
    data = make_request(params)
    try:
        return int(data["result"]) / Config.WEI_TO_ETH if data else None
    except (ValueError, KeyError, TypeError):
        logging.exception("Failed to parse ETH balance.")
        return None


def get_eth_price(api_key: str) -> Optional[float]:
    """Retrieve current ETH price in USD."""
    params = {
        "module": "stats",
        "action": "ethprice",
        "apikey": api_key
    }
    data = make_request(params)
    try:
        return float(data["result"]["ethusd"]) if data else None
    except (ValueError, KeyError, TypeError):
        logging.exception("Failed to parse ETH price.")
        return None


def get_last_transactions(address: str, api_key: str, count: int) -> List[Dict[str, Any]]:
    """Retrieve recent transactions for a given Ethereum address."""
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


def calculate_transaction_totals(transactions: List[Dict[str, Any]], address: str) -> Tuple[float, float]:
    """Calculate total ETH received and sent by the address."""
    total_in, total_out = 0.0, 0.0
    address = address.lower()

    for tx in transactions:
        try:
            value_eth = int(tx.get("value", 0)) / Config.WEI_TO_ETH
            from_addr = tx.get("from", "").lower()
            to_addr = tx.get("to", "").lower()

            if to_addr == address:
                total_in += value_eth
            elif from_addr == address:
                total_out += value_eth
        except (ValueError, TypeError):
            logging.warning(f"Skipping malformed transaction: {tx}")

    return total_in, total_out


def save_transactions_to_csv(transactions: List[Dict[str, Any]], filename: str, eth_price: Optional[float]) -> None:
    """Save transactions to a CSV file."""
    if not transactions:
        logging.warning("No transactions to write.")
        return

    fieldnames = ["hash", "blockNumber", "timeStamp", "from", "to",
                  "value (ETH)", "value (USD)", "gas", "gasPrice"]

    try:
        transactions.sort(key=lambda tx: int(tx.get("timeStamp", 0)), reverse=True)

        with open(filename, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()

            for tx in transactions:
                try:
                    value_eth = int(tx.get("value", 0)) / Config.WEI_TO_ETH
                    writer.writerow({
                        "hash": tx.get("hash", ""),
                        "blockNumber": tx.get("blockNumber", ""),
                        "timeStamp": datetime.utcfromtimestamp(int(tx.get("timeStamp", 0))).isoformat(),
                        "from": tx.get("from", ""),
                        "to": tx.get("to", ""),
                        "value (ETH)": round(value_eth, 6),
                        "value (USD)": round(value_eth * eth_price, 2) if eth_price else "",
                        "gas": tx.get("gas", ""),
                        "gasPrice": tx.get("gasPrice", "")
                    })
                except (ValueError, KeyError, TypeError) as e:
                    logging.warning(f"Skipping invalid transaction row: {e}")

        logging.info(f"Saved {len(transactions)} transactions to '{filename}'")
    except IOError as e:
        logging.error(f"Error writing to file '{filename}': {e}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch ETH balance and recent transactions for an address."
    )
    parser.add_argument("address", help="Ethereum address")
    parser.add_argument(
        "apikey",
        nargs="?",
        default=os.getenv("ETHERSCAN_API_KEY"),
        help="Etherscan API key (or set ETHERSCAN_API_KEY env variable)"
    )
    parser.add_argument("--count", type=int, default=Config.DEFAULT_TRANSACTION_COUNT,
                        help=f"Number of transactions to fetch (default: {Config.DEFAULT_TRANSACTION_COUNT})")
    parser.add_argument("--csv", default=Config.DEFAULT_CSV_FILENAME,
                        help=f"CSV output filename (default: {Config.DEFAULT_CSV_FILENAME})")

    args = parser.parse_args()

    if not args.apikey:
        parser.error("Etherscan API key is required. Pass as argument or set ETHERSCAN_API_KEY env variable.")

    logging.info(f"Analyzing address: {args.address}")

    balance = get_eth_balance(args.address, args.apikey)
    if balance is not None:
        logging.info(f"Balance: {balance:.6f} ETH")
    else:
        logging.error("Failed to retrieve ETH balance.")

    eth_price = get_eth_price(args.apikey)
    if eth_price is not None:
        logging.info(f"Current ETH Price: ${eth_price:.2f}")
    else:
        logging.warning("ETH price unavailable.")

    transactions = get_last_transactions(args.address, args.apikey, args.count)
    logging.info(f"Fetched {len(transactions)} transactions.")

    if transactions:
        total_in, total_out = calculate_transaction_totals(transactions, args.address)
        logging.info(f"Total Received: {total_in:.6f} ETH | Total Sent: {total_out:.6f} ETH")
        save_transactions_to_csv(transactions, args.csv, eth_price)


if __name__ == "__main__":
    main()
