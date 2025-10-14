#!/usr/bin/env python3
"""
Etherscan Transaction Analyzer
------------------------------
Fetches Ethereum balance, price, and latest transactions for a given address.
Outputs a CSV file with detailed transaction data.
"""

import argparse
import csv
import logging
import os
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple, Optional, Dict, Any

import requests
from requests.adapters import HTTPAdapter, Retry


@dataclass(frozen=True)
class Config:
    """Static configuration constants."""
    BASE_URL: str = "https://api.etherscan.io/api"
    WEI_TO_ETH: int = 10 ** 18
    DEFAULT_CSV_FILENAME: str = "transactions.csv"
    DEFAULT_TRANSACTION_COUNT: int = 10
    TIMEOUT: int = 10
    RETRIES: int = 3


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


def create_session() -> requests.Session:
    """Create a requests session with retry logic."""
    session = requests.Session()
    retries = Retry(
        total=Config.RETRIES,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def make_request(session: requests.Session, params: Dict[str, str]) -> Optional[Dict[str, Any]]:
    """Perform an HTTP GET request to the Etherscan API."""
    try:
        response = session.get(Config.BASE_URL, params=params, timeout=Config.TIMEOUT)
        response.raise_for_status()
        data = response.json()

        if data.get("status") != "1" or "result" not in data:
            logging.error(f"Etherscan API returned an error: {data.get('message', 'Unknown')}")
            return None
        return data
    except (requests.RequestException, ValueError) as e:
        logging.error(f"Request failed: {e} | Params: {params}")
        return None


def wei_to_eth(value: str) -> Optional[float]:
    """Convert Wei (string) to ETH (float)."""
    try:
        return int(value) / Config.WEI_TO_ETH
    except (ValueError, TypeError):
        return None


def get_eth_balance(session: requests.Session, address: str, api_key: str) -> Optional[float]:
    """Retrieve the ETH balance for the specified address."""
    params = {
        "module": "account",
        "action": "balance",
        "address": address,
        "tag": "latest",
        "apikey": api_key,
    }
    data = make_request(session, params)
    return wei_to_eth(data["result"]) if data else None


def get_eth_price(session: requests.Session, api_key: str) -> Optional[float]:
    """Retrieve the current ETH price in USD."""
    params = {
        "module": "stats",
        "action": "ethprice",
        "apikey": api_key,
    }
    data = make_request(session, params)
    if not data:
        return None
    try:
        return float(data["result"]["ethusd"])
    except (KeyError, ValueError):
        logging.error("Failed to parse ETH price.")
        return None


def get_recent_transactions(session: requests.Session, address: str, api_key: str, limit: int) -> List[Dict[str, Any]]:
    """Fetch the most recent transactions for the given Ethereum address."""
    params = {
        "module": "account",
        "action": "txlist",
        "address": address,
        "startblock": "0",
        "endblock": "99999999",
        "sort": "desc",
        "apikey": api_key,
    }
    data = make_request(session, params)
    return data["result"][:limit] if data else []


def summarize_transactions(transactions: List[Dict[str, Any]], address: str) -> Tuple[float, float]:
    """Calculate the total ETH received and sent for an address."""
    address = address.lower()
    total_in, total_out = 0.0, 0.0

    for tx in transactions:
        value = wei_to_eth(tx.get("value", "0"))
        if value is None:
            continue

        if tx.get("to", "").lower() == address:
            total_in += value
        elif tx.get("from", "").lower() == address:
            total_out += value

    return total_in, total_out


def save_transactions_to_csv(transactions: List[Dict[str, Any]], filename: str, eth_price: Optional[float]) -> None:
    """Save transaction data to a CSV file."""
    if not transactions:
        logging.warning("No transactions to write to CSV.")
        return

    output_path = Path(filename)
    fieldnames = [
        "hash", "blockNumber", "timeStamp", "from", "to",
        "value (ETH)", "value (USD)", "gas", "gasPrice (Gwei)"
    ]

    try:
        with output_path.open("w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for tx in sorted(transactions, key=lambda t: int(t["timeStamp"]), reverse=True):
                value_eth = wei_to_eth(tx.get("value", "0")) or 0.0
                value_usd = round(value_eth * eth_price, 2) if eth_price else ""

                writer.writerow({
                    "hash": tx.get("hash", ""),
                    "blockNumber": tx.get("blockNumber", ""),
                    "timeStamp": datetime.utcfromtimestamp(int(tx.get("timeStamp", 0))).isoformat(),
                    "from": tx.get("from", ""),
                    "to": tx.get("to", ""),
                    "value (ETH)": round(value_eth, 6),
                    "value (USD)": value_usd,
                    "gas": tx.get("gas", ""),
                    "gasPrice (Gwei)": round(int(tx.get("gasPrice", 0)) / 1e9, 2),
                })

        logging.info(f"✅ Saved {len(transactions)} transactions to '{output_path.resolve()}'")
    except Exception as e:
        logging.error(f"Failed to write CSV file '{filename}': {e}")


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Fetch ETH balance, price, and transactions for an Ethereum address."
    )
    parser.add_argument("address", help="Ethereum address to analyze")
    parser.add_argument(
        "apikey",
        nargs="?",
        default=os.getenv("ETHERSCAN_API_KEY"),
        help="Etherscan API key (or set ETHERSCAN_API_KEY environment variable)"
    )
    parser.add_argument(
        "--count", type=int, default=Config.DEFAULT_TRANSACTION_COUNT,
        help=f"Number of transactions to fetch (default: {Config.DEFAULT_TRANSACTION_COUNT})"
    )
    parser.add_argument(
        "--csv", default=Config.DEFAULT_CSV_FILENAME,
        help=f"CSV output filename (default: {Config.DEFAULT_CSV_FILENAME})"
    )

    args = parser.parse_args()

    if not args.apikey:
        parser.error("Missing Etherscan API key. Use argument or environment variable ETHERSCAN_API_KEY.")

    session = create_session()
    logging.info(f"Analyzing Ethereum address: {args.address}")

    balance = get_eth_balance(session, args.address, args.apikey)
    eth_price = get_eth_price(session, args.apikey)
    transactions = get_recent_transactions(session, args.address, args.apikey, args.count)

    if balance is not None:
        logging.info(f"💰 Balance: {balance:.6f} ETH")
    else:
        logging.warning("Could not fetch balance.")

    if eth_price is not None:
        logging.info(f"📈 Current ETH Price: ${eth_price:.2f}")
    else:
        logging.warning("Could not fetch ETH price.")

    logging.info(f"📜 Retrieved {len(transactions)} transactions.")

    if transactions:
        total_in, total_out = summarize_transactions(transactions, args.address)
        logging.info(f"🔹 Total Received: {total_in:.6f} ETH | 🔸 Total Sent: {total_out:.6f} ETH")
        save_transactions_to_csv(transactions, args.csv, eth_price)


if __name__ == "__main__":
    main()
