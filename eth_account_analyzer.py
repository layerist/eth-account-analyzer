#!/usr/bin/env python3
"""
Etherscan Transaction Analyzer
------------------------------
Fetches Ethereum balance, price, and recent transactions for a given address.
Outputs a CSV or JSON file with detailed transaction data.
"""

import argparse
import csv
import json
import logging
import os
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter, Retry
from tabulate import tabulate


@dataclass(frozen=True)
class Config:
    BASE_URL: str = "https://api.etherscan.io/api"
    WEI_TO_ETH: int = 10 ** 18
    DEFAULT_TX_COUNT: int = 10
    TIMEOUT: int = 10
    RETRIES: int = 3
    CSV_DEFAULT: str = "transactions.csv"


# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S"
)


# ----------------------------- Network Layer ----------------------------- #

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
    """Perform GET request with retries and error handling."""
    try:
        response = session.get(Config.BASE_URL, params=params, timeout=Config.TIMEOUT)
        response.raise_for_status()
        data = response.json()
        if data.get("status") != "1" or "result" not in data:
            msg = data.get("message", "Unknown error")
            logging.error(f"Etherscan API error: {msg}")
            return None
        return data
    except (requests.RequestException, ValueError) as e:
        logging.error(f"Request failed: {e} | Params: {params}")
        return None


# ----------------------------- Utility Functions ----------------------------- #

def wei_to_eth(value: str) -> float:
    """Convert Wei (string) to ETH."""
    try:
        return int(value) / Config.WEI_TO_ETH
    except (ValueError, TypeError):
        return 0.0


def eth_to_usd(eth_value: float, price: Optional[float]) -> float:
    """Convert ETH to USD safely."""
    return round(eth_value * price, 2) if (price and eth_value) else 0.0


# ----------------------------- API Functions ----------------------------- #

def get_eth_balance(session: requests.Session, address: str, api_key: str) -> Optional[float]:
    params = {"module": "account", "action": "balance", "address": address, "tag": "latest", "apikey": api_key}
    data = make_request(session, params)
    return wei_to_eth(data["result"]) if data else None


def get_eth_price(session: requests.Session, api_key: str) -> Optional[float]:
    params = {"module": "stats", "action": "ethprice", "apikey": api_key}
    data = make_request(session, params)
    try:
        return float(data["result"]["ethusd"]) if data else None
    except (KeyError, ValueError, TypeError):
        logging.warning("Failed to parse ETH price.")
        return None


def get_recent_transactions(session: requests.Session, address: str, api_key: str, limit: int) -> List[Dict[str, Any]]:
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


# ----------------------------- Processing ----------------------------- #

def summarize_transactions(transactions: List[Dict[str, Any]], address: str) -> Tuple[float, float]:
    """Summarize total in/out for the address."""
    address = address.lower()
    total_in = sum(wei_to_eth(tx["value"]) for tx in transactions if tx.get("to", "").lower() == address)
    total_out = sum(wei_to_eth(tx["value"]) for tx in transactions if tx.get("from", "").lower() == address)
    return total_in, total_out


# ----------------------------- Output ----------------------------- #

def save_csv(transactions: List[Dict[str, Any]], filename: str, eth_price: Optional[float]) -> None:
    """Save transactions as CSV."""
    if not transactions:
        logging.warning("No transactions to save.")
        return

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_file = Path(filename).with_stem(Path(filename).stem + f"_{timestamp}")

    fieldnames = ["hash", "blockNumber", "timeStamp", "from", "to", "value (ETH)", "value (USD)", "gas", "gasPrice (Gwei)"]

    with output_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for tx in sorted(transactions, key=lambda t: int(t["timeStamp"]), reverse=True):
            value_eth = wei_to_eth(tx["value"])
            writer.writerow({
                "hash": tx["hash"],
                "blockNumber": tx["blockNumber"],
                "timeStamp": datetime.utcfromtimestamp(int(tx["timeStamp"])).isoformat(),
                "from": tx["from"],
                "to": tx["to"],
                "value (ETH)": round(value_eth, 6),
                "value (USD)": eth_to_usd(value_eth, eth_price),
                "gas": tx["gas"],
                "gasPrice (Gwei)": round(int(tx["gasPrice"]) / 1e9, 2)
            })

    logging.info(f"✅ Saved {len(transactions)} transactions to: {output_file.resolve()}")


def save_json(transactions: List[Dict[str, Any]], filename: str) -> None:
    """Save transactions as JSON."""
    output_file = Path(filename).with_suffix(".json")
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(transactions, f, indent=2)
    logging.info(f"✅ Saved JSON output to: {output_file.resolve()}")


def print_summary(address: str, balance: Optional[float], price: Optional[float],
                  total_in: float, total_out: float, transactions: List[Dict[str, Any]]) -> None:
    """Print human-readable summary."""
    print("\n📊 Ethereum Address Summary")
    print("=" * 50)
    print(f"Address: {address}")
    print(f"Balance: {balance or 0:.6f} ETH")
    print(f"ETH Price: ${price or 0:.2f}")
    print(f"Total Received: {total_in:.6f} ETH | Total Sent: {total_out:.6f} ETH")
    print(f"Transactions Retrieved: {len(transactions)}\n")

    if transactions:
        table = [
            [
                tx["hash"][:10] + "...",
                tx["from"][:10] + "...",
                tx["to"][:10] + "...",
                round(wei_to_eth(tx["value"]), 6),
                datetime.utcfromtimestamp(int(tx["timeStamp"])).strftime("%Y-%m-%d %H:%M:%S")
            ]
            for tx in transactions[:10]
        ]
        print(tabulate(table, headers=["Hash", "From", "To", "Value (ETH)", "Time"], tablefmt="fancy_grid"))


# ----------------------------- CLI ----------------------------- #

def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze Ethereum transactions via Etherscan API.")
    parser.add_argument("address", help="Ethereum address")
    parser.add_argument("apikey", nargs="?", default=os.getenv("ETHERSCAN_API_KEY"), help="Etherscan API key")
    parser.add_argument("--count", type=int, default=Config.DEFAULT_TX_COUNT, help="Number of recent transactions")
    parser.add_argument("--csv", default=Config.CSV_DEFAULT, help="CSV output filename")
    parser.add_argument("--json", action="store_true", help="Also export JSON")
    args = parser.parse_args()

    if not args.apikey:
        parser.error("Missing Etherscan API key (use arg or ENV ETHERSCAN_API_KEY).")

    session = create_session()
    logging.info(f"🔍 Fetching data for {args.address}")

    # Fetch all concurrently
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            "balance": executor.submit(get_eth_balance, session, args.address, args.apikey),
            "price": executor.submit(get_eth_price, session, args.apikey),
            "tx": executor.submit(get_recent_transactions, session, args.address, args.apikey, args.count)
        }
        results = {k: f.result() for k, f in futures.items()}

    balance, price, transactions = results["balance"], results["price"], results["tx"]

    total_in, total_out = summarize_transactions(transactions, args.address)
    print_summary(args.address, balance, price, total_in, total_out, transactions)

    if transactions:
        save_csv(transactions, args.csv, price)
        if args.json:
            save_json(transactions, args.csv)


if __name__ == "__main__":
    main()
