#!/usr/bin/env python3
"""
Improved Etherscan Transaction Analyzer
---------------------------------------
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
from typing import List, Dict, Any, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter, Retry
from tabulate import tabulate


# --------------------------------------------------------------------------- #
#                                CONFIG                                       #
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class Config:
    BASE_URL: str = "https://api.etherscan.io/api"
    WEI_TO_ETH: int = 10 ** 18
    DEFAULT_TX_COUNT: int = 10
    TIMEOUT: int = 10
    RETRIES: int = 3
    CSV_DEFAULT: str = "transactions.csv"
    MAX_THREADS: int = min(8, (os.cpu_count() or 2) * 2)
    CACHE_DIR: Path = Path(".cache_etherscan")


# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S"
)


# --------------------------------------------------------------------------- #
#                     NETWORK & API HELPER FUNCTIONS                          #
# --------------------------------------------------------------------------- #

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
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def api_call(session: requests.Session, **params) -> Dict[str, Any]:
    """Generic API call wrapper with error raising."""
    try:
        r = session.get(Config.BASE_URL, params=params, timeout=Config.TIMEOUT)
        r.raise_for_status()
        data = r.json()

        if data.get("status") != "1":
            raise RuntimeError(f"Etherscan error: {data.get('message')} | Params: {params}")

        return data["result"]

    except Exception as e:
        raise RuntimeError(f"Request failed: {e} | Params: {params}") from e


# --------------------------------------------------------------------------- #
#                            DATA PARSING                                     #
# --------------------------------------------------------------------------- #

def wei_to_eth(value: str) -> float:
    """Convert Wei to ETH safely."""
    try:
        return int(value) / Config.WI_TO_ETH
    except Exception:
        return 0.0


def eth_to_usd(eth_value: float, price_usd: Optional[float]) -> float:
    return round(eth_value * price_usd, 2) if price_usd else 0.0


# --------------------------------------------------------------------------- #
#                          API-SPECIFIC CALLS                                 #
# --------------------------------------------------------------------------- #

def get_eth_balance(session: requests.Session, address: str, key: str) -> float:
    result = api_call(session, module="account", action="balance", address=address, tag="latest", apikey=key)
    return wei_to_eth(result)


def get_eth_price(session: requests.Session, key: str) -> Optional[float]:
    result = api_call(session, module="stats", action="ethprice", apikey=key)
    try:
        return float(result["ethusd"])
    except:
        return None


def get_recent_transactions(
    session: requests.Session, address: str, key: str, limit: int
) -> List[Dict[str, Any]]:
    
    # Optional local cache
    Config.CACHE_DIR.mkdir(exist_ok=True)
    cache_file = Config.CACHE_DIR / f"{address.lower()}_tx.json"

    if cache_file.exists():
        try:
            with cache_file.open() as f:
                cached = json.load(f)
                if isinstance(cached, list):
                    return cached[:limit]
        except:
            pass

    result = api_call(
        session,
        module="account",
        action="txlist",
        address=address,
        startblock=0,
        endblock=99999999,
        sort="desc",
        apikey=key
    )

    # Save full result to cache
    with cache_file.open("w") as f:
        json.dump(result, f)

    return result[:limit]


# --------------------------------------------------------------------------- #
#                           PROCESSING                                        #
# --------------------------------------------------------------------------- #

def summarize_transactions(tx: List[Dict[str, Any]], address: str):
    addr = address.lower()
    total_in = sum(wei_to_eth(t["value"]) for t in tx if t.get("to", "").lower() == addr)
    total_out = sum(wei_to_eth(t["value"]) for t in tx if t.get("from", "").lower() == addr)
    return total_in, total_out


# --------------------------------------------------------------------------- #
#                          PARALLEL FETCHING                                  #
# --------------------------------------------------------------------------- #

def fetch_all(tasks: Dict[str, Callable]) -> Dict[str, Any]:
    """Run tasks in parallel and return results as dict."""
    results = {}
    with ThreadPoolExecutor(max_workers=Config.MAX_THREADS) as ex:
        futures = {name: ex.submit(func) for name, func in tasks.items()}
        for name, f in futures.items():
            try:
                results[name] = f.result()
            except Exception as e:
                logging.error(f"Task '{name}' failed: {e}")
                results[name] = None
    return results


# --------------------------------------------------------------------------- #
#                           OUTPUT                                             #
# --------------------------------------------------------------------------- #

def timestamp_path(path: Path) -> Path:
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return path.with_name(f"{path.stem}_{ts}{path.suffix}")


def save_csv(tx, filename, price):
    if not tx:
        logging.warning("No transactions to save.")
        return

    output_file = timestamp_path(Path(filename))
    fields = [
        "hash", "blockNumber", "timeStamp", "from", "to",
        "value (ETH)", "value (USD)", "gas", "gasPrice (Gwei)"
    ]

    with output_file.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()

        for t in sorted(tx, key=lambda x: int(x["timeStamp"]), reverse=True):
            value_eth = wei_to_eth(t["value"])
            w.writerow({
                "hash": t["hash"],
                "blockNumber": t["blockNumber"],
                "timeStamp": datetime.utcfromtimestamp(int(t["timeStamp"])).isoformat(),
                "from": t["from"],
                "to": t["to"],
                "value (ETH)": value_eth,
                "value (USD)": eth_to_usd(value_eth, price),
                "gas": t["gas"],
                "gasPrice (Gwei)": round(int(t["gasPrice"]) / 1e9, 2)
            })

    logging.info(f"Saved CSV → {output_file}")


def save_json(tx, filename):
    out = timestamp_path(Path(filename)).with_suffix(".json")
    with out.open("w") as f:
        json.dump(tx, f, indent=2)
    logging.info(f"Saved JSON → {out}")


def print_summary(addr, bal, price, total_in, total_out, tx):
    print("\n📊 Ethereum Address Summary")
    print("=" * 50)
    print(f"Address:        {addr}")
    print(f"Balance:        {bal or 0:.6f} ETH")
    print(f"ETH Price:      ${price or 0:.2f}")
    print(f"Total Received: {total_in:.6f} ETH")
    print(f"Total Sent:     {total_out:.6f} ETH")
    print(f"Transactions:   {len(tx)}\n")

    if tx:
        table = [
            [
                t["hash"][:10] + "...",
                t["from"][:10] + "...",
                t["to"][:10] + "...",
                round(wei_to_eth(t["value"]), 6),
                datetime.utcfromtimestamp(int(t["timeStamp"])).strftime("%Y-%m-%d %H:%M:%S")
            ]
            for t in tx[:10]
        ]
        print(tabulate(
            table,
            headers=["Hash", "From", "To", "ETH", "Time"],
            tablefmt="fancy_grid"
        ))


# --------------------------------------------------------------------------- #
#                           CLI                                                #
# --------------------------------------------------------------------------- #

def main():
    parser = argparse.ArgumentParser(description="Improved Ethereum Transaction Analyzer via Etherscan API.")
    parser.add_argument("address")
    parser.add_argument("apikey", nargs="?", default=os.getenv("ETHERSCAN_API_KEY"))
    parser.add_argument("--count", type=int, default=Config.DEFAULT_TX_COUNT)
    parser.add_argument("--csv", default=Config.CSV_DEFAULT)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    if not args.apikey:
        parser.error("Missing Etherscan API key (arg or ENV ETHERSCAN_API_KEY).")

    session = create_session()
    logging.info(f"Fetching data for {args.address}")

    tasks = {
        "balance": lambda: get_eth_balance(session, args.address, args.apikey),
        "price":   lambda: get_eth_price(session, args.apikey),
        "tx":      lambda: get_recent_transactions(session, args.address, args.apikey, args.count)
    }

    r = fetch_all(tasks)
    balance, price, tx = r["balance"], r["price"], r["tx"]

    total_in, total_out = summarize_transactions(tx or [], args.address)
    print_summary(args.address, balance, price, total_in, total_out, tx or [])

    if tx:
        save_csv(tx, args.csv, price)
        if args.json:
            save_json(tx, args.csv)


if __name__ == "__main__":
    main()
