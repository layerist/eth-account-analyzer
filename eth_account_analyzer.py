#!/usr/bin/env python3
"""
Improved Etherscan Transaction Analyzer
---------------------------------------
- Robust API handling and validation
- Fixed bugs (typos, error cases)
- Clear separation of concerns
- Safer caching and serialization
- Better typing and readability
"""

from __future__ import annotations

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
#                                   CONFIG                                    #
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class Config:
    BASE_URL: str = "https://api.etherscan.io/api"
    WEI_TO_ETH: int = 10**18
    DEFAULT_TX_COUNT: int = 10
    TIMEOUT: int = 10
    RETRIES: int = 3
    CSV_DEFAULT: str = "transactions.csv"
    MAX_THREADS: int = min(8, (os.cpu_count() or 2) * 2)
    CACHE_DIR: Path = Path(".cache_etherscan")


# --------------------------------------------------------------------------- #
#                                   LOGGING                                   #
# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)


# --------------------------------------------------------------------------- #
#                              NETWORK HELPERS                                #
# --------------------------------------------------------------------------- #

def create_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=Config.RETRIES,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def api_call(session: requests.Session, **params) -> Any:
    try:
        r = session.get(Config.BASE_URL, params=params, timeout=Config.TIMEOUT)
        r.raise_for_status()
        payload = r.json()

        if payload.get("status") != "1":
            raise RuntimeError(payload.get("message", "Unknown API error"))

        return payload["result"]

    except Exception as e:
        raise RuntimeError(f"Etherscan request failed | params={params}") from e


# --------------------------------------------------------------------------- #
#                               DATA UTILITIES                                 #
# --------------------------------------------------------------------------- #

def wei_to_eth(value: str | int) -> float:
    try:
        return int(value) / Config.WEI_TO_ETH
    except Exception:
        return 0.0


def eth_to_usd(eth: float, price: Optional[float]) -> float:
    return round(eth * price, 2) if price else 0.0


# --------------------------------------------------------------------------- #
#                               API FUNCTIONS                                  #
# --------------------------------------------------------------------------- #

def get_eth_balance(session: requests.Session, address: str, key: str) -> float:
    result = api_call(
        session,
        module="account",
        action="balance",
        address=address,
        tag="latest",
        apikey=key,
    )
    return wei_to_eth(result)


def get_eth_price(session: requests.Session, key: str) -> Optional[float]:
    result = api_call(session, module="stats", action="ethprice", apikey=key)
    try:
        return float(result["ethusd"])
    except Exception:
        return None


def get_recent_transactions(
    session: requests.Session,
    address: str,
    key: str,
    limit: int,
) -> List[Dict[str, Any]]:
    Config.CACHE_DIR.mkdir(exist_ok=True)
    cache_file = Config.CACHE_DIR / f"{address.lower()}_tx.json"

    if cache_file.exists():
        try:
            with cache_file.open("r", encoding="utf-8") as f:
                cached = json.load(f)
                if isinstance(cached, list):
                    return cached[:limit]
        except Exception:
            pass

    result = api_call(
        session,
        module="account",
        action="txlist",
        address=address,
        startblock=0,
        endblock=99999999,
        sort="desc",
        apikey=key,
    )

    with cache_file.open("w", encoding="utf-8") as f:
        json.dump(result, f)

    return result[:limit]


# --------------------------------------------------------------------------- #
#                                PROCESSING                                    #
# --------------------------------------------------------------------------- #

def summarize_transactions(
    tx: List[Dict[str, Any]],
    address: str,
) -> tuple[float, float]:
    addr = address.lower()
    total_in = sum(
        wei_to_eth(t["value"]) for t in tx if t.get("to", "").lower() == addr
    )
    total_out = sum(
        wei_to_eth(t["value"]) for t in tx if t.get("from", "").lower() == addr
    )
    return total_in, total_out


# --------------------------------------------------------------------------- #
#                              PARALLEL EXECUTION                              #
# --------------------------------------------------------------------------- #

def fetch_all(tasks: Dict[str, Callable[[], Any]]) -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=Config.MAX_THREADS) as executor:
        future_map = {
            executor.submit(func): name for name, func in tasks.items()
        }
        for future in as_completed(future_map):
            name = future_map[future]
            try:
                results[name] = future.result()
            except Exception as e:
                logging.error("Task '%s' failed: %s", name, e)
                results[name] = None
    return results


# --------------------------------------------------------------------------- #
#                                   OUTPUT                                     #
# --------------------------------------------------------------------------- #

def timestamp_path(path: Path) -> Path:
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return path.with_name(f"{path.stem}_{ts}{path.suffix}")


def save_csv(tx: List[Dict[str, Any]], filename: str, price: Optional[float]) -> None:
    if not tx:
        logging.warning("No transactions to save.")
        return

    output = timestamp_path(Path(filename))
    fields = (
        "hash",
        "blockNumber",
        "timeStamp",
        "from",
        "to",
        "value_eth",
        "value_usd",
        "gas",
        "gas_price_gwei",
    )

    with output.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()

        for t in sorted(tx, key=lambda x: int(x["timeStamp"]), reverse=True):
            eth = wei_to_eth(t["value"])
            writer.writerow(
                {
                    "hash": t["hash"],
                    "blockNumber": t["blockNumber"],
                    "timeStamp": datetime.utcfromtimestamp(
                        int(t["timeStamp"])
                    ).isoformat(),
                    "from": t["from"],
                    "to": t["to"],
                    "value_eth": eth,
                    "value_usd": eth_to_usd(eth, price),
                    "gas": t["gas"],
                    "gas_price_gwei": round(int(t["gasPrice"]) / 1e9, 2),
                }
            )

    logging.info("Saved CSV → %s", output)


def save_json(tx: List[Dict[str, Any]], filename: str) -> None:
    output = timestamp_path(Path(filename)).with_suffix(".json")
    with output.open("w", encoding="utf-8") as f:
        json.dump(tx, f, indent=2)
    logging.info("Saved JSON → %s", output)


def print_summary(
    address: str,
    balance: Optional[float],
    price: Optional[float],
    total_in: float,
    total_out: float,
    tx: List[Dict[str, Any]],
) -> None:
    print("\nEthereum Address Summary")
    print("=" * 60)
    print(f"Address:        {address}")
    print(f"Balance:        {balance or 0:.6f} ETH")
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
                datetime.utcfromtimestamp(int(t["timeStamp"])).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
            ]
            for t in tx[:10]
        ]
        print(
            tabulate(
                table,
                headers=("Hash", "From", "To", "ETH", "Time"),
                tablefmt="fancy_grid",
            )
        )


# --------------------------------------------------------------------------- #
#                                    CLI                                       #
# --------------------------------------------------------------------------- #

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ethereum transaction analyzer using Etherscan API"
    )
    parser.add_argument("address", help="Ethereum address")
    parser.add_argument(
        "apikey",
        nargs="?",
        default=os.getenv("ETHERSCAN_API_KEY"),
        help="Etherscan API key",
    )
    parser.add_argument("--count", type=int, default=Config.DEFAULT_TX_COUNT)
    parser.add_argument("--csv", default=Config.CSV_DEFAULT)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    if not args.apikey:
        parser.error("Missing Etherscan API key (argument or ENV ETHERSCAN_API_KEY)")

    session = create_session()
    logging.info("Fetching data for %s", args.address)

    tasks = {
        "balance": lambda: get_eth_balance(session, args.address, args.apikey),
        "price": lambda: get_eth_price(session, args.apikey),
        "tx": lambda: get_recent_transactions(
            session, args.address, args.apikey, args.count
        ),
    }

    results = fetch_all(tasks)

    balance = results.get("balance")
    price = results.get("price")
    tx = results.get("tx") or []

    total_in, total_out = summarize_transactions(tx, args.address)
    print_summary(args.address, balance, price, total_in, total_out, tx)

    if tx:
        save_csv(tx, args.csv, price)
        if args.json:
            save_json(tx, args.csv)


if __name__ == "__main__":
    main()
