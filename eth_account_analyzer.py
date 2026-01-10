#!/usr/bin/env python3
"""
Etherscan Transaction Analyzer
------------------------------
- Robust API handling with retries
- Safe caching and serialization
- Clear separation of concerns
- Strong typing and defensive parsing
- Parallelized network calls
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
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
    WEI_PER_ETH: int = 10**18
    DEFAULT_TX_COUNT: int = 10
    TIMEOUT_SECONDS: int = 10
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


def api_call(session: requests.Session, **params: Any) -> Any:
    try:
        response = session.get(
            Config.BASE_URL,
            params=params,
            timeout=Config.TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()

        if payload.get("status") != "1":
            message = payload.get("message", "Unknown API error")
            raise RuntimeError(message)

        return payload.get("result")

    except Exception as exc:
        raise RuntimeError(
            f"Etherscan request failed | params={params}"
        ) from exc


# --------------------------------------------------------------------------- #
#                               DATA UTILITIES                                #
# --------------------------------------------------------------------------- #

def wei_to_eth(value: str | int) -> float:
    try:
        return int(value) / Config.WEI_PER_ETH
    except (TypeError, ValueError):
        return 0.0


def eth_to_usd(eth: float, price: Optional[float]) -> float:
    if price is None:
        return 0.0
    return round(eth * price, 2)


# --------------------------------------------------------------------------- #
#                               API FUNCTIONS                                 #
# --------------------------------------------------------------------------- #

def get_eth_balance(
    session: requests.Session,
    address: str,
    api_key: str,
) -> float:
    result = api_call(
        session,
        module="account",
        action="balance",
        address=address,
        tag="latest",
        apikey=api_key,
    )
    return wei_to_eth(result)


def get_eth_price(
    session: requests.Session,
    api_key: str,
) -> Optional[float]:
    result = api_call(
        session,
        module="stats",
        action="ethprice",
        apikey=api_key,
    )
    try:
        return float(result["ethusd"])
    except (KeyError, TypeError, ValueError):
        return None


def get_recent_transactions(
    session: requests.Session,
    address: str,
    api_key: str,
    limit: int,
) -> List[Dict[str, Any]]:
    Config.CACHE_DIR.mkdir(exist_ok=True)
    cache_file = Config.CACHE_DIR / f"{address.lower()}_tx.json"

    if cache_file.exists():
        try:
            with cache_file.open("r", encoding="utf-8") as fh:
                cached = json.load(fh)
                if isinstance(cached, list):
                    return cached[:limit]
        except Exception:
            logging.warning("Ignoring corrupted cache: %s", cache_file)

    result = api_call(
        session,
        module="account",
        action="txlist",
        address=address,
        startblock=0,
        endblock=99999999,
        sort="desc",
        apikey=api_key,
    )

    try:
        with cache_file.open("w", encoding="utf-8") as fh:
            json.dump(result, fh)
    except Exception:
        logging.warning("Failed to write cache: %s", cache_file)

    return result[:limit]


# --------------------------------------------------------------------------- #
#                                PROCESSING                                   #
# --------------------------------------------------------------------------- #

def summarize_transactions(
    transactions: List[Dict[str, Any]],
    address: str,
) -> Tuple[float, float]:
    addr = address.lower()

    total_received = sum(
        wei_to_eth(tx.get("value", 0))
        for tx in transactions
        if tx.get("to", "").lower() == addr
    )

    total_sent = sum(
        wei_to_eth(tx.get("value", 0))
        for tx in transactions
        if tx.get("from", "").lower() == addr
    )

    return total_received, total_sent


# --------------------------------------------------------------------------- #
#                              PARALLEL EXECUTION                              #
# --------------------------------------------------------------------------- #

def fetch_all(tasks: Dict[str, Callable[[], Any]]) -> Dict[str, Any]:
    results: Dict[str, Any] = {}

    with ThreadPoolExecutor(max_workers=Config.MAX_THREADS) as executor:
        future_map = {
            executor.submit(func): name
            for name, func in tasks.items()
        }

        for future in as_completed(future_map):
            name = future_map[future]
            try:
                results[name] = future.result()
            except Exception as exc:
                logging.error("Task '%s' failed: %s", name, exc)
                results[name] = None

    return results


# --------------------------------------------------------------------------- #
#                                   OUTPUT                                    #
# --------------------------------------------------------------------------- #

def timestamped_path(path: Path) -> Path:
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return path.with_name(f"{path.stem}_{ts}{path.suffix}")


def save_csv(
    transactions: List[Dict[str, Any]],
    filename: str,
    eth_price: Optional[float],
) -> None:
    if not transactions:
        logging.warning("No transactions to save.")
        return

    output_path = timestamped_path(Path(filename))

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

    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()

        for tx in sorted(
            transactions,
            key=lambda t: int(t.get("timeStamp", 0)),
            reverse=True,
        ):
            eth_value = wei_to_eth(tx.get("value", 0))

            writer.writerow({
                "hash": tx.get("hash"),
                "blockNumber": tx.get("blockNumber"),
                "timeStamp": datetime.utcfromtimestamp(
                    int(tx.get("timeStamp", 0))
                ).isoformat(),
                "from": tx.get("from"),
                "to": tx.get("to"),
                "value_eth": eth_value,
                "value_usd": eth_to_usd(eth_value, eth_price),
                "gas": tx.get("gas"),
                "gas_price_gwei": round(
                    int(tx.get("gasPrice", 0)) / 1e9, 2
                ),
            })

    logging.info("CSV saved → %s", output_path)


def save_json(
    transactions: List[Dict[str, Any]],
    filename: str,
) -> None:
    output_path = timestamped_path(Path(filename)).with_suffix(".json")
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(transactions, fh, indent=2)
    logging.info("JSON saved → %s", output_path)


def print_summary(
    address: str,
    balance: Optional[float],
    price: Optional[float],
    total_in: float,
    total_out: float,
    transactions: List[Dict[str, Any]],
) -> None:
    print("\nEthereum Address Summary")
    print("=" * 60)
    print(f"Address:        {address}")
    print(f"Balance:        {balance or 0:.6f} ETH")
    print(f"ETH Price:      ${price or 0:.2f}")
    print(f"Total Received: {total_in:.6f} ETH")
    print(f"Total Sent:     {total_out:.6f} ETH")
    print(f"Transactions:   {len(transactions)}\n")

    if not transactions:
        return

    table = []
    for tx in transactions[:10]:
        table.append([
            tx.get("hash", "")[:10] + "...",
            tx.get("from", "")[:10] + "...",
            tx.get("to", "")[:10] + "...",
            round(wei_to_eth(tx.get("value", 0)), 6),
            datetime.utcfromtimestamp(
                int(tx.get("timeStamp", 0))
            ).strftime("%Y-%m-%d %H:%M:%S"),
        ])

    print(tabulate(
        table,
        headers=("Hash", "From", "To", "ETH", "Time"),
        tablefmt="fancy_grid",
    ))


# --------------------------------------------------------------------------- #
#                                    CLI                                      #
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
        help="Etherscan API key (or ENV ETHERSCAN_API_KEY)",
    )
    parser.add_argument("--count", type=int, default=Config.DEFAULT_TX_COUNT)
    parser.add_argument("--csv", default=Config.CSV_DEFAULT)
    parser.add_argument("--json", action="store_true")

    args = parser.parse_args()

    if not args.apikey:
        parser.error("Missing Etherscan API key")

    session = create_session()
    logging.info("Fetching data for %s", args.address)

    tasks = {
        "balance": lambda: get_eth_balance(session, args.address, args.apikey),
        "price": lambda: get_eth_price(session, args.apikey),
        "transactions": lambda: get_recent_transactions(
            session, args.address, args.apikey, args.count
        ),
    }

    results = fetch_all(tasks)

    balance = results.get("balance")
    price = results.get("price")
    transactions = results.get("transactions") or []

    total_in, total_out = summarize_transactions(
        transactions,
        args.address,
    )

    print_summary(
        args.address,
        balance,
        price,
        total_in,
        total_out,
        transactions,
    )

    if transactions:
        save_csv(transactions, args.csv, price)
        if args.json:
            save_json(transactions, args.csv)


if __name__ == "__main__":
    main()
