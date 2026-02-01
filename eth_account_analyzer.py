#!/usr/bin/env python3
"""
Etherscan Transaction Analyzer
==============================

Improvements over the original version:
- Clearer error semantics with custom exceptions
- Stronger typing and validation helpers
- Centralized cache handling with TTL support
- Better session lifecycle management
- Safer numeric parsing and timestamp handling
- Cleaner parallel task orchestration
- Minor performance, readability, and style improvements
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
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
    MAX_THREADS: int = min(8, (os.cpu_count() or 2) * 2)

    CACHE_DIR: Path = Path(".cache_etherscan")
    CACHE_TTL_SECONDS: int = 300  # 5 minutes

    CSV_DEFAULT: str = "transactions.csv"


# --------------------------------------------------------------------------- #
#                                   LOGGING                                   #
# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)


# --------------------------------------------------------------------------- #
#                                   ERRORS                                    #
# --------------------------------------------------------------------------- #

class EtherscanError(RuntimeError):
    pass


class CacheError(RuntimeError):
    pass


# --------------------------------------------------------------------------- #
#                              NETWORK HELPERS                                #
# --------------------------------------------------------------------------- #

def create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=Config.RETRIES,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
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
    except Exception as exc:
        raise EtherscanError(f"HTTP failure | params={params}") from exc

    if payload.get("status") != "1":
        raise EtherscanError(payload.get("message", "Unknown API error"))

    return payload.get("result")


# --------------------------------------------------------------------------- #
#                               CACHE UTILITIES                               #
# --------------------------------------------------------------------------- #

def cache_path(key: str) -> Path:
    Config.CACHE_DIR.mkdir(exist_ok=True)
    return Config.CACHE_DIR / f"{key}.json"


def load_cache(path: Path) -> Optional[Any]:
    try:
        if not path.exists():
            return None

        age = time.time() - path.stat().st_mtime
        if age > Config.CACHE_TTL_SECONDS:
            return None

        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as exc:
        raise CacheError(f"Failed to load cache: {path}") from exc


def save_cache(path: Path, data: Any) -> None:
    try:
        with path.open("w", encoding="utf-8") as fh:
            json.dump(data, fh)
    except Exception as exc:
        logging.warning("Cache write failed (%s): %s", path, exc)


# --------------------------------------------------------------------------- #
#                               DATA UTILITIES                                #
# --------------------------------------------------------------------------- #

def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def wei_to_eth(value: Any) -> float:
    return safe_int(value) / Config.WEI_PER_ETH


def eth_to_usd(eth: float, price: Optional[float]) -> float:
    return round(eth * price, 2) if price else 0.0


def parse_utc(ts: Any) -> str:
    return datetime.utcfromtimestamp(
        safe_int(ts)
    ).isoformat()


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
    cache = cache_path(f"{address.lower()}_tx")
    cached = load_cache(cache)
    if isinstance(cached, list):
        return cached[:limit]

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

    if isinstance(result, list):
        save_cache(cache, result)
        return result[:limit]

    return []


# --------------------------------------------------------------------------- #
#                                PROCESSING                                   #
# --------------------------------------------------------------------------- #

def summarize_transactions(
    transactions: Iterable[Dict[str, Any]],
    address: str,
) -> Tuple[float, float]:
    addr = address.lower()

    received = 0.0
    sent = 0.0

    for tx in transactions:
        value = wei_to_eth(tx.get("value"))
        if tx.get("to", "").lower() == addr:
            received += value
        elif tx.get("from", "").lower() == addr:
            sent += value

    return received, sent


# --------------------------------------------------------------------------- #
#                              PARALLEL EXECUTION                              #
# --------------------------------------------------------------------------- #

def run_tasks(tasks: Dict[str, Callable[[], Any]]) -> Dict[str, Any]:
    results: Dict[str, Any] = {}

    with ThreadPoolExecutor(max_workers=Config.MAX_THREADS) as pool:
        futures = {pool.submit(fn): name for name, fn in tasks.items()}

        for future in as_completed(futures):
            name = futures[future]
            try:
                results[name] = future.result()
            except Exception as exc:
                logging.error("Task '%s' failed: %s", name, exc)
                results[name] = None

    return results


# --------------------------------------------------------------------------- #
#                                   OUTPUT                                    #
# --------------------------------------------------------------------------- #

def timestamped(path: Path) -> Path:
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return path.with_name(f"{path.stem}_{ts}{path.suffix}")


def save_csv(
    transactions: List[Dict[str, Any]],
    filename: str,
    eth_price: Optional[float],
) -> None:
    if not transactions:
        return

    path = timestamped(Path(filename))
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

    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()

        for tx in transactions:
            eth = wei_to_eth(tx.get("value"))
            writer.writerow({
                "hash": tx.get("hash"),
                "blockNumber": tx.get("blockNumber"),
                "timeStamp": parse_utc(tx.get("timeStamp")),
                "from": tx.get("from"),
                "to": tx.get("to"),
                "value_eth": eth,
                "value_usd": eth_to_usd(eth, eth_price),
                "gas": tx.get("gas"),
                "gas_price_gwei": round(
                    safe_int(tx.get("gasPrice")) / 1e9, 2
                ),
            })

    logging.info("CSV saved → %s", path)


def save_json(transactions: List[Dict[str, Any]], filename: str) -> None:
    path = timestamped(Path(filename)).with_suffix(".json")
    with path.open("w", encoding="utf-8") as fh:
        json.dump(transactions, fh, indent=2)
    logging.info("JSON saved → %s", path)


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

    rows = []
    for tx in transactions[:10]:
        rows.append([
            (tx.get("hash") or "")[:10] + "...",
            (tx.get("from") or "")[:10] + "...",
            (tx.get("to") or "")[:10] + "...",
            round(wei_to_eth(tx.get("value")), 6),
            parse_utc(tx.get("timeStamp")),
        ])

    print(tabulate(
        rows,
        headers=("Hash", "From", "To", "ETH", "Time"),
        tablefmt="fancy_grid",
    ))


# --------------------------------------------------------------------------- #
#                                    CLI                                      #
# --------------------------------------------------------------------------- #

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ethereum transaction analyzer (Etherscan)"
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

    results = run_tasks({
        "balance": lambda: get_eth_balance(session, args.address, args.apikey),
        "price": lambda: get_eth_price(session, args.apikey),
        "txs": lambda: get_recent_transactions(
            session, args.address, args.apikey, args.count
        ),
    })

    transactions = results.get("txs") or []
    total_in, total_out = summarize_transactions(transactions, args.address)

    print_summary(
        args.address,
        results.get("balance"),
        results.get("price"),
        total_in,
        total_out,
        transactions,
    )

    if transactions:
        save_csv(transactions, args.csv, results.get("price"))
        if args.json:
            save_json(transactions, args.csv)


if __name__ == "__main__":
    main()
