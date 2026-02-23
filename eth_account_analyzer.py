#!/usr/bin/env python3
"""
Etherscan Transaction Analyzer (Improved)

Enhancements:
- Deterministic hashed cache keys
- Decimal-based ETH math (no float precision loss)
- Ethereum address validation
- Clean session lifecycle management
- Clearer API error semantics
- Safer JSON handling
- Better CLI UX
- Strong typing consistency
- More resilient parallel execution
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter, Retry
from tabulate import tabulate


# --------------------------------------------------------------------------- #
#                                   CONFIG                                    #
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class Config:
    BASE_URL: str = "https://api.etherscan.io/api"
    WEI_PER_ETH: Decimal = Decimal("1000000000000000000")
    DEFAULT_TX_COUNT: int = 10
    TIMEOUT_SECONDS: int = 10
    RETRIES: int = 3
    MAX_THREADS: int = min(8, (os.cpu_count() or 2) * 2)

    CACHE_DIR: Path = Path(".cache_etherscan")
    CACHE_TTL_SECONDS: int = 300
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


class ValidationError(ValueError):
    pass


# --------------------------------------------------------------------------- #
#                               VALIDATION                                    #
# --------------------------------------------------------------------------- #

def validate_eth_address(address: str) -> str:
    if not isinstance(address, str):
        raise ValidationError("Address must be string")

    if not address.startswith("0x") or len(address) != 42:
        raise ValidationError("Invalid Ethereum address format")

    return address.lower()


# --------------------------------------------------------------------------- #
#                              NETWORK HELPERS                                #
# --------------------------------------------------------------------------- #

def create_session() -> requests.Session:
    session = requests.Session()

    retry = Retry(
        total=Config.RETRIES,
        backoff_factor=0.8,
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
    except requests.RequestException as exc:
        raise EtherscanError(f"Network error: {exc}") from exc

    try:
        payload = response.json()
    except ValueError as exc:
        raise EtherscanError("Invalid JSON response") from exc

    if payload.get("status") != "1":
        raise EtherscanError(payload.get("message", "Unknown API error"))

    return payload.get("result")


# --------------------------------------------------------------------------- #
#                                   CACHE                                     #
# --------------------------------------------------------------------------- #

def _hash_key(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


def cache_path(key: str) -> Path:
    Config.CACHE_DIR.mkdir(exist_ok=True)
    return Config.CACHE_DIR / f"{_hash_key(key)}.json"


def load_cache(path: Path) -> Optional[Any]:
    if not path.exists():
        return None

    age = time.time() - path.stat().st_mtime
    if age > Config.CACHE_TTL_SECONDS:
        return None

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise CacheError(f"Failed reading cache {path}") from exc


def save_cache(path: Path, data: Any) -> None:
    try:
        path.write_text(json.dumps(data), encoding="utf-8")
    except Exception as exc:
        logging.warning("Cache write failed: %s", exc)


# --------------------------------------------------------------------------- #
#                               NUMERIC HELPERS                               #
# --------------------------------------------------------------------------- #

def safe_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError):
        return Decimal(0)


def wei_to_eth(value: Any) -> Decimal:
    return safe_decimal(value) / Config.WEI_PER_ETH


def eth_to_usd(eth: Decimal, price: Optional[Decimal]) -> Decimal:
    if not price:
        return Decimal(0)
    return (eth * price).quantize(Decimal("0.01"))


def parse_utc(ts: Any) -> str:
    try:
        return datetime.fromtimestamp(
            int(ts), tz=timezone.utc
        ).isoformat()
    except Exception:
        return "N/A"


# --------------------------------------------------------------------------- #
#                               API FUNCTIONS                                 #
# --------------------------------------------------------------------------- #

def get_eth_balance(session: requests.Session, address: str, api_key: str) -> Decimal:
    result = api_call(
        session,
        module="account",
        action="balance",
        address=address,
        tag="latest",
        apikey=api_key,
    )
    return wei_to_eth(result)


def get_eth_price(session: requests.Session, api_key: str) -> Optional[Decimal]:
    result = api_call(
        session,
        module="stats",
        action="ethprice",
        apikey=api_key,
    )
    try:
        return Decimal(result["ethusd"])
    except Exception:
        return None


def get_recent_transactions(
    session: requests.Session,
    address: str,
    api_key: str,
    limit: int,
) -> List[Dict[str, Any]]:
    key = f"{address}_txlist"
    path = cache_path(key)

    cached = load_cache(path)
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
        save_cache(path, result)
        return result[:limit]

    return []


# --------------------------------------------------------------------------- #
#                                PROCESSING                                   #
# --------------------------------------------------------------------------- #

def summarize_transactions(
    transactions: Iterable[Dict[str, Any]],
    address: str,
) -> Tuple[Decimal, Decimal]:

    received = Decimal(0)
    sent = Decimal(0)

    for tx in transactions:
        value = wei_to_eth(tx.get("value"))
        if tx.get("to", "").lower() == address:
            received += value
        elif tx.get("from", "").lower() == address:
            sent += value

    return received, sent


# --------------------------------------------------------------------------- #
#                             PARALLEL EXECUTION                              #
# --------------------------------------------------------------------------- #

def run_tasks(tasks: Dict[str, Callable[[], Any]]) -> Dict[str, Any]:
    results: Dict[str, Any] = {}

    with ThreadPoolExecutor(max_workers=Config.MAX_THREADS) as executor:
        futures = {executor.submit(fn): name for name, fn in tasks.items()}

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

def print_summary(
    address: str,
    balance: Optional[Decimal],
    price: Optional[Decimal],
    total_in: Decimal,
    total_out: Decimal,
    transactions: List[Dict[str, Any]],
) -> None:

    print("\nEthereum Address Summary")
    print("=" * 60)
    print(f"Address:        {address}")
    print(f"Balance:        {balance or Decimal(0):.6f} ETH")
    print(f"ETH Price:      ${price or Decimal(0):.2f}")
    print(f"Total Received: {total_in:.6f} ETH")
    print(f"Total Sent:     {total_out:.6f} ETH")
    print(f"Transactions:   {len(transactions)}\n")

    rows = []
    for tx in transactions[:10]:
        rows.append([
            (tx.get("hash") or "")[:12],
            wei_to_eth(tx.get("value")),
            parse_utc(tx.get("timeStamp")),
        ])

    if rows:
        print(tabulate(rows, headers=("Hash", "ETH", "Time"), tablefmt="fancy_grid"))


# --------------------------------------------------------------------------- #
#                                    CLI                                      #
# --------------------------------------------------------------------------- #

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ethereum transaction analyzer (Etherscan)"
    )

    parser.add_argument("address", help="Ethereum address")
    parser.add_argument(
        "--apikey",
        default=os.getenv("ETHERSCAN_API_KEY"),
        help="Etherscan API key (or ENV ETHERSCAN_API_KEY)",
    )
    parser.add_argument("--count", type=int, default=Config.DEFAULT_TX_COUNT)

    args = parser.parse_args()

    if not args.apikey:
        parser.error("Missing Etherscan API key")

    try:
        address = validate_eth_address(args.address)
    except ValidationError as exc:
        parser.error(str(exc))

    session = create_session()

    try:
        results = run_tasks({
            "balance": lambda: get_eth_balance(session, address, args.apikey),
            "price": lambda: get_eth_price(session, args.apikey),
            "txs": lambda: get_recent_transactions(
                session, address, args.apikey, args.count
            ),
        })
    finally:
        session.close()

    transactions = results.get("txs") or []
    total_in, total_out = summarize_transactions(transactions, address)

    print_summary(
        address,
        results.get("balance"),
        results.get("price"),
        total_in,
        total_out,
        transactions,
    )


if __name__ == "__main__":
    main()
