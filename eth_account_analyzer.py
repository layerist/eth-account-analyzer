#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import threading
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
# CONFIG
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class Config:
    BASE_URL: str = "https://api.etherscan.io/api"

    WEI_PER_ETH: Decimal = Decimal("1000000000000000000")

    DEFAULT_TX_COUNT: int = 10
    TIMEOUT_SECONDS: int = 10
    RETRIES: int = 4

    MAX_THREADS: int = min(8, (os.cpu_count() or 2) * 2)

    CACHE_DIR: Path = Path(".cache_etherscan")
    CACHE_TTL_SECONDS: int = 300

    RATE_LIMIT_PER_SEC: float = 4.5

    MAX_PAGES: int = 5  # for full scan safety


# --------------------------------------------------------------------------- #
# LOGGING
# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# ERRORS
# --------------------------------------------------------------------------- #

class EtherscanError(RuntimeError):
    pass


class ValidationError(ValueError):
    pass


# --------------------------------------------------------------------------- #
# RATE LIMITER
# --------------------------------------------------------------------------- #

class RateLimiter:
    def __init__(self, rate: float):
        self.min_interval = 1.0 / rate
        self.lock = threading.Lock()
        self.last = 0.0

    def wait(self):
        with self.lock:
            now = time.time()
            delta = now - self.last
            if delta < self.min_interval:
                time.sleep(self.min_interval - delta)
            self.last = time.time()


rate_limiter = RateLimiter(Config.RATE_LIMIT_PER_SEC)


# --------------------------------------------------------------------------- #
# SESSION
# --------------------------------------------------------------------------- #

_thread_local = threading.local()


def get_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        session = requests.Session()

        retry = Retry(
            total=Config.RETRIES,
            backoff_factor=0.8,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )

        adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        _thread_local.session = session

    return _thread_local.session


# --------------------------------------------------------------------------- #
# VALIDATION
# --------------------------------------------------------------------------- #

def validate_eth_address(address: str) -> str:
    if not isinstance(address, str):
        raise ValidationError("Address must be string")

    if not address.startswith("0x") or len(address) != 42:
        raise ValidationError("Invalid Ethereum address format")

    return address.lower()


# --------------------------------------------------------------------------- #
# CACHE
# --------------------------------------------------------------------------- #

def _hash_key(data: Dict[str, Any]) -> str:
    raw = json.dumps(data, sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()


def cache_path(key: Dict[str, Any]) -> Path:
    Config.CACHE_DIR.mkdir(exist_ok=True)
    return Config.CACHE_DIR / f"{_hash_key(key)}.json"


def load_cache(path: Path) -> Optional[Any]:
    try:
        if not path.exists():
            return None

        if time.time() - path.stat().st_mtime > Config.CACHE_TTL_SECONDS:
            return None

        return json.loads(path.read_text())
    except Exception:
        return None


def save_cache(path: Path, data: Any) -> None:
    try:
        path.write_text(json.dumps(data))
    except Exception as exc:
        log.debug("Cache write failed: %s", exc)


# --------------------------------------------------------------------------- #
# API CORE
# --------------------------------------------------------------------------- #

def api_call(api_key: str, **params: Any) -> Any:
    session = get_session()

    params["apikey"] = api_key

    for attempt in range(Config.RETRIES + 1):
        rate_limiter.wait()

        try:
            r = session.get(
                Config.BASE_URL,
                params=params,
                timeout=Config.TIMEOUT_SECONDS,
            )
            r.raise_for_status()
        except requests.RequestException as exc:
            if attempt == Config.RETRIES:
                raise EtherscanError(f"Network error: {exc}")
            time.sleep(1.5 * (attempt + 1))
            continue

        try:
            data = r.json()
        except Exception:
            raise EtherscanError("Invalid JSON response")

        status = data.get("status")
        message = data.get("message", "")
        result = data.get("result")

        # handle rate limit INSIDE JSON
        if isinstance(result, str) and "rate limit" in result.lower():
            log.warning("Rate limit hit, retrying...")
            time.sleep(1.5 * (attempt + 1))
            continue

        if status == "0":
            if message in ("No transactions found", "No records found"):
                return []
            raise EtherscanError(message or "API error")

        return result

    raise EtherscanError("Max retries exceeded")


# --------------------------------------------------------------------------- #
# HELPERS
# --------------------------------------------------------------------------- #

def safe_decimal(v: Any) -> Decimal:
    try:
        return Decimal(str(v))
    except (InvalidOperation, TypeError):
        return Decimal(0)


def wei_to_eth(v: Any) -> Decimal:
    return safe_decimal(v) / Config.WEI_PER_ETH


def parse_utc(ts: Any) -> str:
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "N/A"


# --------------------------------------------------------------------------- #
# API FUNCTIONS
# --------------------------------------------------------------------------- #

def get_eth_balance(address: str, api_key: str) -> Decimal:
    result = api_call(api_key, module="account", action="balance", address=address, tag="latest")
    return wei_to_eth(result)


def get_eth_price(api_key: str) -> Optional[Decimal]:
    result = api_call(api_key, module="stats", action="ethprice")
    try:
        return Decimal(result["ethusd"])
    except Exception:
        return None


def get_transactions(address: str, api_key: str, limit: int, full_scan: bool) -> List[Dict[str, Any]]:
    key = {
        "addr": address,
        "limit": limit,
        "full": full_scan,
        "k": hashlib.md5(api_key.encode()).hexdigest(),
    }

    path = cache_path(key)
    cached = load_cache(path)
    if cached:
        return cached

    txs: List[Dict[str, Any]] = []

    page = 1
    offset = min(100, limit)

    while True:
        result = api_call(
            api_key,
            module="account",
            action="txlist",
            address=address,
            startblock=0,
            endblock=99999999,
            page=page,
            offset=offset,
            sort="desc",
        )

        if not result:
            break

        txs.extend(result)

        if not full_scan:
            break

        if len(result) < offset or page >= Config.MAX_PAGES:
            break

        page += 1

    txs = txs[:limit]

    save_cache(path, txs)
    return txs


# --------------------------------------------------------------------------- #
# PROCESSING
# --------------------------------------------------------------------------- #

def summarize(transactions: Iterable[Dict[str, Any]], address: str) -> Tuple[Decimal, Decimal]:
    received = Decimal(0)
    sent = Decimal(0)

    for tx in transactions:
        value = wei_to_eth(tx.get("value"))

        if (tx.get("to") or "").lower() == address:
            received += value
        elif (tx.get("from") or "").lower() == address:
            sent += value

    return received, sent


# --------------------------------------------------------------------------- #
# PARALLEL
# --------------------------------------------------------------------------- #

def run_tasks(tasks: Dict[str, Callable[[], Any]]) -> Dict[str, Any]:
    results: Dict[str, Any] = {}

    with ThreadPoolExecutor(max_workers=Config.MAX_THREADS) as executor:
        futures = {executor.submit(fn): name for name, fn in tasks.items()}

        for future in as_completed(futures):
            name = futures[future]
            try:
                results[name] = future.result(timeout=Config.TIMEOUT_SECONDS + 5)
            except Exception as exc:
                log.error("%s failed: %s", name, exc)
                results[name] = None

    return results


# --------------------------------------------------------------------------- #
# OUTPUT
# --------------------------------------------------------------------------- #

def print_summary(address: str, balance, price, total_in, total_out, txs):
    print("\nEthereum Address Summary")
    print("=" * 60)
    print(f"Address:        {address}")
    print(f"Balance:        {balance or 0:.6f} ETH")
    print(f"ETH Price:      ${price or 0:.2f}")
    print(f"Total Received: {total_in:.6f} ETH")
    print(f"Total Sent:     {total_out:.6f} ETH")
    print(f"Transactions:   {len(txs)}\n")

    rows = [
        [
            (tx.get("hash") or "")[:12],
            f"{wei_to_eth(tx.get('value')):.6f}",
            parse_utc(tx.get("timeStamp")),
        ]
        for tx in txs
    ]

    if rows:
        print(tabulate(rows, headers=("Hash", "ETH", "Time"), tablefmt="fancy_grid"))


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def main():
    parser = argparse.ArgumentParser(description="Ethereum wallet analyzer (Etherscan)")

    parser.add_argument("address")
    parser.add_argument("--apikey", default=os.getenv("ETHERSCAN_API_KEY"))
    parser.add_argument("--count", type=int, default=Config.DEFAULT_TX_COUNT)
    parser.add_argument("--full", action="store_true", help="Scan multiple pages")
    parser.add_argument("--json", action="store_true", help="Output JSON")
    parser.add_argument("--verbose", action="store_true")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if not args.apikey:
        parser.error("Missing API key")

    address = validate_eth_address(args.address)

    tasks = {
        "balance": lambda: get_eth_balance(address, args.apikey),
        "price": lambda: get_eth_price(args.apikey),
        "txs": lambda: get_transactions(address, args.apikey, args.count, args.full),
    }

    results = run_tasks(tasks)

    txs = results.get("txs") or []
    total_in, total_out = summarize(txs, address)

    if args.json:
        print(json.dumps({
            "address": address,
            "balance": str(results.get("balance")),
            "price": str(results.get("price")),
            "received": str(total_in),
            "sent": str(total_out),
            "tx_count": len(txs),
        }, indent=2))
        return

    print_summary(
        address,
        results.get("balance"),
        results.get("price"),
        total_in,
        total_out,
        txs,
    )


if __name__ == "__main__":
    main()
