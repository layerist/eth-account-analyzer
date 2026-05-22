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
from typing import Any, Dict, Iterable, List, Optional

import requests
from requests.adapters import HTTPAdapter, Retry
from tabulate import tabulate


# --------------------------------------------------------------------------- #
# CONFIG
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class Config:
    BASE_URL: str = "https://api.etherscan.io/api"
    WEI = Decimal("1000000000000000000")

    TIMEOUT: int = 10
    RETRIES: int = 4
    RATE_LIMIT: float = 4.5

    DEFAULT_TX: int = 10
    MAX_THREADS: int = min(8, (os.cpu_count() or 2) * 2)
    MAX_PAGES: int = 5

    CACHE_DIR: Path = Path(".cache_eth")
    CACHE_TTL: int = 300


CFG = Config()


# --------------------------------------------------------------------------- #
# LOGGING
# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("eth")


# --------------------------------------------------------------------------- #
# ERRORS
# --------------------------------------------------------------------------- #

class EtherscanError(RuntimeError):
    pass


class ValidationError(ValueError):
    pass


# --------------------------------------------------------------------------- #
# RATE LIMITER (simple token bucket-ish)
# --------------------------------------------------------------------------- #

class RateLimiter:
    def __init__(self, rate: float):
        self.min_delay = 1.0 / rate
        self.lock = threading.Lock()
        self.last = 0.0

    def wait(self):
        with self.lock:
            now = time.time()
            wait = self.min_delay - (now - self.last)
            if wait > 0:
                time.sleep(wait)
            self.last = time.time()


rate_limiter = RateLimiter(CFG.RATE_LIMIT)


# --------------------------------------------------------------------------- #
# SESSION
# --------------------------------------------------------------------------- #

_local = threading.local()


def session() -> requests.Session:
    if hasattr(_local, "s"):
        return _local.s

    s = requests.Session()
    retry = Retry(
        total=CFG.RETRIES,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)

    _local.s = s
    return s


# --------------------------------------------------------------------------- #
# VALIDATION
# --------------------------------------------------------------------------- #

def validate(addr: str) -> str:
    if not isinstance(addr, str):
        raise ValidationError("Address must be string")
    if not addr.startswith("0x") or len(addr) != 42:
        raise ValidationError("Invalid ETH address")
    return addr.lower()


# --------------------------------------------------------------------------- #
# CACHE
# --------------------------------------------------------------------------- #

def _hash(obj: Dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True).encode()).hexdigest()


def cache_file(key: Dict[str, Any]) -> Path:
    CFG.CACHE_DIR.mkdir(exist_ok=True)
    return CFG.CACHE_DIR / f"{_hash(key)}.json"


def load_cache(p: Path) -> Optional[Any]:
    try:
        if not p.exists():
            return None
        if time.time() - p.stat().st_mtime > CFG.CACHE_TTL:
            return None
        return json.loads(p.read_text())
    except Exception:
        return None


def save_cache(p: Path, data: Any):
    try:
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(data))
        tmp.replace(p)
    except Exception as e:
        log.debug("cache write failed: %s", e)


# --------------------------------------------------------------------------- #
# API CORE
# --------------------------------------------------------------------------- #

def api(key: str, **params) -> Any:
    s = session()
    params["apikey"] = key

    for i in range(CFG.RETRIES + 1):
        rate_limiter.wait()

        try:
            r = s.get(CFG.BASE_URL, params=params, timeout=CFG.TIMEOUT)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            if i == CFG.RETRIES:
                raise EtherscanError(str(e))
            time.sleep(1.2 * (i + 1))
            continue

        result = data.get("result")
        status = data.get("status")

        if isinstance(result, str) and "rate limit" in result.lower():
            time.sleep(1.5 * (i + 1))
            continue

        if status == "0":
            if data.get("message") in ("No transactions found", "No records found"):
                return []
            raise EtherscanError(data.get("message", "API error"))

        return result

    raise EtherscanError("max retries reached")


# --------------------------------------------------------------------------- #
# HELPERS
# --------------------------------------------------------------------------- #

def dec(x: Any) -> Decimal:
    try:
        return Decimal(str(x))
    except Exception:
        return Decimal(0)


def wei_to_eth(x: Any) -> Decimal:
    return dec(x) / CFG.WEI


def ts(x: Any) -> str:
    try:
        return datetime.fromtimestamp(int(x), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "N/A"


# --------------------------------------------------------------------------- #
# API WRAPPERS
# --------------------------------------------------------------------------- #

def balance(addr: str, key: str) -> Decimal:
    return wei_to_eth(api(key, module="account", action="balance", address=addr, tag="latest"))


def price(key: str) -> Optional[Decimal]:
    r = api(key, module="stats", action="ethprice")
    try:
        return Decimal(r["ethusd"])
    except Exception:
        return None


def txs(addr: str, key: str, limit: int, full: bool) -> List[Dict[str, Any]]:
    cache_key = {"a": addr, "l": limit, "f": full, "k": hashlib.md5(key.encode()).hexdigest()}
    path = cache_file(cache_key)

    cached = load_cache(path)
    if cached:
        return cached

    out: List[Dict[str, Any]] = []
    page = 1
    offset = min(100, limit)

    while True:
        r = api(
            key,
            module="account",
            action="txlist",
            address=addr,
            startblock=0,
            endblock=99999999,
            page=page,
            offset=offset,
            sort="desc",
        )

        if not r:
            break

        out.extend(r)

        if not full or len(r) < offset or page >= CFG.MAX_PAGES:
            break

        page += 1

    out = out[:limit]
    save_cache(path, out)
    return out


# --------------------------------------------------------------------------- #
# PROCESSING
# --------------------------------------------------------------------------- #

def summarize(txs: Iterable[Dict[str, Any]], addr: str):
    rcv, sent = Decimal(0), Decimal(0)

    for t in txs:
        v = wei_to_eth(t.get("value"))
        if (t.get("to") or "").lower() == addr:
            rcv += v
        elif (t.get("from") or "").lower() == addr:
            sent += v

    return rcv, sent


# --------------------------------------------------------------------------- #
# PARALLEL
# --------------------------------------------------------------------------- #

def run(tasks: Dict[str, callable]) -> Dict[str, Any]:
    res: Dict[str, Any] = {}

    with ThreadPoolExecutor(max_workers=CFG.MAX_THREADS) as ex:
        fut = {ex.submit(fn): k for k, fn in tasks.items()}

        for f in as_completed(fut):
            k = fut[f]
            try:
                res[k] = f.result(timeout=CFG.TIMEOUT + 5)
            except Exception as e:
                log.error("%s failed: %s", k, e)
                res[k] = None

    return res


# --------------------------------------------------------------------------- #
# OUTPUT
# --------------------------------------------------------------------------- #

def show(addr, bal, pr, rcv, sent, tx):
    print("\nETH WALLET REPORT")
    print("=" * 55)
    print("Address:   ", addr)
    print("Balance:   ", f"{bal or 0:.6f} ETH")
    print("Price:     ", f"${pr or 0:.2f}")
    print("Received:  ", f"{rcv:.6f} ETH")
    print("Sent:      ", f"{sent:.6f} ETH")
    print("Tx count:  ", len(tx))
    print()

    table = [
        [(t.get("hash") or "")[:12], f"{wei_to_eth(t.get('value')):.6f}", ts(t.get("timeStamp"))]
        for t in tx
    ]

    if table:
        print(tabulate(table, headers=["Hash", "ETH", "Time"], tablefmt="fancy_grid"))


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def main():
    p = argparse.ArgumentParser("Ethereum analyzer")
    p.add_argument("address")
    p.add_argument("--apikey", default=os.getenv("ETHERSCAN_API_KEY"))
    p.add_argument("--count", type=int, default=CFG.DEFAULT_TX)
    p.add_argument("--full", action="store_true")
    p.add_argument("--json", action="store_true")
    p.add_argument("--verbose", action="store_true")

    a = p.parse_args()

    if a.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if not a.apikey:
        p.error("missing API key")

    addr = validate(a.address)

    jobs = {
        "bal": lambda: balance(addr, a.apikey),
        "pr": lambda: price(a.apikey),
        "tx": lambda: txs(addr, a.apikey, a.count, a.full),
    }

    r = run(jobs)

    tx = r.get("tx") or []
    rcv, sent = summarize(tx, addr)

    if a.json:
        print(json.dumps({
            "address": addr,
            "balance": str(r.get("bal")),
            "price": str(r.get("pr")),
            "received": str(rcv),
            "sent": str(sent),
            "tx_count": len(tx),
        }, indent=2))
        return

    show(addr, r.get("bal"), r.get("pr"), rcv, sent, tx)


if __name__ == "__main__":
    main()
