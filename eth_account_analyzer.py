#!/usr/bin/env python3
"""Etherscan wallet analyzer.

Features:
- Etherscan API V2 with configurable chain ID
- shared, thread-safe request pacing
- retry/backoff with Retry-After support
- atomic TTL cache
- parallel balance, price and transaction requests
- transaction direction/status/fee reporting
- JSON output suitable for scripts

Requires:
    pip install requests tabulate
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import random
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional, TypeVar

import requests
from requests.adapters import HTTPAdapter
from tabulate import tabulate


LOG = logging.getLogger("etherscan_wallet")
T = TypeVar("T")
ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
ZERO = Decimal("0")
WEI = Decimal("1000000000000000000")
GWEI = Decimal("1000000000")


class EtherscanError(RuntimeError):
    """Etherscan returned an error or an unusable response."""


class ValidationError(ValueError):
    """CLI input validation failed."""


@dataclass(frozen=True)
class Settings:
    base_url: str
    chain_id: int
    timeout: float
    retries: int
    rate_limit: float
    cache_dir: Path
    cache_ttl: int
    max_pages: int
    page_size: int
    no_cache: bool


@dataclass(frozen=True)
class TxView:
    hash: str
    timestamp: str
    direction: str
    counterparty: str
    value_eth: Decimal
    fee_eth: Decimal
    status: str
    block_number: int

    def to_json(self) -> dict[str, Any]:
        result = asdict(self)
        result["value_eth"] = decimal_text(self.value_eth)
        result["fee_eth"] = decimal_text(self.fee_eth)
        return result


class RateLimiter:
    """Process-wide fixed-interval limiter based on monotonic time."""

    def __init__(self, calls_per_second: float) -> None:
        if calls_per_second <= 0:
            raise ValidationError("rate limit must be greater than zero")
        self._interval = 1.0 / calls_per_second
        self._next_allowed = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            delay = self._next_allowed - now
            if delay > 0:
                time.sleep(delay)
                now = time.monotonic()
            self._next_allowed = max(now, self._next_allowed) + self._interval


class AtomicJsonCache:
    def __init__(self, directory: Path, ttl: int, disabled: bool = False) -> None:
        self.directory = directory
        self.ttl = ttl
        self.disabled = disabled

    def _path(self, key: Mapping[str, Any]) -> Path:
        payload = json.dumps(key, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        return self.directory / f"{digest}.json"

    def get(self, key: Mapping[str, Any]) -> Optional[Any]:
        if self.disabled or self.ttl <= 0:
            return None
        path = self._path(key)
        try:
            if not path.is_file():
                return None
            age = time.time() - path.stat().st_mtime
            if age > self.ttl:
                path.unlink(missing_ok=True)
                return None
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError) as exc:
            LOG.debug("Ignoring unreadable cache file %s: %s", path, exc)
            return None

    def put(self, key: Mapping[str, Any], value: Any) -> None:
        if self.disabled or self.ttl <= 0:
            return
        path = self._path(key)
        tmp = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        try:
            self.directory.mkdir(parents=True, exist_ok=True)
            tmp.write_text(
                json.dumps(value, ensure_ascii=False, separators=(",", ":")),
                encoding="utf-8",
            )
            os.replace(tmp, path)
        except OSError as exc:
            LOG.debug("Cache write failed for %s: %s", path, exc)
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass


class EtherscanClient:
    def __init__(self, api_key: str, settings: Settings) -> None:
        self.api_key = api_key
        self.settings = settings
        self.limiter = RateLimiter(settings.rate_limit)
        self.cache = AtomicJsonCache(settings.cache_dir, settings.cache_ttl, settings.no_cache)
        self._local = threading.local()

    def _session(self) -> requests.Session:
        existing = getattr(self._local, "session", None)
        if existing is not None:
            return existing

        session = requests.Session()
        adapter = HTTPAdapter(
            max_retries=0,  # retries are handled in request() to inspect API-level errors
            pool_connections=8,
            pool_maxsize=8,
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update({
            "Accept": "application/json",
            "User-Agent": "etherscan-wallet-analyzer/2.0",
        })
        self._local.session = session
        return session

    def request(self, *, module: str, action: str, **params: Any) -> Any:
        query = {
            "chainid": str(self.settings.chain_id),
            "module": module,
            "action": action,
            **params,
            "apikey": self.api_key,
        }
        last_error: Optional[BaseException] = None

        for attempt in range(self.settings.retries + 1):
            self.limiter.wait()
            try:
                response = self._session().get(
                    self.settings.base_url,
                    params=query,
                    timeout=self.settings.timeout,
                )

                if response.status_code == 429 or response.status_code >= 500:
                    retry_after = parse_retry_after(response.headers.get("Retry-After"))
                    raise RetryableError(
                        f"HTTP {response.status_code}", retry_after=retry_after
                    )

                response.raise_for_status()
                try:
                    payload = response.json()
                except ValueError as exc:
                    preview = response.text[:200].replace("\n", " ")
                    raise EtherscanError(f"Invalid JSON response: {preview!r}") from exc

                if not isinstance(payload, dict):
                    raise EtherscanError("Unexpected API response type")

                status = str(payload.get("status", ""))
                message = str(payload.get("message", ""))
                result = payload.get("result")
                combined = f"{message} {result}".lower()

                if is_no_records(message, result):
                    return []
                if is_retryable_api_error(combined):
                    raise RetryableError(str(result or message or "rate limited"))
                if status == "0":
                    detail = result if result not in (None, "") else message
                    raise EtherscanError(f"Etherscan {module}/{action}: {detail}")
                if "result" not in payload:
                    raise EtherscanError(f"Etherscan {module}/{action}: missing result")
                return result

            except RetryableError as exc:
                last_error = exc
                delay = exc.retry_after if exc.retry_after is not None else backoff(attempt)
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_error = exc
                delay = backoff(attempt)
            except requests.HTTPError as exc:
                raise EtherscanError(f"HTTP error: {exc}") from exc

            if attempt >= self.settings.retries:
                break
            LOG.warning(
                "%s/%s failed (%s), retrying in %.2fs [%d/%d]",
                module,
                action,
                last_error,
                delay,
                attempt + 1,
                self.settings.retries,
            )
            time.sleep(delay)

        raise EtherscanError(
            f"Etherscan {module}/{action} failed after {self.settings.retries + 1} attempts: "
            f"{last_error}"
        )

    def balance(self, address: str) -> Decimal:
        result = self.request(
            module="account", action="balance", address=address, tag="latest"
        )
        return wei_to_eth(result)

    def eth_price(self) -> Optional[Decimal]:
        # ethprice is Ethereum-specific; for other chains it may be unavailable or
        # still represent ETH rather than the chain's native token.
        try:
            result = self.request(module="stats", action="ethprice")
            if not isinstance(result, dict):
                return None
            return to_decimal(result.get("ethusd"), default=None)
        except EtherscanError as exc:
            LOG.warning("ETH/USD price unavailable: %s", exc)
            return None

    def transactions(self, address: str, count: int) -> list[dict[str, Any]]:
        if count == 0:
            return []

        cache_key = {
            "version": 2,
            "chain_id": self.settings.chain_id,
            "address": address,
            "count": count,
            "max_pages": self.settings.max_pages,
            "page_size": self.settings.page_size,
        }
        cached = self.cache.get(cache_key)
        if isinstance(cached, list):
            LOG.debug("Transaction cache hit")
            return cached

        collected: list[dict[str, Any]] = []
        page = 1
        while page <= self.settings.max_pages and len(collected) < count:
            offset = min(self.settings.page_size, count - len(collected))
            result = self.request(
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
            if not isinstance(result, list):
                raise EtherscanError("txlist returned a non-list result")

            rows = [row for row in result if isinstance(row, dict)]
            collected.extend(rows)
            if len(rows) < offset:
                break
            page += 1

        collected = collected[:count]
        self.cache.put(cache_key, collected)
        return collected


class RetryableError(RuntimeError):
    def __init__(self, message: str, retry_after: Optional[float] = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


def backoff(attempt: int) -> float:
    return min(20.0, 0.8 * (2**attempt)) + random.uniform(0.0, 0.25)


def parse_retry_after(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        return max(0.0, min(float(value), 60.0))
    except ValueError:
        return None


def is_retryable_api_error(text: str) -> bool:
    markers = (
        "rate limit",
        "max rate limit",
        "temporarily unavailable",
        "timeout",
        "server too busy",
    )
    return any(marker in text for marker in markers)


def is_no_records(message: str, result: Any) -> bool:
    text = f"{message} {result}".lower()
    return "no transactions found" in text or "no records found" in text


def validate_address(value: str) -> str:
    value = value.strip()
    if not ADDRESS_RE.fullmatch(value):
        raise ValidationError("address must be 0x followed by exactly 40 hexadecimal characters")
    return value.lower()


def positive_int(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return number


def non_negative_int(value: str) -> int:
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError("must be zero or greater")
    return number


def positive_float(value: str) -> float:
    number = float(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return number


def to_decimal(value: Any, default: Optional[Decimal] = ZERO) -> Optional[Decimal]:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default


def wei_to_eth(value: Any) -> Decimal:
    return (to_decimal(value) or ZERO) / WEI


def decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def utc_timestamp(value: Any) -> str:
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except (TypeError, ValueError, OSError, OverflowError):
        return "N/A"


def short_address(value: str) -> str:
    if len(value) <= 18:
        return value
    return f"{value[:10]}…{value[-6:]}"


def transaction_status(tx: Mapping[str, Any]) -> str:
    if str(tx.get("isError", "0")) == "1" or str(tx.get("txreceipt_status", "1")) == "0":
        return "failed"
    return "ok"


def transaction_fee_eth(tx: Mapping[str, Any]) -> Decimal:
    gas_used = to_decimal(tx.get("gasUsed")) or ZERO
    gas_price = to_decimal(tx.get("gasPrice")) or ZERO
    return (gas_used * gas_price) / WEI


def normalize_transactions(rows: Iterable[Mapping[str, Any]], address: str) -> list[TxView]:
    views: list[TxView] = []
    for tx in rows:
        sender = str(tx.get("from") or "").lower()
        recipient = str(tx.get("to") or "").lower()
        if sender == address and recipient == address:
            direction = "self"
            counterparty = address
        elif sender == address:
            direction = "out"
            counterparty = recipient or "contract creation"
        elif recipient == address:
            direction = "in"
            counterparty = sender or "unknown"
        else:
            direction = "other"
            counterparty = recipient or sender or "unknown"

        views.append(TxView(
            hash=str(tx.get("hash") or ""),
            timestamp=utc_timestamp(tx.get("timeStamp")),
            direction=direction,
            counterparty=counterparty,
            value_eth=wei_to_eth(tx.get("value")),
            fee_eth=transaction_fee_eth(tx) if sender == address else ZERO,
            status=transaction_status(tx),
            block_number=int(tx.get("blockNumber") or 0),
        ))
    return views


def summarize(views: Iterable[TxView]) -> dict[str, Decimal | int]:
    received = ZERO
    sent = ZERO
    fees = ZERO
    failed = 0

    for tx in views:
        if tx.status == "failed":
            failed += 1
            # A failed transaction transfers no value, but the sender still pays gas.
            fees += tx.fee_eth
            continue
        if tx.direction == "in":
            received += tx.value_eth
        elif tx.direction == "out":
            sent += tx.value_eth
        fees += tx.fee_eth

    return {"received": received, "sent": sent, "fees": fees, "failed": failed}


def run_parallel(tasks: Mapping[str, Callable[[], T]], workers: int = 3) -> dict[str, Any]:
    results: dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=min(workers, len(tasks)), thread_name_prefix="api") as pool:
        futures = {pool.submit(func): name for name, func in tasks.items()}
        for future in as_completed(futures):
            name = futures[future]
            try:
                results[name] = future.result()
            except Exception as exc:  # preserve partial report when one endpoint fails
                LOG.error("%s request failed: %s", name, exc)
                results[name] = None
    return results


def render_text(
    *,
    address: str,
    chain_id: int,
    balance: Optional[Decimal],
    eth_price: Optional[Decimal],
    views: list[TxView],
    summary: Mapping[str, Decimal | int],
) -> None:
    print("\nETHERSCAN WALLET REPORT")
    print("=" * 72)
    print(f"Address:          {address}")
    print(f"Chain ID:         {chain_id}")
    print(f"Native balance:   {format_eth(balance)}")
    print(f"ETH/USD:          {format_usd(eth_price)}")
    print(f"Sample received:  {format_eth(summary['received'])}")
    print(f"Sample sent:      {format_eth(summary['sent'])}")
    print(f"Sample fees:      {format_eth(summary['fees'])}")
    print(f"Transactions:     {len(views)} ({summary['failed']} failed)")
    print("\nNote: received/sent/fees cover only the displayed normal transactions.\n")

    rows = [
        [
            tx.timestamp.replace("T", " ").replace("Z", ""),
            tx.direction,
            tx.status,
            f"{tx.value_eth:.8f}",
            f"{tx.fee_eth:.8f}" if tx.fee_eth else "-",
            short_address(tx.counterparty),
            tx.hash[:14] + ("…" if len(tx.hash) > 14 else ""),
        ]
        for tx in views
    ]
    if rows:
        print(tabulate(
            rows,
            headers=["UTC time", "Dir", "Status", "Value ETH", "Fee ETH", "Counterparty", "Hash"],
            tablefmt="simple_grid",
        ))
    else:
        print("No normal transactions found.")


def format_eth(value: Any) -> str:
    if not isinstance(value, Decimal):
        return "N/A"
    return f"{value:.8f}"


def format_usd(value: Optional[Decimal]) -> str:
    return "N/A" if value is None else f"${value:,.2f}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze an EVM address through Etherscan API V2.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("address", help="EVM address (0x + 40 hex characters)")
    parser.add_argument("--apikey", default=os.getenv("ETHERSCAN_API_KEY"), help="Etherscan API key")
    parser.add_argument("--chain-id", type=positive_int, default=int(os.getenv("ETHERSCAN_CHAIN_ID", "1")))
    parser.add_argument("--count", type=non_negative_int, default=10, help="number of recent normal transactions")
    parser.add_argument("--max-pages", type=positive_int, default=5)
    parser.add_argument("--page-size", type=positive_int, default=100, help="maximum rows requested per API page")
    parser.add_argument("--rate", type=positive_float, default=3.0, help="maximum API calls per second")
    parser.add_argument("--timeout", type=positive_float, default=12.0, help="HTTP timeout in seconds")
    parser.add_argument("--retries", type=non_negative_int, default=4)
    parser.add_argument("--cache-ttl", type=non_negative_int, default=300, help="transaction cache TTL in seconds")
    parser.add_argument("--cache-dir", type=Path, default=Path(".cache_eth"))
    parser.add_argument("--no-cache", action="store_true")
    parser.add_argument("--no-price", action="store_true", help="skip the ETH/USD request")
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
    )

    if not args.apikey:
        parser.error("missing API key: use --apikey or ETHERSCAN_API_KEY")

    try:
        address = validate_address(args.address)
        settings = Settings(
            base_url="https://api.etherscan.io/v2/api",
            chain_id=args.chain_id,
            timeout=args.timeout,
            retries=args.retries,
            rate_limit=args.rate,
            cache_dir=args.cache_dir,
            cache_ttl=args.cache_ttl,
            max_pages=args.max_pages,
            page_size=min(args.page_size, 1000),
            no_cache=args.no_cache,
        )
        client = EtherscanClient(args.apikey, settings)

        tasks: dict[str, Callable[[], Any]] = {
            "balance": lambda: client.balance(address),
            "transactions": lambda: client.transactions(address, args.count),
        }
        if not args.no_price:
            tasks["price"] = client.eth_price

        result = run_parallel(tasks)
        raw_transactions = result.get("transactions") or []
        views = normalize_transactions(raw_transactions, address)
        stats = summarize(views)

        if args.json:
            document = {
                "address": address,
                "chain_id": settings.chain_id,
                "native_balance": (
                    decimal_text(result["balance"])
                    if isinstance(result.get("balance"), Decimal)
                    else None
                ),
                "eth_usd": (
                    decimal_text(result["price"])
                    if isinstance(result.get("price"), Decimal)
                    else None
                ),
                "sample": {
                    "transaction_count": len(views),
                    "received_eth": decimal_text(stats["received"]),
                    "sent_eth": decimal_text(stats["sent"]),
                    "fees_eth": decimal_text(stats["fees"]),
                    "failed_count": stats["failed"],
                },
                "transactions": [tx.to_json() for tx in views],
            }
            print(json.dumps(document, ensure_ascii=False, indent=2))
        else:
            render_text(
                address=address,
                chain_id=settings.chain_id,
                balance=result.get("balance"),
                eth_price=result.get("price"),
                views=views,
                summary=stats,
            )

        # Balance and transactions are the core report; price is optional.
        return 0 if result.get("balance") is not None and result.get("transactions") is not None else 2

    except (ValidationError, EtherscanError) as exc:
        LOG.error("%s", exc)
        return 2
    except KeyboardInterrupt:
        LOG.warning("Interrupted")
        return 130


if __name__ == "__main__":
    sys.exit(main())
