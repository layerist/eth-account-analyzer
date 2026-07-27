#!/usr/bin/env python3
"""Robust Etherscan wallet analyzer.

Highlights:
- Etherscan API V2 with configurable chain ID and base URL
- thread-safe global request pacing
- bounded exponential backoff with Retry-After support
- atomic TTL cache with schema versioning
- parallel balance, price, and transaction requests
- correct fixed-size pagination (no skipped rows)
- precise Decimal-based value and fee calculations
- partial-result reporting and machine-readable JSON

Requires:
    pip install requests tabulate
"""

from __future__ import annotations

import argparse
import email.utils
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


APP_NAME = "etherscan-wallet-analyzer"
APP_VERSION = "3.0"
DEFAULT_BASE_URL = "https://api.etherscan.io/v2/api"
CACHE_SCHEMA_VERSION = 3
LOG = logging.getLogger("etherscan_wallet")
T = TypeVar("T")
ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
ZERO = Decimal("0")
WEI = Decimal(10) ** 18
MAX_RETRY_DELAY = 60.0


class EtherscanError(RuntimeError):
    """Etherscan returned an error or an unusable response."""


class ValidationError(ValueError):
    """User input or configuration validation failed."""


class RetryableError(RuntimeError):
    def __init__(self, message: str, retry_after: Optional[float] = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


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
    value_native: Decimal
    fee_native: Decimal
    status: str
    block_number: int
    nonce: int
    method_id: str

    def to_json(self) -> dict[str, Any]:
        result = asdict(self)
        result["value_native"] = decimal_text(self.value_native)
        result["fee_native"] = decimal_text(self.fee_native)
        return result


class RateLimiter:
    """Thread-safe fixed-interval limiter based on monotonic time."""

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
            stat = path.stat()
            if not path.is_file():
                return None
            age = max(0.0, time.time() - stat.st_mtime)
            if age > self.ttl:
                path.unlink(missing_ok=True)
                return None
            return json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
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
        session = getattr(self._local, "session", None)
        if session is not None:
            return session

        session = requests.Session()
        adapter = HTTPAdapter(max_retries=0, pool_connections=4, pool_maxsize=4)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": f"{APP_NAME}/{APP_VERSION}",
            }
        )
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
                    timeout=(min(5.0, self.settings.timeout), self.settings.timeout),
                )

                if response.status_code == 429 or 500 <= response.status_code <= 599:
                    raise RetryableError(
                        f"HTTP {response.status_code}",
                        retry_after=parse_retry_after(response.headers.get("Retry-After")),
                    )

                response.raise_for_status()
                payload = decode_json_response(response)
                status = str(payload.get("status", ""))
                message = str(payload.get("message", ""))
                result = payload.get("result")
                combined = f"{message} {result}".lower()

                if is_no_records(message, result):
                    return []
                if is_retryable_api_error(combined):
                    raise RetryableError(str(result or message or "temporary API error"))
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
                status = exc.response.status_code if exc.response is not None else "unknown"
                preview = response_preview(exc.response)
                raise EtherscanError(f"HTTP {status} for {module}/{action}: {preview}") from exc

            if attempt >= self.settings.retries:
                break
            LOG.warning(
                "%s/%s failed (%s); retrying in %.2fs [%d/%d]",
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
        return wei_to_native(
            self.request(module="account", action="balance", address=address, tag="latest")
        )

    def eth_price(self) -> Optional[Decimal]:
        try:
            result = self.request(module="stats", action="ethprice")
            if not isinstance(result, dict):
                return None
            return parse_decimal(result.get("ethusd"), field="ethusd", required=False)
        except EtherscanError as exc:
            LOG.warning("ETH/USD price unavailable: %s", exc)
            return None

    def transactions(self, address: str, count: int) -> list[dict[str, Any]]:
        if count == 0:
            return []

        cache_key = {
            "schema": CACHE_SCHEMA_VERSION,
            "chain_id": self.settings.chain_id,
            "address": address,
            "count": count,
            "max_pages": self.settings.max_pages,
            "page_size": self.settings.page_size,
        }
        cached = self.cache.get(cache_key)
        if isinstance(cached, list) and all(isinstance(row, dict) for row in cached):
            LOG.debug("Transaction cache hit")
            return cached

        collected: list[dict[str, Any]] = []
        offset = min(self.settings.page_size, 10000)

        for page in range(1, self.settings.max_pages + 1):
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
            if len(collected) >= count or len(result) < offset:
                break
        else:
            if len(collected) < count:
                LOG.warning(
                    "Reached --max-pages=%d before collecting %d transactions",
                    self.settings.max_pages,
                    count,
                )

        collected = collected[:count]
        self.cache.put(cache_key, collected)
        return collected


def decode_json_response(response: requests.Response) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError as exc:
        raise EtherscanError(f"Invalid JSON response: {response_preview(response)!r}") from exc
    if not isinstance(payload, dict):
        raise EtherscanError(f"Unexpected API response type: {type(payload).__name__}")
    return payload


def response_preview(response: Optional[requests.Response], limit: int = 240) -> str:
    if response is None:
        return "no response body"
    return response.text[:limit].replace("\r", " ").replace("\n", " ").strip() or "empty body"


def backoff(attempt: int) -> float:
    return min(20.0, 0.8 * (2**attempt)) + random.uniform(0.0, 0.35)


def parse_retry_after(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        return min(max(float(value), 0.0), MAX_RETRY_DELAY)
    except ValueError:
        pass

    try:
        retry_at = email.utils.parsedate_to_datetime(value)
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        seconds = (retry_at - datetime.now(timezone.utc)).total_seconds()
        return min(max(seconds, 0.0), MAX_RETRY_DELAY)
    except (TypeError, ValueError, OverflowError):
        return None


def is_retryable_api_error(text: str) -> bool:
    markers = (
        "rate limit",
        "max rate limit",
        "temporarily unavailable",
        "timeout",
        "server too busy",
        "please try again",
        "query timeout",
    )
    return any(marker in text for marker in markers)


def is_no_records(message: str, result: Any) -> bool:
    text = f"{message} {result}".lower()
    return "no transactions found" in text or "no records found" in text


def validate_address(value: str) -> str:
    normalized = value.strip()
    if not ADDRESS_RE.fullmatch(normalized):
        raise ValidationError("address must be 0x followed by exactly 40 hexadecimal characters")
    return normalized.lower()


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValidationError(f"{name} must be an integer, got {raw!r}") from exc


def positive_int(value: str) -> int:
    try:
        number = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if number <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return number


def non_negative_int(value: str) -> int:
    try:
        number = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if number < 0:
        raise argparse.ArgumentTypeError("must be zero or greater")
    return number


def positive_float(value: str) -> float:
    try:
        number = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if number <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return number


def parse_decimal(value: Any, *, field: str, required: bool = True) -> Optional[Decimal]:
    if value is None or value == "":
        if required:
            raise EtherscanError(f"Missing numeric field: {field}")
        return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        if required:
            raise EtherscanError(f"Invalid numeric field {field}: {value!r}") from exc
        return None
    if not number.is_finite():
        if required:
            raise EtherscanError(f"Non-finite numeric field {field}: {value!r}")
        return None
    return number


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def wei_to_native(value: Any) -> Decimal:
    return (parse_decimal(value, field="wei") or ZERO) / WEI


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


def transaction_fee_native(tx: Mapping[str, Any]) -> Decimal:
    gas_used = parse_decimal(tx.get("gasUsed"), field="gasUsed") or ZERO
    gas_price = parse_decimal(tx.get("gasPrice"), field="gasPrice") or ZERO
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

        method_id = str(tx.get("methodId") or "")
        if not method_id:
            input_data = str(tx.get("input") or "")
            method_id = input_data[:10] if input_data.startswith("0x") and len(input_data) >= 10 else ""

        views.append(
            TxView(
                hash=str(tx.get("hash") or ""),
                timestamp=utc_timestamp(tx.get("timeStamp")),
                direction=direction,
                counterparty=counterparty,
                value_native=wei_to_native(tx.get("value")),
                fee_native=transaction_fee_native(tx) if sender == address else ZERO,
                status=transaction_status(tx),
                block_number=safe_int(tx.get("blockNumber")),
                nonce=safe_int(tx.get("nonce")),
                method_id=method_id,
            )
        )
    return views


def summarize(views: Iterable[TxView]) -> dict[str, Decimal | int]:
    received = ZERO
    sent = ZERO
    fees = ZERO
    failed = 0
    successful = 0

    for tx in views:
        fees += tx.fee_native
        if tx.status == "failed":
            failed += 1
            continue
        successful += 1
        if tx.direction == "in":
            received += tx.value_native
        elif tx.direction == "out":
            sent += tx.value_native

    return {
        "received": received,
        "sent": sent,
        "fees": fees,
        "failed": failed,
        "successful": successful,
        "net": received - sent - fees,
    }


def run_parallel(tasks: Mapping[str, Callable[[], T]], workers: int = 3) -> dict[str, Any]:
    if not tasks:
        return {}
    results: dict[str, Any] = {}
    with ThreadPoolExecutor(
        max_workers=min(workers, len(tasks)), thread_name_prefix="etherscan-api"
    ) as pool:
        futures = {pool.submit(func): name for name, func in tasks.items()}
        for future in as_completed(futures):
            name = futures[future]
            try:
                results[name] = future.result()
            except Exception as exc:
                LOG.error("%s request failed: %s", name, exc)
                results[name] = None
    return results


def render_text(
    *,
    address: str,
    chain_id: int,
    native_symbol: str,
    balance: Optional[Decimal],
    eth_price: Optional[Decimal],
    views: list[TxView],
    summary: Mapping[str, Decimal | int],
) -> None:
    unit = native_symbol.upper()
    print(f"\n{APP_NAME.upper()} REPORT")
    print("=" * 76)
    print(f"Address:             {address}")
    print(f"Chain ID:            {chain_id}")
    print(f"Native balance:      {format_native(balance, unit)}")
    print(f"ETH/USD:             {format_usd(eth_price)}")
    print(f"Sample received:     {format_native(summary['received'], unit)}")
    print(f"Sample sent:         {format_native(summary['sent'], unit)}")
    print(f"Sample fees:         {format_native(summary['fees'], unit)}")
    print(f"Sample net movement: {format_native(summary['net'], unit)}")
    print(
        f"Transactions:        {len(views)} "
        f"({summary['successful']} successful, {summary['failed']} failed)"
    )
    print("\nNote: sample totals cover only displayed normal transactions.\n")

    rows = [
        [
            tx.timestamp.replace("T", " ").replace("Z", ""),
            tx.direction,
            tx.status,
            format(tx.value_native, ".8f"),
            format(tx.fee_native, ".8f") if tx.fee_native else "-",
            short_address(tx.counterparty),
            tx.method_id or "-",
            tx.hash[:14] + ("…" if len(tx.hash) > 14 else ""),
        ]
        for tx in views
    ]
    if rows:
        print(
            tabulate(
                rows,
                headers=[
                    "UTC time",
                    "Dir",
                    "Status",
                    f"Value {unit}",
                    f"Fee {unit}",
                    "Counterparty",
                    "Method",
                    "Hash",
                ],
                tablefmt="simple_grid",
            )
        )
    else:
        print("No normal transactions found.")


def format_native(value: Any, symbol: str) -> str:
    if not isinstance(value, Decimal):
        return "N/A"
    return f"{value:.8f} {symbol}"


def format_usd(value: Optional[Decimal]) -> str:
    return "N/A" if value is None else f"${value:,.2f}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze an EVM address through Etherscan API V2.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("address", help="EVM address (0x + 40 hex characters)")
    parser.add_argument("--apikey", default=os.getenv("ETHERSCAN_API_KEY"), help="Etherscan API key")
    parser.add_argument(
        "--base-url",
        default=os.getenv("ETHERSCAN_BASE_URL", DEFAULT_BASE_URL),
        help="Etherscan-compatible API V2 URL",
    )
    parser.add_argument(
        "--chain-id",
        type=positive_int,
        default=env_int("ETHERSCAN_CHAIN_ID", 1),
        help="EVM chain ID",
    )
    parser.add_argument("--native-symbol", default=os.getenv("NATIVE_SYMBOL", "ETH"))
    parser.add_argument("--count", type=non_negative_int, default=10, help="recent normal transactions")
    parser.add_argument("--max-pages", type=positive_int, default=5)
    parser.add_argument("--page-size", type=positive_int, default=100, help="rows per API page")
    parser.add_argument("--rate", type=positive_float, default=3.0, help="maximum API calls per second")
    parser.add_argument("--timeout", type=positive_float, default=12.0, help="read timeout in seconds")
    parser.add_argument("--retries", type=non_negative_int, default=4)
    parser.add_argument("--cache-ttl", type=non_negative_int, default=300, help="transaction cache TTL")
    parser.add_argument("--cache-dir", type=Path, default=Path(".cache_eth"))
    parser.add_argument("--no-cache", action="store_true")
    parser.add_argument("--no-price", action="store_true", help="skip ETH/USD request")
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--version", action="version", version=f"%(prog)s {APP_VERSION}")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    try:
        parser = build_parser()
    except ValidationError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
    )

    if not args.apikey or not args.apikey.strip():
        parser.error("missing API key: use --apikey or ETHERSCAN_API_KEY")

    try:
        address = validate_address(args.address)
        native_symbol = args.native_symbol.strip().upper()
        if not native_symbol or len(native_symbol) > 12:
            raise ValidationError("native symbol must contain 1 to 12 characters")

        base_url = args.base_url.strip()
        if not base_url.startswith(("https://", "http://")):
            raise ValidationError("base URL must start with http:// or https://")

        settings = Settings(
            base_url=base_url,
            chain_id=args.chain_id,
            timeout=args.timeout,
            retries=args.retries,
            rate_limit=args.rate,
            cache_dir=args.cache_dir.expanduser(),
            cache_ttl=args.cache_ttl,
            max_pages=args.max_pages,
            page_size=min(args.page_size, 10000),
            no_cache=args.no_cache,
        )
        client = EtherscanClient(args.apikey.strip(), settings)

        tasks: dict[str, Callable[[], Any]] = {
            "balance": lambda: client.balance(address),
            "transactions": lambda: client.transactions(address, args.count),
        }
        # Etherscan's ethprice endpoint is meaningful for Ethereum. On another chain,
        # request it only when the user explicitly keeps the default behavior in mind.
        if not args.no_price and settings.chain_id == 1:
            tasks["price"] = client.eth_price
        elif not args.no_price:
            LOG.info("Skipping ETH/USD price on chain ID %d; use --no-price to silence", settings.chain_id)

        result = run_parallel(tasks)
        raw_transactions = result.get("transactions")
        if raw_transactions is None:
            raw_transactions = []
        views = normalize_transactions(raw_transactions, address)
        stats = summarize(views)

        if args.json:
            document = {
                "schema_version": 1,
                "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "address": address,
                "chain_id": settings.chain_id,
                "native_symbol": native_symbol,
                "native_balance": decimal_or_none(result.get("balance")),
                "eth_usd": decimal_or_none(result.get("price")),
                "sample": {
                    "transaction_count": len(views),
                    "successful_count": stats["successful"],
                    "failed_count": stats["failed"],
                    "received_native": decimal_text(stats["received"]),
                    "sent_native": decimal_text(stats["sent"]),
                    "fees_native": decimal_text(stats["fees"]),
                    "net_native": decimal_text(stats["net"]),
                },
                "transactions": [tx.to_json() for tx in views],
                "errors": {
                    key: "request failed"
                    for key in ("balance", "transactions", "price")
                    if key in tasks and result.get(key) is None
                },
            }
            print(json.dumps(document, ensure_ascii=False, indent=2))
        else:
            render_text(
                address=address,
                chain_id=settings.chain_id,
                native_symbol=native_symbol,
                balance=result.get("balance"),
                eth_price=result.get("price"),
                views=views,
                summary=stats,
            )

        core_ok = result.get("balance") is not None and result.get("transactions") is not None
        return 0 if core_ok else 2

    except (ValidationError, EtherscanError) as exc:
        LOG.error("%s", exc)
        return 2
    except KeyboardInterrupt:
        LOG.warning("Interrupted")
        return 130


def decimal_or_none(value: Any) -> Optional[str]:
    return decimal_text(value) if isinstance(value, Decimal) else None


if __name__ == "__main__":
    sys.exit(main())
