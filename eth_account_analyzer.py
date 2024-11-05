import requests
import csv
import logging
import argparse
from datetime import datetime
from typing import List, Tuple, Optional, Dict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
BASE_URL = 'https://api.etherscan.io/api'
WEI_TO_ETH = 10**18

def make_request(endpoint: str, params: Dict[str, str]) -> Optional[dict]:
    """Handles HTTP requests and error handling for the Etherscan API."""
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        if data.get('status') != '1':
            logging.error(f"API Error: {data.get('message')}")
            return None
        return data
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        return None

def get_eth_balance(address: str, api_key: str) -> Optional[float]:
    """Fetches the ETH balance of the specified address."""
    params = {
        'module': 'account',
        'action': 'balance',
        'address': address,
        'tag': 'latest',
        'apikey': api_key
    }
    data = make_request('balance', params)
    if data:
        return int(data.get('result', 0)) / WEI_TO_ETH
    return None

def get_last_transactions(address: str, api_key: str, count: int = 10) -> List[dict]:
    """Fetches the latest transactions of the specified address, limited by count."""
    params = {
        'module': 'account',
        'action': 'txlist',
        'address': address,
        'startblock': '0',
        'endblock': '99999999',
        'sort': 'desc',
        'apikey': api_key
    }
    data = make_request('txlist', params)
    return data.get('result', [])[:count] if data else []

def get_eth_price(api_key: str) -> Optional[float]:
    """Fetches the current price of ETH in USD."""
    params = {
        'module': 'stats',
        'action': 'ethprice',
        'apikey': api_key
    }
    data = make_request('ethprice', params)
    if data:
        try:
            return float(data['result'].get('ethusd', 0.0))
        except ValueError as e:
            logging.error(f"Error parsing ETH price: {e}")
    return None

def calculate_transaction_totals(transactions: List[dict], address: str) -> Tuple[float, float]:
    """Calculates total incoming and outgoing ETH based on transactions."""
    total_incoming, total_outgoing = 0.0, 0.0
    address_lower = address.lower()

    for tx in transactions:
        try:
            value_eth = int(tx['value']) / WEI_TO_ETH
            if tx['to'].lower() == address_lower:
                total_incoming += value_eth
            elif tx['from'].lower() == address_lower:
                total_outgoing += value_eth
        except (KeyError, ValueError) as e:
            logging.error(f"Error processing transaction data: {e}")
    return total_incoming, total_outgoing

def save_transactions_to_csv(transactions: List[dict], filename: str) -> None:
    """Saves transaction details to a CSV file."""
    try:
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['TxHash', 'BlockNumber', 'TimeStamp', 'From', 'To', 'Value (ETH)', 'Gas', 'GasPrice', 'Input'])
            for tx in transactions:
                writer.writerow([
                    tx['hash'],
                    tx['blockNumber'],
                    datetime.utcfromtimestamp(int(tx['timeStamp'])),
                    tx['from'],
                    tx['to'],
                    int(tx['value']) / WEI_TO_ETH,
                    tx['gas'],
                    tx['gasPrice'],
                    tx['input']
                ])
        logging.info(f'Transactions successfully saved to {filename}')
    except IOError as e:
        logging.error(f'Error saving transactions to CSV: {e}')

def main():
    parser = argparse.ArgumentParser(description='Fetch and analyze Ethereum transactions.')
    parser.add_argument('address', type=str, help='Ethereum address')
    parser.add_argument('apikey', type=str, help='Etherscan API key')
    parser.add_argument('--count', type=int, default=10, help='Number of transactions to fetch')
    parser.add_argument('--csv', type=str, default='transactions.csv', help='Filename to save transactions')
    args = parser.parse_args()

    # Fetch ETH balance
    balance = get_eth_balance(args.address, args.apikey)
    if balance is not None:
        logging.info(f'ETH Balance: {balance:.4f} ETH')
    else:
        logging.warning(f"Failed to fetch balance for address: {args.address}")

    # Fetch recent transactions
    transactions = get_last_transactions(args.address, args.apikey, args.count)
    if transactions:
        logging.info(f'Fetched {len(transactions)} transactions.')
    else:
        logging.warning(f"No transactions found for address: {args.address}")

    # Fetch ETH price
    eth_price = get_eth_price(args.apikey)
    if eth_price is not None:
        logging.info(f'Current ETH Price: ${eth_price:.2f} USD')
    else:
        logging.warning("Failed to fetch ETH price.")

    # Calculate total incoming/outgoing ETH
    if transactions:
        total_incoming, total_outgoing = calculate_transaction_totals(transactions, args.address)
        logging.info(f'Total Incoming: {total_incoming:.4f} ETH, Total Outgoing: {total_outgoing:.4f} ETH')

        # Save transactions to CSV
        save_transactions_to_csv(transactions, args.csv)

if __name__ == '__main__':
    main()
