import requests
import csv
import logging
import argparse
from datetime import datetime
from typing import List, Tuple, Optional

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
BASE_URL = 'https://api.etherscan.io/api'
WEI_TO_ETH = 10**18

def make_request(endpoint: str, params: dict) -> Optional[dict]:
    """Handles HTTP requests and error handling for the Etherscan API."""
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        if data.get('status') != '1':
            logging.error(f"API Error: {data.get('message')}")
            return None
        return data
    except (requests.RequestException, ValueError) as e:
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
        try:
            return int(data['result']) / WEI_TO_ETH
        except (KeyError, ValueError):
            logging.error("Error processing balance data.")
    return None

def get_last_transactions(address: str, api_key: str, count: int = 10) -> List[dict]:
    """Fetches the latest transactions of the specified address, limited by count."""
    params = {
        'module': 'account',
        'action': 'txlist',
        'address': address,
        'startblock': 0,
        'endblock': 99999999,
        'sort': 'desc',
        'apikey': api_key
    }
    data = make_request('txlist', params)
    if data:
        return data.get('result', [])[:count]
    return []

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
            return float(data['result']['ethusd'])
        except (KeyError, ValueError):
            logging.error("Error processing ETH price data.")
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
            else:
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
                    datetime.fromtimestamp(int(tx['timeStamp'])),
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

    address = args.address
    api_key = args.apikey
    count = args.count
    csv_filename = args.csv

    # Fetch ETH balance
    balance = get_eth_balance(address, api_key)
    if balance is not None:
        logging.info(f'ETH Balance: {balance:.4f} ETH')

    # Fetch recent transactions
    transactions = get_last_transactions(address, api_key, count)
    if transactions:
        logging.info(f'Fetched {len(transactions)} transactions.')

    # Fetch ETH price
    eth_price = get_eth_price(api_key)
    if eth_price is not None:
        logging.info(f'Current ETH Price: ${eth_price:.2f} USD')

    # Calculate total incoming/outgoing ETH
    total_incoming, total_outgoing = calculate_transaction_totals(transactions, address)
    logging.info(f'Total Incoming: {total_incoming:.4f} ETH, Total Outgoing: {total_outgoing:.4f} ETH')

    # Save transactions to CSV
    save_transactions_to_csv(transactions, csv_filename)

if __name__ == '__main__':
    main()
