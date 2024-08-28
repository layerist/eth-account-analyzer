import requests
import csv
import logging
import argparse
from datetime import datetime
from typing import List, Tuple, Optional

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Base URL for API requests
BASE_URL = 'https://api.etherscan.io/api'
WEI_TO_ETH = 10**18

def get_eth_balance(address: str, api_key: str) -> Optional[float]:
    """Get the ETH balance of the specified address."""
    url = f'{BASE_URL}?module=account&action=balance&address={address}&tag=latest&apikey={api_key}'
    try:
        response = requests.get(url)
        response.raise_for_status()
        result = response.json().get('result')
        if result is None:
            logging.error('No result found in response when fetching ETH balance.')
            return None
        balance_wei = int(result)
        return balance_wei / WEI_TO_ETH
    except requests.exceptions.RequestException as e:
        logging.error(f'Error fetching ETH balance: {e}')
    except ValueError as e:
        logging.error(f'Error parsing ETH balance: {e}')
    return None

def get_last_transactions(address: str, api_key: str, count: int = 10) -> List[dict]:
    """Get the last transactions of the specified address."""
    url = f'{BASE_URL}?module=account&action=txlist&address={address}&startblock=0&endblock=99999999&sort=desc&apikey={api_key}'
    try:
        response = requests.get(url)
        response.raise_for_status()
        result = response.json().get('result')
        if not isinstance(result, list):
            logging.error('Unexpected result format when fetching transactions.')
            return []
        return result[:count]
    except requests.exceptions.RequestException as e:
        logging.error(f'Error fetching transactions: {e}')
    except ValueError as e:
        logging.error(f'Error parsing transactions: {e}')
    return []

def get_eth_price(api_key: str) -> Optional[float]:
    """Get the current ETH price in USD."""
    url = f'{BASE_URL}?module=stats&action=ethprice&apikey={api_key}'
    try:
        response = requests.get(url)
        response.raise_for_status()
        result = response.json().get('result')
        if not result or 'ethusd' not in result:
            logging.error('Unexpected result format when fetching ETH price.')
            return None
        return float(result.get('ethusd', 0))
    except requests.exceptions.RequestException as e:
        logging.error(f'Error fetching ETH price: {e}')
    except ValueError as e:
        logging.error(f'Error parsing ETH price: {e}')
    return None

def calculate_transaction_totals(transactions: List[dict], address: str) -> Tuple[float, float]:
    """Calculate the total sum of incoming and outgoing transactions."""
    total_incoming = 0.0
    total_outgoing = 0.0
    for tx in transactions:
        try:
            value_eth = int(tx['value']) / WEI_TO_ETH
            if tx['to'].lower() == address.lower():
                total_incoming += value_eth
            else:
                total_outgoing += value_eth
        except KeyError as e:
            logging.error(f'Missing expected transaction key: {e}')
    return total_incoming, total_outgoing

def save_transactions_to_csv(transactions: List[dict], filename: str = 'transactions.csv') -> None:
    """Save transaction information to a CSV file."""
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
        logging.info(f'Transaction information saved to {filename}')
    except IOError as e:
        logging.error(f'Error saving transactions to CSV: {e}')

def main():
    parser = argparse.ArgumentParser(description='Fetch and analyze ETH transactions.')
    parser.add_argument('address', type=str, help='Ethereum address')
    parser.add_argument('apikey', type=str, help='Etherscan API key')
    args = parser.parse_args()

    address = args.address
    api_key = args.apikey

    # Get ETH balance
    balance = get_eth_balance(address, api_key)
    if balance is not None:
        logging.info(f'ETH Balance: {balance:.4f} ETH')

    # Get last 10 transactions
    transactions = get_last_transactions(address, api_key)
    if transactions:
        logging.info(f'Fetched {len(transactions)} transactions.')

    # Get current ETH price in USD
    eth_price = get_eth_price(api_key)
    if eth_price is not None:
        logging.info(f'Current ETH Price: ${eth_price:.2f} USD')

    # Calculate total sum of incoming and outgoing transactions
    total_incoming, total_outgoing = calculate_transaction_totals(transactions, address)
    logging.info(f'Total Incoming Transactions: {total_incoming:.4f} ETH')
    logging.info(f'Total Outgoing Transactions: {total_outgoing:.4f} ETH')

    # Save transaction information to CSV file
    save_transactions_to_csv(transactions)

if __name__ == '__main__':
    main()
