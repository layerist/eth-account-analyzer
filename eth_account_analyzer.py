import requests
import csv
from datetime import datetime

# Your Etherscan API key
API_KEY = 'YOUR_ETHERSCAN_API_KEY'

# Base URL for API requests
BASE_URL = 'https://api.etherscan.io/api'

def get_eth_balance(address):
    """Get the ETH balance of the specified address."""
    url = f'{BASE_URL}?module=account&action=balance&address={address}&tag=latest&apikey={API_KEY}'
    response = requests.get(url).json()
    balance_wei = int(response['result'])
    balance_eth = balance_wei / 10**18
    return balance_eth

def get_last_transactions(address, count=10):
    """Get the last transactions of the specified address."""
    url = f'{BASE_URL}?module=account&action=txlist&address={address}&startblock=0&endblock=99999999&sort=desc&apikey={API_KEY}'
    response = requests.get(url).json()
    transactions = response['result'][:count]
    return transactions

def get_eth_price():
    """Get the current ETH price in USD."""
    url = f'{BASE_URL}?module=stats&action=ethprice&apikey={API_KEY}'
    response = requests.get(url).json()
    eth_price_usd = float(response['result']['ethusd'])
    return eth_price_usd

def calculate_transaction_totals(transactions):
    """Calculate the total sum of incoming and outgoing transactions."""
    total_incoming = 0
    total_outgoing = 0
    for tx in transactions:
        value_eth = int(tx['value']) / 10**18
        if tx['to'].lower() == address.lower():
            total_incoming += value_eth
        else:
            total_outgoing += value_eth
    return total_incoming, total_outgoing

def save_transactions_to_csv(transactions, filename='transactions.csv'):
    """Save transaction information to a CSV file."""
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
                int(tx['value']) / 10**18,
                tx['gas'],
                tx['gasPrice'],
                tx['input']
            ])

# Example usage
address = 'YOUR_ETH_ADDRESS'

# Get ETH balance
balance = get_eth_balance(address)
print(f'ETH Balance: {balance:.4f} ETH')

# Get last 10 transactions
transactions = get_last_transactions(address)
print(f'Last 10 Transactions: {transactions}')

# Get current ETH price in USD
eth_price = get_eth_price()
print(f'Current ETH Price: ${eth_price:.2f} USD')

# Calculate total sum of incoming and outgoing transactions
total_incoming, total_outgoing = calculate_transaction_totals(transactions)
print(f'Total Incoming Transactions: {total_incoming:.4f} ETH')
print(f'Total Outgoing Transactions: {total_outgoing:.4f} ETH')

# Save transaction information to CSV file
save_transactions_to_csv(transactions)
print(f'Transaction information saved to transactions.csv')
