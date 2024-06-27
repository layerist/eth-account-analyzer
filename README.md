#### Project: ETH Account Analyzer

A Python program that interacts with the Etherscan API to fetch and analyze Ethereum account data.

---

#### Features

- Retrieve the ETH balance of a specified address.
- Fetch the last 10 transactions of a specified address.
- Get the current price of ETH in USD.
- Calculate the total sum of incoming and outgoing transactions.
- Save transaction information to a CSV file.

---

#### Prerequisites

- Python 3.x
- `requests` library

Install the `requests` library using pip:

```bash
pip install requests
```

---

#### Usage

1. **Clone the repository**:

    ```bash
    git clone https://github.com/layerist/eth-account-analyzer.git
    cd eth-account-analyzer
    ```

2. **Replace placeholders**:

    - Replace `YOUR_ETHERSCAN_API_KEY` with your actual Etherscan API key.
    - Replace `YOUR_ETH_ADDRESS` with the Ethereum address you want to analyze.

3. **Run the program**:

    ```bash
    python eth_account_analyzer.py
    ```

4. **Example Output**:

    ```
    ETH Balance: 1.2345 ETH
    Last 10 Transactions: [{'blockNumber': '1234567', 'timeStamp': '1609459200', ...}, ...]
    Current ETH Price: $2000.00 USD
    Total Incoming Transactions: 1.2345 ETH
    Total Outgoing Transactions: 0.5678 ETH
    Transaction information saved to transactions.csv
    ```

---

#### Functions

- `get_eth_balance(address)`: Returns the ETH balance of the specified address.
- `get_last_transactions(address, count=10)`: Returns the last `count` transactions of the specified address.
- `get_eth_price()`: Returns the current ETH price in USD.
- `calculate_transaction_totals(transactions)`: Calculates the total sum of incoming and outgoing transactions.
- `save_transactions_to_csv(transactions, filename='transactions.csv')`: Saves transaction information to a CSV file.

---

#### Contributing

Contributions are welcome! Please submit a pull request or open an issue to discuss what you would like to change.

---

#### License

This project is licensed under the MIT License.

---

#### Contact

For any questions or issues, please open an issue on GitHub.
