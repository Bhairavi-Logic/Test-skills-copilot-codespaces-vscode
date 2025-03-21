import pandas
import requests
import csv
from datetime import datetime
from collections import deque, defaultdict

def fetch_transactions_from_etherscan(wallet_address, api_key):
    url = f"https://api.etherscan.io/api?module=account&action=txlist&address={wallet_address}&startblock=0&endblock=99999999&sort=asc&apikey={api_key}"
    response = requests.get(url)
    data = response.json()
    return data.get("result", [])

def fetch_internal_transactions(wallet_address, api_key):
    url = f"https://api.etherscan.io/api?module=account&action=txlistinternal&address={wallet_address}&startblock=0&endblock=99999999&sort=asc&apikey={api_key}"
    response = requests.get(url)
    data = response.json()
    return data.get("result", [])

def fetch_token_transfers(wallet_address, api_key):
    url = f"https://api.etherscan.io/api?module=account&action=tokentx&address={wallet_address}&startblock=0&endblock=99999999&sort=asc&apikey={api_key}"
    response = requests.get(url)
    data = response.json()
    return data.get("result", [])

def fetch_eth_price(timestamp):
    date = datetime.utcfromtimestamp(int(timestamp)).strftime('%d-%m-%Y')
    url = f"https://api.coingecko.com/api/v3/coins/ethereum/history?date={date}&localization=false"
    url = f"https://api.binance.com/api/v3/klines?symbol=ETHUSDT&interval=1m&startTime=timestamp&endTime=timestamp"
    response = requests.get(url)
    return response.json().get("market_data", {}).get("current_price", {}).get("aud", 0)


def get_eth_price_BNCE(timestamp):
    # Convert timestamp to milliseconds (Binance API uses ms)
    timestamp_ms = timestamp * 1000

    # Binance API endpoint for 1-minute historical data
    url = f"https://api.binance.com/api/v3/klines?symbol=ETHUSDT&interval=1m&startTime={timestamp_ms}&limit=1"

    try:
        response = requests.get(url)
        data = response.json()

        if not data:
            return "No data available for this timestamp."

        # Binance response format: [open_time, open, high, low, close, volume, ...]
        open_time, open_price, high, low, close_price, *_ = data[0]

        # Convert open_time back to readable format
        open_time_dt = datetime.utcfromtimestamp(open_time / 1000).strftime('%Y-%m-%d %H:%M:%S UTC')

        return {
            "timestamp": timestamp,
            "datetime_utc": open_time_dt,
            "open": float(open_price),
            "high": float(high),
            "low": float(low),
            "close": float(close_price)
        }

    except Exception as e:
        return f"Error fetching data: {e}"


def sort_transactions_by_block(transactions):
    return sorted(transactions, key=lambda tx: int(tx["hash"]))



def calculate_cost_basis(transactions, method="FIFO"):
    cost_queues = {"FIFO": {}, "LIFO": {}, "WAC": {}}
    running_balance = {}

    for key, tx in transactions.items():
        running_balance[tx["inToken"]] = tx["inTokenValue"]

        tx["fy"] = datetime.utcfromtimestamp(int(tx["timeStamp"])).year
        long_term = short_term = 0
        profit = 0
        transaction_type = tx["action"]

        eth_price = tx["ETH_Price"]
        inToken = tx["inToken"]

        if "deposit" in  transaction_type:
            if inToken not in cost_queues["FIFO"]:
                cost_queues["FIFO"][inToken] = deque()

            cost_queues["FIFO"][inToken].append((tx["inTokenValue"], 0, tx["timeStamp"]))
            running_balance[inToken] += tx["inTokenValue"]

        if "buy order" in  transaction_type:
            if inToken not in cost_queues["FIFO"]:
                cost_queues["FIFO"][inToken] = deque()

            cost_queues["FIFO"][inToken].append((tx["inTokenValue"], tx["inTokenValue"]*eth_price, tx["timeStamp"]))
            running_balance[inToken] += tx["inTokenValue"]


        if transaction_type in ["sell Order", "withdrawal", "sold to KK"]:

            outToken = tx["outToken"]
            sell_qty = tx["outTokenValue"]
            cost = 0
            remaining_qty = sell_qty
            cost_basis, long_term_basis, short_term_basis = 0, 0, 0
            if method == "FIFO":

                while remaining_qty > 0 and cost_queues["FIFO"].get(outToken):
                    qty, cost, purchase_date = cost_queues["FIFO"][outToken].popleft()
                    holding_period = (int(tx["timeStamp"]) - int(purchase_date)) / (60 * 60 * 24 * 365)
                    taken_qty = min(qty, remaining_qty)
                    cost_per_unit = cost / qty
                    cost_basis += cost_per_unit * taken_qty

                    if holding_period > 365:
                        long_term_basis += cost_per_unit * taken_qty
                        remaining_qty -= taken_qty
                    else:
                        short_term_basis += cost_per_unit * taken_qty
                        remaining_qty -= taken_qty

                    if qty > taken_qty:
                        cost_queues["FIFO"][outToken].appendleft(
                            (qty - taken_qty, cost - (cost_per_unit * taken_qty), purchase_date))

                    AUD_factor = 1
                    #if to_token != "AUD":
                        #AUD_factor = get_btc_price_at(str(from_currency_row.creationTime),
                        #                              symbol=to_currency + "AUD").pop()

                    amount_received = tx["outTokenValue"]
                    trading_fee = tx["gasFee"]
                    profit = (amount_received * AUD_factor) - cost_basis - (trading_fee * AUD_factor)
                    long_term_gains = (amount_received * AUD_factor) - long_term_basis - (
                        trading_fee if not short_term_basis else 0 * AUD_factor) if long_term_basis else 0
                    short_term_gains = (amount_received * AUD_factor) - short_term_basis - (
                                trading_fee * AUD_factor) if short_term_basis else 0

                    running_balance[outToken] -= tx["outTokenValue"]
                    running_balance[inToken] += tx["inTokenValue"]
                    running_balance["ETH"] -= tx["gasFee"]

                    tx["profit"] = profit
                    tx["long_term_capital_gains"] = long_term_gains
                    tx["short_term_capital_gains"] = short_term_gains

        #capital_gains.append(tx)
    capital_gains = transactions
    return capital_gains

def write_csv(transactions, filename="eth_tax_report.csv"):
    with open(filename, "w", newline="") as file:
        fieldnames = ["date", "in_transaction_type", "asset_in", "amount_paid", "quantity_in", "buy_trading_fee", "out_transaction_type", "asset_out", "quantity_out", "amount_received", "sell_trading_fee", "profit", "balance_qty", "AUD_balance", "FY", "Long Term Capital Gains", "Short Term Capital Gains", "method_caption"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(transactions)

def write_all_transactions_csv(transactions, filename="ETH_Transactions.csv"):
    if transactions:
        keys = {key for tx in transactions for key in tx.keys()}  # Collect all unique keys
        with open(filename, "w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=keys)
            writer.writeheader()
            for tx in transactions:
                filtered_tx = {key: tx.get(key, "") for key in keys}  # Ensure only expected fields are written
                writer.writerow(filtered_tx)

def write_all_transactions_csv1(transactions, filename="ETH_Transactions.csv"):
    fieldnames = set()
    for tx in transactions.values():
        if isinstance(tx, dict):  # Ensure values are dictionaries
            fieldnames.update(tx.keys())

    # Convert set to sorted list (optional, for consistent column order)
    fieldnames = sorted(fieldnames)

    # Write to CSV
    with open(filename, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()  # Write column names
        writer.writerows(transactions.values())  # Write all transaction rows


def get_internal_transaction(internal_transactions, hash):
    for it in internal_transactions:
        if hash == it["hash"]:
            return it

    return None
def get_transaction(transactions, hash):
    for tx in transactions:
        if hash == tx["hash"]:
            return tx

    return None

def get_token_transfers(token_transfers, hash):
    for tt in token_transfers:
        if hash == tt["hash"]:
            return tt

    return None


def get_unique_hash(transactions):
    seen = set()
    unique_hashes = []

    for tx in transactions:
        if "hash" in tx and tx["hash"] not in seen:
            seen.add(tx["hash"])
            unique_hashes.append(tx["hash"])

    return unique_hashes

import ccxt
from datetime import datetime, timezone
def get_eth_price(timestamp, exchange_name='binance'):
    # Convert timestamp to milliseconds (Binance API uses ms)
    timestamp_ms = int(timestamp * 1000)

    # Initialize exchange
    exchange = getattr(ccxt, exchange_name)()

    # Fetch OHLCV (Open-High-Low-Close-Volume) data
    ohlcv = exchange.fetch_ohlcv('ETH/USDT', '1m', since=timestamp_ms, limit=1)

    if not ohlcv:
        return "No data found for the given timestamp."

    # Extract relevant price (Open price at that minute)
    open_time, open_price, high, low, close, volume = ohlcv[0]

    return {
        "timestamp": datetime.fromtimestamp(open_time / 1000, tz=timezone.utc),
        "open_price": open_price,
        "high": high,
        "low": low,
        "close_price": close,
        "volume": volume
    }

def get_token_transfers(token_transfers, hash):
    for tt in token_transfers:
        if hash == tt["hash"]:
            return tt

    return None

def get_to_from_token_transfers (token_transfers, hash, wallet_address):
    from_token = None
    to_token = None

    for tt in token_transfers:
        if hash == tt["hash"] and str(tt["to"]).upper() == wallet_address.upper():
            to_token =  tt
        if hash == tt["hash"] and str(tt["from"]).upper() == wallet_address.upper():
            from_token = tt

    return from_token, to_token

if __name__ == "__main__":
    API_KEY = "W4CZ4A9B73J8EHY63XZI79G19A8JTHERWI"
    WALLET_ADDRESSES = ["0xc76cd22bd674dc4707156c5e13b9ba5bb5921d2a", "0xA9203c8ed31D2C2Bf836338BA31A35081029743D"]  # List of multiple wallet addresses
    WALLET_NAMES = ["Metamask_ETH", "Trust_ETH"]

    WALLET_ADDRESSES = ["0xA9203c8ed31D2C2Bf836338BA31A35081029743D"]  # List of multiple wallet addresses
    WALLET_NAMES = ["Trust_ETH"]

    KK_Wallet = "0x2da83ace1da226f2afe337db28dcd0bd8d97b23d"


    all_transactions = [
        {**tx, 'wallet_name': wallet_name}
        for wallet, wallet_name in zip(WALLET_ADDRESSES, WALLET_NAMES)
        for tx in fetch_transactions_from_etherscan(wallet, API_KEY) +
                  fetch_token_transfers(wallet, API_KEY) +
                  fetch_internal_transactions(wallet, API_KEY)
    ]
    write_all_transactions_csv(all_transactions)


    for wallet, wallet_name in zip(WALLET_ADDRESSES, WALLET_NAMES):
        transactions = fetch_transactions_from_etherscan(wallet, API_KEY)
        token_transfers = fetch_token_transfers(wallet, API_KEY)
        internal_transactions = fetch_internal_transactions(wallet, API_KEY)

    all_transactions = {}
    balance = defaultdict(float)

    at = sorted(transactions + internal_transactions + token_transfers, key=lambda tx: int(tx.get("blockNumber", 0)))
    unique_hash = get_unique_hash(at)
    #at1.sort()
    #unique_hash =  get_unique_hash(sorted(transactions + internal_transactions + token_transfers, key=lambda tx: int(tx.get("blockNumber", 0))))

    #unique_hash =  sort_transactions_by_block(get_unique_hash(transactions + internal_transactions + token_transfers))
    for hash in unique_hash:

        print(hash)
        tx_type = None
        tx = get_transaction(transactions, hash)
        if tx == None:
            tx = get_token_transfers(token_transfers, hash)
            tx_type = "TT" #Token Transfer
        else:
            tx_type = "NT"  #Normal Transaction


        #if tx_type =="NT" and tx["isError"] == "1":
        #    continue

        all_transactions[tx["hash"]]= {
        "wallet_address" : WALLET_ADDRESSES[0],
        "blockNumber" : tx["blockNumber"],
        "timeStamp" : tx["timeStamp" ],
        "hash" : tx["hash" ],
        "nonce" : tx["nonce" ],
        "blockHash" : tx["blockHash" ],
        "transactionIndex" : tx["transactionIndex" ],
        "from" : tx["from" ],
        "outToken": "ETH" if tx_type == "NT" else "",
        "outTokenDecimal": "18" if tx_type == "NT" else "",
        "outTokenValue": '',
        "to" : tx["to"],
        "inToken": "ETH" if tx_type == "NT" else "",
        "inTokenValue": 0,
        "inTokenDecimal": "" ,
        "gas" : tx["gas" ],
        "gasPrice" : tx["gasPrice"],
        "isError" : tx["isError"] if tx_type == "NT" else 0,
        "txreceipt_status" : tx["txreceipt_status"] if tx_type == "NT" else 1,
        "input" : tx["input" ],
        "contractAddress" : tx["contractAddress"],
        "cumulativeGasUsed" : tx["cumulativeGasUsed"],
        "gasUsed" : tx["gasUsed"],
        "confirmations" : tx["confirmations"],
        "methodId" : tx["methodId"] if tx_type == "NT" else "NA:Token_Transfer" ,
        "functionName" : tx["functionName"] if tx_type == "NT" and tx["functionName"] != "" else "NA:Token_Transfer" ,
        "hasTokenTransferRec" : False,
        "hasInternalTransaction": False,
        "profit": 0,
        "short_term_capital_gains":0,
        "long_term_capital_gains": 0
        }

        #if Wallet address is in from then something is going out.
        if str(tx["from"]).upper() == str(WALLET_ADDRESSES[0]).upper():
            all_transactions[tx["hash"]]["direction"] = "Out"
        else:
            all_transactions[tx["hash"]]["direction"] = "In"

        # if method ids are as below in out direction then they are withdrawals
        if all_transactions[tx["hash"]]["direction"] == "Out" and all_transactions[tx["hash"]]["methodId"] in ["0xa9059cbb", "0x"]:
            # withdraw or sold
            all_transactions[tx["hash"]]["action"] = "withdraw" if all_transactions[tx["hash"]][
                                                                              "to"].upper() != KK_Wallet.upper() else "sold to KK"
        elif all_transactions[tx["hash"]]["direction"] == "In":
            if all_transactions[tx["hash"]]["methodId"] in ["0xa9059cbb", "0x"]:
                all_transactions[tx["hash"]]["action"] = "deposit ETH"
                all_transactions[tx["hash"]]["outToken"] = ""
                all_transactions[tx["hash"]]["outTokenDecimal"] = ""
            elif all_transactions[tx["hash"]]["methodId"] in ["NA:Token_Transfer"]:
                all_transactions[tx["hash"]]["action"] = "deposit"

        print(all_transactions[tx["hash"]]["functionName"])
        # if function name is SWAP and direction is out then buy order
        if all_transactions[tx["hash"]]["direction"] == "Out" and "SWAP" in str(all_transactions[tx["hash"]]["functionName"]).upper():
            all_transactions[tx["hash"]]["action"] = "buy order"

        if str(all_transactions[tx["hash"]]["from"]).upper() == str("0xa5025faba6e70b84f74e9b1113e5f7f4e7f4859f").upper():
            print("SPI token swap")
            all_transactions[tx["hash"]]["direction"] = "SPI TokenSwap"
            all_transactions[tx["hash"]]["action"] = "SPI TokenSwap"

            all_transactions[tx["hash"]]["outToken"] = "SPI"
            all_transactions[tx["hash"]]["outTokenValue"] = balance[all_transactions[tx["hash"]]["outToken"]]
            all_transactions[tx["hash"]]["outTokenDecimal"] = "0"
            all_transactions[tx["hash"]]["inToken"] = "SHOP"
            all_transactions[tx["hash"]]["inTokenValue"] = tx["value"]
            all_transactions[tx["hash"]]["inTokenDecimal"] = "18"
        elif "processRouteWithTransferValueOutput" in all_transactions[tx["hash"]]["functionName"]:
            print("token swap")
            all_transactions[tx["hash"]]["direction"] = "TokenSwap"
            all_transactions[tx["hash"]]["action"] = "TokenSwap"
            from_token = None
            to_token = None
            tt1 = get_token_transfers(token_transfers, tx["hash"])
            if tx["hash"] == tt1["hash"] and str(tt1["from"]).upper() == str(WALLET_ADDRESSES[0]).upper():
                from_token = tt1
            if tx["hash"] == tt1["hash"] and str(tt1["to"]).upper() == str(WALLET_ADDRESSES[0]).upper():
                to_token = tt1

            from_token,to_token =  get_to_from_token_transfers (token_transfers,tx["hash"], str(WALLET_ADDRESSES[0]).upper())

            all_transactions[to_token["hash"]]["outToken"] = from_token["tokenSymbol"]
            all_transactions[to_token["hash"]]["outTokenValue"] = from_token["value"]
            all_transactions[to_token["hash"]]["outTokenDecimal"] = from_token["tokenDecimal"]
            all_transactions[to_token["hash"]]["inToken"] = to_token["tokenSymbol"]
            all_transactions[to_token["hash"]]["inTokenValue"] = to_token["value"]
            all_transactions[to_token["hash"]]["inTokenDecimal"] = to_token["tokenDecimal"]
            #tt["used"] = True


        elif "call" in all_transactions[tx["hash"]]["functionName"]:
            print("sale")
            all_transactions[tx["hash"]]["direction"] = "Out"
            all_transactions[tx["hash"]]["action"] = "Sell Order"
            from_token = None
            to_token = None
            tt1 = get_token_transfers(token_transfers, tx["hash"])
            if tx["hash"] == tt1["hash"] and str(tt1["from"]).upper() == str(WALLET_ADDRESSES[0]).upper():
                from_token = tt1
            if tx["hash"] == tt1["hash"] and str(tt1["to"]).upper() == str(WALLET_ADDRESSES[0]).upper():
                to_token = tt1

            it1 = get_internal_transaction(internal_transactions, tx["hash"])
            if tx["hash"] == it1["hash"] and str(it1["from"]).upper() == str(WALLET_ADDRESSES[0]).upper():
                from_token = it1
            if tx["hash"] == it1["hash"] and str(it1["to"]).upper() == str(WALLET_ADDRESSES[0]).upper():
                to_token = it1

            all_transactions[to_token["hash"]]["outToken"] = from_token["tokenSymbol"]
            all_transactions[to_token["hash"]]["outTokenValue"] = from_token["value"]
            all_transactions[to_token["hash"]]["outTokenDecimal"] = from_token["tokenDecimal"]
            all_transactions[to_token["hash"]]["inToken"] = "ETH"
            all_transactions[to_token["hash"]]["inTokenValue"] = to_token["value"]

            tt["used"] = True

        elif "approve" in all_transactions[tx["hash"]]["functionName"] or all_transactions[tx["hash"]]["isError"] == "1":
            all_transactions[tx["hash"]]["direction"] = "Fees Only"
            all_transactions[tx["hash"]]["action"] = "Fees Only"

        else:
            tt = get_token_transfers(token_transfers, tx["hash"])
            if tt != None:
                if str(tt["from"]).upper() == str(WALLET_ADDRESSES[0]).upper():
                    all_transactions[tt["hash"]]["hasTokenTransferRec"] = True
                    all_transactions[tt["hash"]]["outToken"] = tt["tokenSymbol"]
                    all_transactions[tt["hash"]]["outTokenValue"] = tt["value"]
                    all_transactions[tt["hash"]]["outTokenDecimal"] = tt["tokenDecimal"]
                    if tt["tokenSymbol"] != "ETH":
                        #all_transactions[tx["hash"]]["direction"] = "Out"
                        #all_transactions[tx["hash"]]["action"] = "withdraw" if tx["methodId"] in ["0xa9059cbb", "0x"] else "Sell Order"

                        it = get_internal_transaction(internal_transactions,tt["hash"] )
                        if it != None:
                            all_transactions[it["hash"]]["hasInternalTxRec"] = True
                            it["used"] = True
                            if str(it["from"]).upper() == str(WALLET_ADDRESSES[0]).upper():
                                all_transactions[tt["hash"]]["outToken"] = it["tokenSymbol"]
                                all_transactions[it["hash"]]["outTokenValue"] = it["value"]
                            elif str(it["to"]).upper() == str(WALLET_ADDRESSES[0]).upper():
                                all_transactions[tt["hash"]]["inToken"] = "ETH"
                                all_transactions[it["hash"]]["inTokenValue"] = it["value"]

                elif str(tt["to"]).upper() == str(WALLET_ADDRESSES[0]).upper():
                    all_transactions[tt["hash"]]["hasTokenTransferRec"] = True
                    all_transactions[tt["hash"]]["inToken"] = tt["tokenSymbol"]
                    all_transactions[tt["hash"]]["inTokenValue"] = tt["value"]
                    all_transactions[tt["hash"]]["inTokenDecimal"] = tt["tokenDecimal"]
                    all_transactions[tx["hash"]]["outTokenValue"] = tx["value"]
                    all_transactions[tx["hash"]]["outTokenDecimal"] = "18"

                    tt["used"] = True
            else:
                if all_transactions[tx["hash"]]["direction"] == "Out" and all_transactions[tx["hash"]]["methodId"] in ["0x"]:
                    all_transactions[tx["hash"]]["outTokenValue"] = tx["value"]
                    all_transactions[tx["hash"]]["outTokenDecimal"] = "18"

                elif all_transactions[tx["hash"]]["direction"] == "In" and all_transactions[tx["hash"]]["methodId"] in ["0x"]:
                    all_transactions[tx["hash"]]["inTokenValue"] = tx["value"]
                    all_transactions[tx["hash"]]["inTokenDecimal"] = "18"


        if all_transactions[tx["hash"]]["outTokenValue"] != '':
            #Todo: decimal should come from a different logic.
            decimals = 18 if all_transactions[tx["hash"]]["outTokenDecimal"] == '' else all_transactions[tx["hash"]]["outTokenDecimal"]
            denominator = (10 ** int(decimals))
            all_transactions[tx["hash"]]["outTokenValue"] = float(all_transactions[tx["hash"]]["outTokenValue"]) / denominator
        else:
            all_transactions[tx["hash"]]["outTokenValue"] = 0

        decimals = all_transactions[tx["hash"]]["inTokenDecimal"]
        decimals = 18 if decimals == "" else decimals
        denominator = (10 ** int(decimals))
        if all_transactions[tx["hash"]]["inTokenValue"] != 0:
            all_transactions[tx["hash"]]["inTokenValue"] = float(all_transactions[tx["hash"]]["inTokenValue"]) / denominator

        all_transactions[tx["hash"]]["date_time"] = datetime.utcfromtimestamp(int(tx["timeStamp"]))

        all_transactions[tx["hash"]]["gasFee"] = float(tx.get("gasPrice", 0)) * float(tx.get("gasUsed", 0)) / 1e18

        balance["ETH"] -= all_transactions[tx["hash"]]["gasFee"] if all_transactions[tx["hash"]]["direction"] in ["Out","Fees Only","TokenSwap"] else 0
        # balance["ETH"] -= all_transactions[tx["hash"]]["gasFee"]

        balance[all_transactions[tx["hash"]]["outToken"]] -= all_transactions[tx["hash"]]["outTokenValue"]
        balance[all_transactions[tx["hash"]]["inToken"]] += all_transactions[tx["hash"]][
            "inTokenValue"]
        if balance["ETH"] < 0.000000000000000001:
            balance["ETH"] = 0

        if all_transactions[tx["hash"]]["outToken"] !='':
            all_transactions[tx["hash"]]["outTokenBalance"] = balance[all_transactions[tx["hash"]]["outToken"]]

        if all_transactions[tx["hash"]]["inToken"] != '':
            all_transactions[tx["hash"]]["inTokenBalance"] = balance[all_transactions[tx["hash"]]["inToken"]]

        all_transactions[tx["hash"]]["ETH_Balance"] = balance["ETH"]
        all_transactions[tx["hash"]]["ETH_Price"] = get_eth_price(int(all_transactions[tx["hash"]]["timeStamp"]))["open_price"]

    write_all_transactions_csv1(all_transactions, "All_ETH_Transactions.csv")



    df = pandas.DataFrame(all_transactions)

    capital_gains = calculate_cost_basis(all_transactions, method="FIFO")
    write_all_transactions_csv1(capital_gains)

    print("CSV report generated successfully!")
