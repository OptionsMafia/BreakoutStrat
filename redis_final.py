'''
trading_system.py - Optimized trading system using pre-calculated resampled data

'''
import redis
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import json
import sys
import logging
import threading
import ray
import pytz
import pyotp
import requests
from urllib.parse import parse_qs,urlparse
import traceback
import hashlib

# CHANGE MARKET TIME IN IS_MARKET_OPEN() AND EXIT TIME IN STRATEGY()
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("trading_system.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("TradingSystem")

def get_date_prefix():
    """Get date prefix in the format used by Redis keys"""
    now = datetime.now()
    day = now.day
    month = now.strftime("%b").lower()
    year = now.strftime("%Y")
    return f"{day}{month}{year}"

# Constants
RAW_DB = 0  # Database index for raw ticker data with current prices
RESAMPLED_DB = 1  # Database index for resampled data with pivots and EMAs

# Get current date prefixes for Redis keys
RAW_PREFIX = get_date_prefix()
RESAMPLED_PREFIX = get_date_prefix() + "resampled"

############ FLATTRADE EXECUTION START ################

APIKEY='9d87fcbbb8eb47b6b6d577acf3882266'
secretKey = '2025.09e4f220bec24ced931170e7ee7ba611c8517eb1705b65ac'
totp_key='F4A3KMU5W4L6P6IQ2LV6J467S4VQTA7Q'
password = 'Godmode@6'
userid = 'FZ11934'
headerJson =  {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.74 Safari/537.36", "Referer":"https://auth.flattrade.in/"}



ses = requests.Session()
sesUrl = 'https://authapi.flattrade.in/auth/session'
passwordEncrpted =  hashlib.sha256(password.encode()).hexdigest()
ses = requests.Session()

res_pin = ses.post(sesUrl,headers=headerJson)
sid = res_pin.text
url2 = 'https://authapi.flattrade.in/ftauth'
payload = {"UserName":userid,"Password":passwordEncrpted,"PAN_DOB":pyotp.TOTP(totp_key).now(),"App":"","ClientID":"","Key":"","APIKey":APIKEY,"Sid":sid,
          "Override":"Y","Source":"AUTHPAGE"}
res2 = ses.post(url2, json=payload)
reqcodeRes = res2.json()
parsed = urlparse(reqcodeRes['RedirectURL'])  
reqCode = parse_qs(parsed.query)['code'][0]
api_secret =APIKEY+ reqCode + secretKey 
api_secret =  hashlib.sha256(api_secret.encode()).hexdigest()
payload = {"api_key":APIKEY, "request_code":reqCode, "api_secret":api_secret}
url3 = 'https://authapi.flattrade.in/trade/apitoken'  
res3 = ses.post(url3, json=payload)
token = res3.json()['token']
token


############ FLATTRADE EXECUTION END ################

# Replace with these simple global variables:
total_runs = 0
last_print_time = datetime.now()
last_print_active_trades_time = datetime.now()
active_trades_count = 0

used_pivots = {} 

@ray.remote
class OrderManager:
    def __init__(self):
        """Initialize the API connection once per actor"""
        from NorenRestApiPy.NorenApi import NorenApi # type: ignore
        
        class FlatTradeApiPy(NorenApi):
            def __init__(self):
                NorenApi.__init__(self, host='https://piconnect.flattrade.in/PiConnectTP/', 
                                 websocket='wss://piconnect.flattrade.in/PiConnectWSTp/', 
                                 eodhost='https://web.flattrade.in/chartApi/getdata/')
        
        self.api = FlatTradeApiPy()
        self.api.set_session(userid=userid, password=password, usertoken=token)
        self.api.get_limits()
    
    def place_order(self, stockname, b_s):
        """Place an order using the persistent API connection"""
        temp = stockname
        val = temp.split("25")[0]
        stock_name = val+"-EQ"
        if b_s == 'buy':
            if "CE" in temp and "PE" not in temp:
                order_type = "B"
            else:
                order_type = "S"
        elif b_s == 'sell':
            if "CE" in temp and "PE" not in temp:
                order_type = "S"
            else:
                order_type = "B"

        try:
            ret = self.api.place_order(buy_or_sell=order_type, product_type='I',
                            exchange='NSE', tradingsymbol=stock_name,
                            quantity=1, discloseqty=0, price_type='MKT',
                            retention='DAY', remarks='my_order_003', act_id=userid)
            return ret
        except Exception as e:
            return {"error": str(e)}

class RedisConnectionManager:
    def __init__(self, db=0, pool_size=10):
        """
        Initialize a Redis connection manager for the specified database
        
        Args:
            db (int): Redis database index
            pool_size (int): Size of the connection pool
        """
        self.pool = redis.ConnectionPool(
            host='localhost',
            port=6379,
            db=db,
            decode_responses=True,
            max_connections=pool_size
        )
        self.client = None

    def get_client(self):
        """Get a Redis client from the connection pool"""
        if self.client is None:
            self.client = redis.Redis(connection_pool=self.pool)
        return self.client

    def get_resampled_data_batch(self, tokens, max_records=100):
        """
        Get resampled data for multiple tokens at once using Redis pipeline
        
        Args:
            tokens (dict): Dictionary of token_name: token_value pairs
            max_records (int): Maximum number of records to fetch per token
                
        Returns:
            dict: Dictionary of token_name: data_list pairs
        """
        try:
            # print(f"Debug: Fetching resampled data for {len(tokens)} tokens, max records: {max_records}")
            client = self.get_client()
            result = {}
            
            # Create pipeline
            pipe = client.pipeline(transaction=False)
            
            # Add all index queries to pipeline
            for token_name, token_value in tokens.items():
                sorted_key = f"{RESAMPLED_PREFIX}:{token_value}:index"
                pipe.exists(sorted_key)
                # print(f"Debug: Checking existence of key: {sorted_key}")
                
            # Execute pipeline for index existence check
            index_exists_results = pipe.execute()    
            
            # Reset pipeline for data fetching
            pipe = client.pipeline(transaction=False)
            
            # For each token, either use index or scan
            for i, (token_name, token_value) in enumerate(tokens.items()):
                sorted_key = f"{RESAMPLED_PREFIX}:{token_value}:index"
                
                if index_exists_results[i]:
                    # print(f"Debug: Index exists for {token_name}, fetching data")
                    # Get the count to know how many records exist
                    pipe.zcard(sorted_key)
                else:
                    # print(f"Debug: No index found for {token_name}")
                    # No index, initialize with empty list
                    result[token_name] = []
                    
            # Execute pipeline to get counts
            count_results = pipe.execute()
            
            # Reset pipeline for key retrieval
            pipe = client.pipeline(transaction=False)
            
            # Process count results to fetch the last N records
            idx = 0
            for token_name, token_value in tokens.items():
                if idx < len(count_results) and index_exists_results[idx]:
                    count = count_results[idx]
                    # print(f"Debug: Found {count} records for {token_name}")
                    
                    if count > 0:
                        # Calculate the start index to get the most recent records
                        start_idx = max(0, count - max_records)
                        # Get the last max_records records
                        pipe.zrange(sorted_key, start_idx, -1)
                        # print(f"Debug: Fetching records {start_idx} to {count} for {token_name}")
                    else:
                        # No records, initialize with empty list
                        result[token_name] = []
                idx += 1
                
            # Execute pipeline for retrieving keys
            key_results = pipe.execute()           
            
            # Reset pipeline for data retrieval
            pipe = client.pipeline(transaction=False)
            
            # Track which keys belong to which tokens
            key_mapping = {}
            
            # Process the results of key retrieval
            idx = 0
            for token_name, token_value in tokens.items():
                if idx < len(key_results) and index_exists_results[idx]:
                    data_keys = key_results[idx]
                    
                    # If no keys found, initialize empty result
                    if not data_keys:
                        # print(f"Debug: No data keys found for {token_name}")
                        result[token_name] = []
                    else:
                        # print(f"Debug: Found {len(data_keys)} data keys for {token_name}")
                        # For each data key, add hgetall to pipeline
                        for key in data_keys:
                            pipe.hgetall(key)
                            key_mapping[len(key_mapping)] = token_name
                idx += 1
            
            # Execute pipeline for data retrieval
            data_results = pipe.execute()
            
            # Process the data results
            for i, data in enumerate(data_results):
                token_name = key_mapping.get(i)
                if token_name:
                    if token_name not in result:
                        result[token_name] = []
                        
                    # Process data
                    if data:
                        # Convert numeric strings to floats
                        for numeric_field in ['ltp', 'open', 'high', 'low', 'close', 'adjVol', 'ema10', 'ema20', 'pivot']:
                            if numeric_field in data:
                                try:
                                    data[numeric_field] = float(data[numeric_field])
                                except (ValueError, TypeError):
                                    data[numeric_field] = np.nan
                        
                        # Convert timestamp to datetime
                        if 'timestamp' in data:
                            ts = int(float(data['timestamp']))/1000  # Convert to seconds
                            data['datetime'] = datetime.fromtimestamp(ts)
                        
                        result[token_name].append(data)
            
            # Log summary of results
            # for token_name, data_list in result.items():
                # print(f"Debug: Retrieved {len(data_list)} records for {token_name}")
                
            return result
                
        except Exception as e:
            print(f"Error retrieving batch data from Redis: {str(e)}")
            print(traceback.format_exc())
            return {}

    def get_current_prices_batch(self, tokens):
        """
        Fetch current prices for multiple tokens from the raw data database
        
        Args:
            tokens (dict): Dictionary of token_name: token_value pairs
            
        Returns:
            dict: Dictionary of token_name: current_price pairs
        """
        try:
            # Create a client for raw database
            raw_client = redis.Redis(host='localhost', port=6379, db=RAW_DB, decode_responses=True)
            results = {}
            
            # Create pipeline
            pipe = raw_client.pipeline(transaction=False)
            
            # For each token, query the latest data
            for token_name, token_value in tokens.items():
                # First check for an index
                sorted_key = f"{RAW_PREFIX}:{token_value}:index"
                pipe.exists(sorted_key)
                
            # Execute pipeline to check indices
            index_results = pipe.execute()
            
            # Reset pipeline for data retrieval
            pipe = raw_client.pipeline(transaction=False)
            
            # Prepare to fetch latest data
            index_mapping = {}
            i = 0
            for token_name, token_value in tokens.items():
                sorted_key = f"{RAW_PREFIX}:{token_value}:index"
                
                if index_results[i]:
                    # Get the latest key from sorted set (most recent data point)
                    pipe.zrange(sorted_key, -1, -1)
                    index_mapping[len(index_mapping)] = token_name
                    
                i += 1
            
            # Execute pipeline to get latest keys
            key_results = pipe.execute()
            
            # Reset pipeline for data values
            pipe = raw_client.pipeline(transaction=False)
            
            # Map from pipeline index to token
            key_to_token = {}
            
            # For each key result, fetch the data
            for i, keys in enumerate(key_results):
                if keys and len(keys) > 0:
                    token_name = index_mapping.get(i)
                    key = keys[0]
                    pipe.hgetall(key)
                    key_to_token[len(key_to_token)] = token_name
            
            # Execute pipeline to get data
            data_results = pipe.execute()
            
            # Process results
            for i, data in enumerate(data_results):
                token_name = key_to_token.get(i)
                if token_name and data and 'ltp' in data:
                    try:
                        results[token_name] = float(data['ltp'])
                    except (ValueError, TypeError):
                        results[token_name] = None
            
            # Clean up
            raw_client.close()
            return results
                
        except Exception as e:
            logger.error(f"Error fetching batch current prices: {str(e)}")
            return {}

    def get_latest_pivot_info(self, token_name):
        """
        Get the latest pivot information for a token
        
        Args:
            token_name (str): The token name
            
        Returns:
            dict: Pivot information or None if not found
        """
        try:
            client = self.get_client()
            pivot_key = f"{RESAMPLED_PREFIX}:pivot_index:{token_name}"
            pivot_data = client.hgetall(pivot_key)
            
            if not pivot_data:
                return None
                
            # Convert numeric fields
            if 'pivot_index' in pivot_data:
                pivot_data['pivot_index'] = int(pivot_data['pivot_index'])
            if 'timestamp' in pivot_data:
                pivot_data['timestamp'] = float(pivot_data['timestamp'])
                
            return pivot_data
            
        except Exception as e:
            logger.error(f"Error retrieving pivot info for {token_name}: {str(e)}")
            return None

    def close(self):
        """Safely close the connection"""
        if self.client is not None:
            try:
                self.client.close()
            except:
                pass
            finally:
                self.client = None

def load_tokens():
    """
    Load token dictionary from the most recent token file in the tokens directory.
    Returns the token dictionary or empty dict if no file is found.
    """
    try:
        current_date = datetime.now()
        month_abbr = current_date.strftime("%b").lower()
        day = str(current_date.day)
        year = current_date.strftime("%Y")
        date_str = f"{month_abbr}{day}{year}"

        tokens_dir = os.path.join(os.getcwd(), "tokens")
        filename = os.path.join(tokens_dir, f"{date_str}token.txt")

        logger.info(f"Looking for token file: {filename}")

        if not os.path.exists(filename):
            logger.info(f"Token file for today ({date_str}) not found, looking for the most recent file...")
            if os.path.exists(tokens_dir):
                files = [f for f in os.listdir(tokens_dir) if f.endswith('token.txt')]
                if files:
                    files.sort(key=lambda x: os.path.getmtime(os.path.join(tokens_dir, x)), reverse=True)
                    filename = os.path.join(tokens_dir, files[0])
                    logger.info(f"Using the most recent token file: {files[0]}")
                else:
                    logger.error("No token files found in the tokens directory.")
                    return {}
            else:
                logger.error(f"Tokens directory not found. Run tokengen.py first.")
                return {}

        with open(filename, 'r') as file:
            token_dict = json.load(file)
            logger.info(f"Successfully loaded {len(token_dict)} tokens from {filename}")
            return token_dict

    except Exception as e:
        logger.error(f"Error loading token dictionary: {str(e)}")
        return {}

@ray.remote
class SignalTracker:
    def __init__(self):
        self.processed_entries = {}
        self.processed_exits = {}
        self.active_trades = {}
        self.completed_trades = []
        self.symbol_states = {}
        self.used_pivots = {}

    # Add methods to get and update used_pivots
    def get_used_pivot(self, symbol):
        """Get the used pivot value for a symbol"""
        return self.used_pivots.get(symbol, 0)
        
    def set_used_pivot(self, symbol, pivot_value):
        """Set the used pivot value for a symbol"""
        self.used_pivots[symbol] = pivot_value
        return True

    def is_new_entry(self, symbol, entry_time):
        return (symbol not in self.processed_entries or
                entry_time not in self.processed_entries[symbol])

    def is_new_exit(self, symbol, exit_time):
        return (symbol not in self.processed_exits or
                exit_time not in self.processed_exits[symbol])

    def add_entry(self, symbol, entry):
        if symbol not in self.processed_entries:
            self.processed_entries[symbol] = {}

        entry_time = entry['entry_time']
        self.processed_entries[symbol][entry_time] = entry
        self.active_trades[symbol] = entry
        return True

    def add_exit(self, symbol, exit_data):
        if symbol not in self.processed_exits:
            self.processed_exits[symbol] = {}

        exit_time = exit_data['exit_time']
        # Check if this exit has already been processed
        if exit_time in self.processed_exits[symbol]:
            return False  # Don't process duplicate exits
        
        # Check if this symbol is actually in active trades
        if symbol not in self.active_trades:
            return False  # Don't exit a position that's not active
    
        self.processed_exits[symbol][exit_time] = exit_data

        trade_info = {
                'symbol': symbol,
                'entry_time': exit_data['entry_time'],
                'exit_time': exit_data['exit_time'],
                'entry_price': exit_data['entry_price'],
                'exit_price': exit_data['exit_price'],
                'profit_loss': exit_data['profit_loss'],
                'exit_type': exit_data['exit_type'],
                'entry_type': exit_data['entry_type']
            }
        self.completed_trades.append(trade_info)

        if symbol in self.active_trades:
            del self.active_trades[symbol]
        return True

    def get_summary(self):
        # Basic trade counts
        total_trades = len(self.completed_trades)
        if total_trades == 0:
            return {
                'total_trades': 0,
                'winning_trades': 0,
                'losing_trades': 0,
                'win_rate': 0,
                'avg_pl': 0,
                'risk_reward': 0,
                'best_trades': [],
                'worst_trades': []
            }

        winning_trades = [t for t in self.completed_trades if t['profit_loss'] > 0]
        losing_trades = [t for t in self.completed_trades if t['profit_loss'] <= 0]

        win_rate = (len(winning_trades) / total_trades) * 100
        avg_pl = sum(t['profit_loss'] for t in self.completed_trades) / total_trades

        if losing_trades:
            avg_loss = abs(sum(t['profit_loss'] for t in losing_trades) / len(losing_trades))
            avg_win = sum(t['profit_loss'] for t in winning_trades) / len(winning_trades) if winning_trades else 0
            risk_reward = round(avg_win / avg_loss, 2) if avg_loss != 0 else 0
        else:
            risk_reward = 0

        sorted_trades = sorted(self.completed_trades, key=lambda x: x['profit_loss'], reverse=True)
        best_trades = sorted_trades[:3]
        worst_trades = sorted_trades[-3:] if len(sorted_trades) >= 3 else sorted_trades[::-1]

        return {
            'total_trades': total_trades,
            'winning_trades': len(winning_trades),
            'losing_trades': len(losing_trades),
            'win_rate': win_rate,
            'avg_pl': avg_pl,
            'risk_reward': risk_reward,
            'best_trades': best_trades,
            'worst_trades': worst_trades
        }

    def get_active_trades(self):
        """Get all currently active trades with their details"""
        return self.active_trades

    def add_batch_signals(self, batch_signals):
        """Process a batch of signals at once"""
        results = []
        
        # First process all exit signals
        exit_signals = [signal for signal in batch_signals if signal['type'] == 'exit']
        for signal in exit_signals:
            token_name = signal['token_name']
            exit_data = signal['data']
            exit_time = exit_data['exit_time']
            
            # Only process exits for active trades
            if token_name not in self.active_trades:
                continue
                
            # Skip already processed exits
            if token_name in self.processed_exits and exit_time in self.processed_exits[token_name]:
                continue
            
            # Process the exit
            if token_name not in self.processed_exits:
                self.processed_exits[token_name] = {}
            self.processed_exits[token_name][exit_time] = exit_data
            
            trade_info = {
                'symbol': token_name,
                'entry_time': exit_data['entry_time'],
                'exit_time': exit_data['exit_time'],
                'entry_price': exit_data['entry_price'],
                'exit_price': exit_data['exit_price'],
                'profit_loss': exit_data['profit_loss'],
                'exit_type': exit_data['exit_type'],
                'entry_type': exit_data['entry_type']
            }
            self.completed_trades.append(trade_info)
            
            # Remove from active trades
            if token_name in self.active_trades:
                del self.active_trades[token_name]
            results.append(signal)
        
        # Then process entry signals
        # Use a set to track which symbols already had an entry in this batch
        processed_symbols = set()
        
        entry_signals = [signal for signal in batch_signals if signal['type'] == 'entry']
        for signal in entry_signals:
            token_name = signal['token_name']
            
            # Skip if we already processed an entry for this symbol in this batch
            if token_name in processed_symbols:
                continue
                
            # Skip if symbol is already in active trades
            if token_name in self.active_trades:
                continue
                
            entry = signal['data']
            entry_time = entry['entry_time']
            
            # Check for new entry
            if token_name not in self.processed_entries or entry_time not in self.processed_entries[token_name]:
                if token_name not in self.processed_entries:
                    self.processed_entries[token_name] = {}
                self.processed_entries[token_name][entry_time] = entry
                self.active_trades[token_name] = entry
                results.append(signal)
                
                # Mark this symbol as processed in this batch
                processed_symbols.add(token_name)
                
        return results
    
    def get_symbol_state(self, symbol):
        return self.symbol_states.get(symbol, {
            'prev_pivot_high': None,
            'prev_day_pivot': None,
            'pivot_level': None,
            'waiting_for_breakout': False,
            'pivot_candle_index': None,
            'in_trade': False,
            #'used_pivot': None,
            'stop_loss_price': None
        })

    def update_symbol_state(self, symbol, state):
        self.symbol_states[symbol] = state

    def reset_symbol_state(self, symbol):
        if symbol in self.symbol_states:            
            
            self.symbol_states[symbol] = {
                'pivot_level': None,
                'waiting_for_breakout': False,
                'in_trade': False,
                'stop_loss_price': None
            }

    def update_trade_with_sl_adjusted_flag(self, symbol, new_stop_loss):
        """Update the stop loss and mark it as having been adjusted"""
        if symbol in self.active_trades:
            self.active_trades[symbol]['stop_loss'] = new_stop_loss
            self.active_trades[symbol]['sl_adjusted'] = True
            return True
        return False

def is_market_open():
    """Check if the market is currently open"""
    now = datetime.now()

    # Check if it's a weekend
    if now.weekday() >= 5:  # 5 is Saturday, 6 is Sunday
        return False

    # Check market hours
    current_time = now.time()
    market_start = datetime.strptime('09:00', '%H:%M').time()
    market_end = datetime.strptime('15:30', '%H:%M').time()

    return market_start <= current_time <= market_end

# Global state dictionary to track the state for each symbol
symbol_states = {}

def get_symbol_state(symbol):
    """Get state for a specific symbol"""
    if symbol not in symbol_states:
        symbol_states[symbol] = {
            'prev_pivot_high': None,
            'prev_day_pivot': None,
            'pivot_level': None,
            'waiting_for_breakout': False,
            'pivot_candle_index': None,
            'in_trade': False,
            'stop_loss_price': None
        }
    return symbol_states[symbol]

def reset_symbol_state(symbol):
    """Reset state after an exit"""
    if symbol in symbol_states:        
        symbol_states[symbol]['in_trade'] = False
        symbol_states[symbol]['waiting_for_breakout'] = False
        symbol_states[symbol]['pivot_level'] = None

def update_symbol_state_after_entry(symbol, stop_loss):
    """Update symbol state after entry is confirmed by signal tracker"""
    if symbol in symbol_states:
        symbol_states[symbol]['in_trade'] = True
        symbol_states[symbol]['stop_loss_price'] = stop_loss

def get_next_run_time(interval_seconds=30):
    """
    Calculate the next run time based on interval
    
    Returns:
        tuple: (next_run_datetime, seconds_to_wait)
    """
    now = datetime.now()
    seconds = now.second
    next_interval = ((seconds // interval_seconds) + 1) * interval_seconds
    
    if next_interval >= 60:  # Handle minute rollover
        next_minute = now.minute + 1
        next_interval = 0
    else:
        next_minute = now.minute
        
    next_run = now.replace(minute=next_minute, second=next_interval, microsecond=0)
    seconds_to_wait = (next_run - now).total_seconds()
    
    # Ensure we wait at least 5 seconds
    if seconds_to_wait < 5:
        next_run = next_run + timedelta(seconds=interval_seconds)
        seconds_to_wait = (next_run - now).total_seconds()
        
    return next_run, seconds_to_wait

@ray.remote
def process_token_batch(token_batch, signal_tracker, order_manager):
    """
    Process a batch of tokens in parallel
    
    Args:
        token_batch (dict): Dictionary of token_name: token_value pairs
        signal_tracker: Ray actor for tracking signals
        
    Returns:
        list: List of signals generated from the batch
    """
    try:
        # Create a connection manager for resampled data
        resampled_conn = RedisConnectionManager(db=RESAMPLED_DB)
        # Create a connection manager for current prices
        raw_conn = RedisConnectionManager(db=RAW_DB)
        all_signals = []
        
        # Get resampled data for all tokens in the batch
        resampled_data = resampled_conn.get_resampled_data_batch(token_batch)
        
        # Get current prices for all tokens in the batch
        current_prices = raw_conn.get_current_prices_batch(token_batch)

        if 'used_pivots' not in globals():
            used_pivots = {}

 
        # Process each token
        for token_name, token_value in token_batch.items():
            try:

                state = ray.get(signal_tracker.get_symbol_state.remote(token_name))                

                # Get the latest pivot information
                pivot_info = resampled_conn.get_latest_pivot_info(token_name)
                
                # Get the resampled data
                data = resampled_data.get(token_name, [])
                
                # Get the current price
                current_price = current_prices.get(token_name)

                # Early validation - check all required data is available
                if not data:
                    # print(f"Skipping {token_name}: No resampled data available")
                    continue
                
                if not pivot_info:
                    # print(f"Skipping {token_name}: No pivot information available")
                    # time.sleep(2)
                    continue
                
                if not current_price:
                    # print(f"Skipping {token_name}: No current price available")
                    continue
                
                
                # Convert to DataFrame for easier processing
                df = pd.DataFrame(data)
                
                
                # Sort by timestamp if available
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'].astype(float)/1000, unit='s')
                    df = df.sort_values('timestamp')
                
                # Apply the strategy 
                entries = strategy(df, token_name, pivot_info, current_price, signal_tracker, order_manager)
                
                # Process entries
                for entry in entries:
                    entry_time = entry['entry_time']
                    is_new_entry = ray.get(signal_tracker.is_new_entry.remote(token_name, entry_time))
                    
                    if is_new_entry:
                        entry_signal = {
                            'type': 'entry',
                            'token_name': token_name,
                            'data': entry
                        }
                        all_signals.append(entry_signal)
                        new_state = state.copy()
                        new_state.update({
                            'in_trade': True,
                            'stop_loss_price': entries[0]['stop_loss'],
                            # Update other state fields as needed
                        })
                        ray.get(signal_tracker.update_symbol_state.remote(token_name, new_state))
                        
                    # Handle exit signals (if any)
                    if 'exit_time' in entry:
                        exit_time = entry['exit_time']
                        is_new_exit = ray.get(signal_tracker.is_new_exit.remote(token_name, exit_time))
                        
                        if is_new_exit:
                            exit_signal = {
                                'type': 'exit',
                                'token_name': token_name,
                                'data': entry
                            }
                            all_signals.append(exit_signal)
                            ray.get(signal_tracker.reset_symbol_state.remote(token_name))
                            
            except Exception as e:
                logger.error(f"Error processing token {token_name}: {str(e)}")
                all_signals.append({
                    'type': 'error', 
                    'token_name': token_name, 
                    'message': str(e)
                })
        
        # Clean up connections
        resampled_conn.close()
        raw_conn.close()
        return all_signals
        
    except Exception as e:
        logger.error(f"Error in batch processing: {str(e)}")
        return [{'type': 'error', 'token_name': 'batch_processing', 'message': str(e)}]

def calculate_live_ema(previous_ema, current_price, span):
    """
    Calculate the current EMA value using the previous EMA and current price
    
    Args:
        previous_ema (float): Previous EMA value from resampled data
        current_price (float): Current market price
        span (int): EMA period (e.g., 10 for EMA10, 20 for EMA20)
        
    Returns:
        float: Updated EMA value
    """
    # Calculate the smoothing factor
    alpha = 2 / (span + 1)
    
    # Calculate the updated EMA
    current_ema = (current_price * alpha) + (previous_ema * (1 - alpha))
    
    return current_ema

def get_live_ema_values(df, current_price):
    """
    Get live EMA values based on the latest resampled data and current price
    
    Args:
        df (pd.DataFrame): DataFrame with resampled data that includes ema10 and ema20
        current_price (float): Current market price from raw data
        
    Returns:
        tuple: (live_ema10, live_ema20)
    """
    # Ensure dataframe is sorted by timestamp
    if 'timestamp' in df.columns and not df['timestamp'].is_monotonic_increasing:
        df = df.sort_values('timestamp')
    
    # Get the latest EMA values from the resampled data
    if len(df) > 0:
        latest_row = df.iloc[-1]
        latest_ema10 = latest_row.get('ema10')
        latest_ema20 = latest_row.get('ema20')
        
        # Calculate live EMA values
        live_ema10 = calculate_live_ema(latest_ema10, current_price, 10) if latest_ema10 is not None else None
        live_ema20 = calculate_live_ema(latest_ema20, current_price, 20) if latest_ema20 is not None else None

        # print(live_ema10,"asdas",live_ema20)
        
        return live_ema10, live_ema20
    
    return None, None

def check_ema_conditions(df, current_price):
    """
    Check if current EMA10 > EMA20 using live calculation
    
    Args:
        df (pd.DataFrame): DataFrame with resampled data
        current_price (float): Current market price
        
    Returns:
        bool: True if live EMA10 > live EMA20, False otherwise
    """
    # Calculate live EMAs
    live_ema10, live_ema20 = get_live_ema_values(df, current_price)
 
    # Check EMA condition
    if live_ema10 is not None and live_ema20 is not None:
        return live_ema10 > live_ema20
    
    # If we can't calculate live EMAs, fall back to the latest values from the dataframe
    if len(df) > 0:
        latest_row = df.iloc[-1]
        if 'ema10' in latest_row and 'ema20' in latest_row:
            return latest_row['ema10'] > latest_row['ema20']
    
    # If no data is available, return False
    return False

def strategy(df, stockname, pivot_info, current_price, signal_tracker, order_manager):
    """
    Strategy using pre-calculated pivot points
    
    Args:
        df (pd.DataFrame): DataFrame with resampled data
        stockname (str): Token name/symbol
        pivot_info (dict): Latest pivot information
        current_price (float): Current market price from raw data
        
    Returns:
        list: List of entry signals
    """
    # Get state for this symbol
    state = get_symbol_state(stockname)

    # Skip if already in a trade
    if state['in_trade']:
        return []    
    
    # Early return for empty dataframe
    if len(df) == 0:
        return []
    
    entries = []
    current_time = datetime.now().time()
    start_time = datetime.strptime('10:30', '%H:%M').time()
    exit_time = datetime.strptime('15:20', '%H:%M').time()
    
    # Skip if current time is before start time
    if current_time < start_time:
        return []
    
    # Skip if current time is past exit time
    if current_time >= exit_time:
        return []
    
    # Check if we need to sort the dataframe by timestamp
    if 'timestamp' in df.columns and not df['timestamp'].is_monotonic_increasing:
        try:
            df = df.sort_values('timestamp')
        except Exception:
            pass

  
    # import pandas as pd
    # pd.set_option('display.max_rows', None)
    # pd.set_option('display.max_columns', None)
    # pd.set_option('display.width', None)
    # pd.set_option('display.max_colwidth', None)
    
    # # Print the complete dataframe
    # print("\nComplete DataFrame:")
    # print(stockname)
    # print(df)
    # print(pivot_info)
    # # time.sleep(1000)
    
    # # # Reset display options to default
    # pd.reset_option('display.max_rows')
    # pd.reset_option('display.max_columns')
    # pd.reset_option('display.width')
    # pd.reset_option('display.max_colwidth')

    # Extract pivot level from pivot_info and validate against the dataframe
    if pivot_info and 'pivot_index' in pivot_info:
        pivot_index = pivot_info['pivot_index']

        # Check if there are pivot candles in the dataframe
        pivot_candles = df[df['pivot'] == 2]
        
        
        # Try to find the correct pivot candle
        if len(pivot_candles) > 0:
            # First try to find it based on index if we can
            if 'pivot_candle_index' in df.columns:
                matching_pivots = df[df['pivot_candle_index'] == str(pivot_index)]
                if not matching_pivots.empty:
                    pivot_candle = matching_pivots.iloc[0]
                else:
                    # If we can't find by index, use the most recent pivot
                    # pivot_candle = pivot_candles.iloc[-1]
                    logger.info("\nERROR: PIVOT MISMATCH OR SOME SH*T IN: ", stockname)
                    return []
            else:
                # Use the most recent pivot
                # pivot_candle = pivot_candles.iloc[-1]
                logger.info("\nERROR: PIVOT MISMATCH OR SOME SH*T IN: ", stockname)
                return []            

            pivot_level = pivot_candle['high']

            used_pivot = ray.get(signal_tracker.get_used_pivot.remote(stockname))            
            
            # Check if we've already used this pivot
            if used_pivot != 0 and abs(used_pivot - pivot_level) < 0.01:
                # Skip this trade as we've already used this pivot
                return []
            
            # Update state with pivot information
            state['pivot_level'] = pivot_level
            state['pivot_candle_index'] = pivot_index

            live_ema10, _ = get_live_ema_values(df, current_price)

            # Check if live_ema10 is available
            if live_ema10 is None and len(df) > 0:
                # Fall back to the latest EMA10 from the dataframe
                latest_row = df.iloc[-1]
                if 'ema10' in latest_row:
                    live_ema10 = latest_row['ema10']
            
            # Skip if we don't have a valid EMA10 value
            if live_ema10 is None:
                return []

            # Check if current price has broken above the pivot level
            if current_price > pivot_level and check_ema_conditions(df, current_price) and current_price > live_ema10:
                # print(stockname)
                
                
                # Calculate movement percentage (10-candle lookback)
                if len(df) >= 12:
                    # Sort to ensure we're using the right order
                    df_sorted = df.sort_values('timestamp')
                    recent_df = df_sorted.iloc[-12:]
                    
                    tenth_prev_close = float(recent_df.iloc[0]['low'])
                    
                    current_close = float(recent_df.iloc[-1]['close'])
                    try:
                        movement_percentage = ((current_close - tenth_prev_close) / tenth_prev_close) * 100
                    except Exception as e:
                        # print("Movement percent 0 division error for ", stockname)
                        return []

                    if movement_percentage > 20:
                        if  'adjVol' in df.columns and 'close' in df.columns:

                            # Check volume condition 
                            recent_volume = 0
                            # Calculate price_volume for the last 5 candles
                            last_5_candles = df_sorted.iloc[-6:]
                            try:
                                recent_volume = (last_5_candles['close'] * last_5_candles['adjVol']).sum()                     
                            except:
                                # print("volume: 0 division error for ", stockname)
                                return []
                    
                        # Apply movement and volume filters
                        if  recent_volume > 1000000:
                            
                            # Determine entry price
                            entry_price = current_price
                            
                            # Check for valid entry - percentage difference from pivot should be <= 20%
                            percentage_diff = ((entry_price - pivot_level) / pivot_level) * 100

                            if percentage_diff <= 20:
                                
                                # Calculate stop loss
                                stop_loss_price = entry_price * 0.99

                                try:
                                    ray.get(order_manager.place_order.remote(stockname, 'buy'))
                                except:
                                    logger.error("Not able to place order in strategy for ", stockname)
                                
                                # Create entry signal
                                entry = {
                                    'entry_time': datetime.now(),
                                    'entry_price': entry_price,
                                    'stop_loss': stop_loss_price,
                                    'pivot_level': pivot_level,
                                    'ema10': df_sorted.iloc[-1]['ema10'] if 'ema10' in df_sorted.columns else None,
                                    'entry_type': 'Breakout',
                                    'entry_reason': f'Breakout above pivot {pivot_level:.2f} with {movement_percentage:.2f}% movement',
                                    'movement_percentage': movement_percentage,
                                    'sl_adjusted': False,
                                    'symbol': stockname
                                }
                                
                                entries.append(entry)
                                
                                # Update state
                                state['in_trade'] = True
                                state['stop_loss_price'] = stop_loss_price
                                ray.get(signal_tracker.set_used_pivot.remote(stockname, pivot_level))                                

    return entries

def print_signal(signal):
    """Print a signal in a readable format"""
    if signal['type'] == 'entry':
        entry = signal['data']
        print(f"\nNew signal for {signal['token_name']}:")
        print(f"Entry Time: {entry['entry_time']}")
        print(f"Entry Price: {entry['entry_price']:.2f}")
        print(f"Stop Loss: {entry['stop_loss']:.2f}")
        print(f"Entry Type: {entry['entry_type']}")
        print(f"Entry Reason: {entry['entry_reason']}")
    elif signal['type'] == 'exit':
        exit_data = signal['data']
        print(f"\nExit signal for {signal['token_name']}:")
        print(f"Exit Time: {exit_data['exit_time']}")
        print(f"Exit Price: {exit_data['exit_price']:.2f}")
        print(f"Exit Type: {exit_data['exit_type']}")
        print(f"Exit Reason: {exit_data['exit_reason']}")
        print(f"Profit/Loss: {exit_data['profit_loss']:.2f}%")
    elif signal['type'] == 'error':
        print(f"Error processing {signal['token_name']}: {signal['message']}")

def print_active_trades(active_trades, raw_conn, resampled_conn):
    """Print a summary of all currently active trades"""
    if not active_trades:
        return

    print(f"\n=== ACTIVE TRADES ===")
    print(f"Total Active Trades: {len(active_trades)} at ", datetime.now().time())
    print("-" * 100)
    print(f"{'Symbol':<20} {'Entry Time':<25} {'Entry Price':>10} {'Current':>10} {'P/L %':>10} {'Stop Loss':>10} ")
    print("-" * 100)
    
    # Build a batch query for current prices
    tokens_to_query = {symbol: token_dict.get(symbol) for symbol in active_trades.keys()}
    
    # Get current prices from raw data
    current_prices = raw_conn.get_current_prices_batch(tokens_to_query)
    
    # Get technical data for EMA values from resampled data
    # resampled_data = resampled_conn.get_resampled_data_batch(tokens_to_query)
    
    # Store all EMAs in a dictionary first to prevent scope issues
    # ema_values = {}
    
    # Calculate all EMAs first
    # for symbol in active_trades.keys():
    #     current_price = current_prices.get(symbol)
    #     data = resampled_data.get(symbol, [])
        
    #     if data and current_price:
    #         df = pd.DataFrame(data)
    #         if not df.empty and 'ema10' in df.columns:
    #             if 'timestamp' in df.columns:
    #                 df['timestamp'] = pd.to_datetime(df['timestamp'].astype(float)/1000, unit='s')
    #                 df = df.sort_values('timestamp')
                
    #             latest_ema10 = df.iloc[-1]['ema10']
    #             live_ema10 = calculate_live_ema(latest_ema10, current_price, 10)
    #             ema_values[symbol] = live_ema10
    
    # Now display rows using pre-calculated EMAs from our dictionary
    for symbol, trade in active_trades.items():
        current_price = current_prices.get(symbol)
        
        # Get the pre-calculated EMA value
        # ema10_value = ema_values.get(symbol, 'N/A')
        
        # if isinstance(ema10_value, (int, float)):
        #     ema10_display = f"{ema10_value:.2f}"
        # else:
        #     ema10_display = 'N/A'
        
        if current_price:
            pnl_percentage = ((current_price - trade['entry_price']) / trade['entry_price']) * 100
            print(f"{symbol:<20} {str(trade['entry_time']):<25} {trade['entry_price']:>10.2f} {current_price:>10.2f} {pnl_percentage:>10.2f} {trade['stop_loss']:>10.2f} ")
        else:
            print(f"{symbol:<20} {str(trade['entry_time']):<25} {trade['entry_price']:>10.2f} {'N/A':>10} {'N/A':>10} {trade['stop_loss']:>10.2f}")
    print("-" * 100)

def batch_tokens(token_dict, batch_size=25):
    """
    Split token dictionary into batches
    
    Args:
        token_dict (dict): Dictionary of token_name: token_value pairs
        batch_size (int): Number of tokens per batch
        
    Returns:
        list: List of token batch dictionaries
    """
    batches = []
    items = list(token_dict.items())
    
    for i in range(0, len(items), batch_size):
        batch_dict = dict(items[i:i+batch_size])
        batches.append(batch_dict)
        
    return batches

def check_for_exits(active_trades, raw_conn, resampled_conn, signal_tracker, order_manager):
    """
    Check if any active trades need to exit based on current prices and technical conditions
    
    Args:
        active_trades (dict): Dictionary of active trades
        raw_conn (RedisConnectionManager): Connection manager for raw data (current prices)
        resampled_conn (RedisConnectionManager): Connection manager for resampled data (technicals)
        signal_tracker (SignalTracker): Ray actor for tracking signals
        
    Returns:
        int: Number of trades exited
    """
    if not active_trades:
        return 0
    
    trades_to_exit = []
    current_time = datetime.now().time()
    exit_time = datetime.strptime('15:20', '%H:%M').time()
    
    # Process each active trade individually
    for symbol, trade in active_trades.items():
        # Create a single-item query for this symbol
        single_token_query = {symbol: token_dict.get(symbol)}
        
        # Get current price for just this symbol
        current_prices = raw_conn.get_current_prices_batch(single_token_query)
        current_price = current_prices.get(symbol)
        
        # Get technical data for just this symbol
        resampled_data = resampled_conn.get_resampled_data_batch(single_token_query)
        data = resampled_data.get(symbol, [])
        
        if not current_price:
            continue

        # Calculate current profit percentage
        profit_percentage = ((current_price - trade['entry_price']) / trade['entry_price']) * 100

        # Check if profit is 5% or more AND we haven't adjusted the stop loss yet
        if profit_percentage >= 5.0 and not trade.get('sl_adjusted', False):
            # Calculate new stop loss at 3% below current price
            new_stop_loss = current_price * 0.97
            
            # Only update if the new stop loss is higher than the existing one
            if new_stop_loss > trade['stop_loss']:
                # Update the stop loss and mark as adjusted
                ray.get(signal_tracker.update_trade_with_sl_adjusted_flag.remote(symbol, new_stop_loss))
                
                # Update local copy for this iteration
                trade['stop_loss'] = new_stop_loss
                trade['sl_adjusted'] = True
                
                # Print stop loss adjustment notification
                print(f"\nAdjusted stop loss for {symbol}: New SL: {new_stop_loss:.2f} (original: {trade['entry_price'] * 0.99:.2f})")
                
        # Get latest EMA10 from resampled data if available
        previous_candle_ema10 = None
        live_ema10 = None
        if data:
            df = pd.DataFrame(data)
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'].astype(float)/1000, unit='s')
                df = df.sort_values('timestamp')
            
            if not df.empty and 'ema10' in df.columns:
                previous_candle_ema10 = df.iloc[-1]['ema10']
                live_ema10 = calculate_live_ema(previous_candle_ema10, current_price, 10)
                print("EMA: ", live_ema10, "for ", symbol)
        
        # Check exit conditions
        stop_loss_hit = current_price <= trade['stop_loss']
        ema_exit = live_ema10 is not None and current_price < live_ema10
        time_exit = current_time >= exit_time

        exit_data = None
        if stop_loss_hit or ema_exit or time_exit:
            # Check stop loss
            if stop_loss_hit:
                stop_loss_price = trade['stop_loss']
                profit_loss = ((stop_loss_price - trade['entry_price']) / trade['entry_price']) * 100
                exit_data = {
                    'exit_time': datetime.now(),
                    'exit_price': trade['stop_loss'],
                    'exit_type': 'Stop Loss',
                    'exit_reason': f'Price hit stop loss at {trade["stop_loss"]:.2f} (1% below entry)',
                    'entry_time': trade['entry_time'],
                    'entry_price': trade['entry_price'],
                    'profit_loss': profit_loss,
                    'entry_type': trade['entry_type']
                }
            # Check EMA exit
            elif ema_exit:
                profit_loss = ((current_price - trade['entry_price']) / trade['entry_price']) * 100
                exit_data = {
                    'exit_time': datetime.now(),
                    'exit_price': current_price,
                    'exit_type': 'EMA Exit',
                    'exit_reason': f'Price broke below EMA at {live_ema10:.2f}',
                    'entry_time': trade['entry_time'],
                    'entry_price': trade['entry_price'],
                    'profit_loss': profit_loss,
                    'entry_type': trade['entry_type']
                }
            # Check time exit
            elif time_exit:
                profit_loss = ((current_price - trade['entry_price']) / trade['entry_price']) * 100
                exit_data = {
                    'exit_time': datetime.now(),
                    'exit_price': current_price,
                    'exit_type': 'Time Exit',
                    'exit_reason': 'Market closing time reached (15:20)',
                    'entry_time': trade['entry_time'],
                    'entry_price': trade['entry_price'],
                    'profit_loss': profit_loss,
                    'entry_type': trade['entry_type']
                }
                
        if exit_data:
            try:
                ray.get(order_manager.place_order.remote(symbol, 'sell'))
            except:
                logger.error("Not able to place order in strategy for ", symbol)
            
            # Store the pivot level in the global dictionary
            if 'pivot_level' in trade and trade['pivot_level'] is not None:
                ray.get(signal_tracker.set_used_pivot.remote(symbol, trade['pivot_level']))
            
            trades_to_exit.append((symbol, exit_data))
    
    # Process all exits in one batch for efficiency
    if trades_to_exit:
        batch_signals = []
        for symbol, exit_data in trades_to_exit:
            batch_signals.append({
                'type': 'exit',
                'token_name': symbol,
                'data': exit_data
            })
            
        # Add all exits to signal tracker in a single batch operation
        new_signals = ray.get(signal_tracker.add_batch_signals.remote(batch_signals))
        
        # Print signals
        for signal in new_signals:
            print_signal(signal)
            
        # Reset state for exited symbols
        for symbol, _ in trades_to_exit:            
            reset_symbol_state(symbol)
            ray.get(signal_tracker.reset_symbol_state.remote(symbol))

    return len(trades_to_exit)

def main():
    """
    Main function to run the trading system
    """
    global total_runs, active_trades_count, used_pivots
    try:
        # Initialize Ray
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)
            logger.info(f"Initialized Ray with {ray.cluster_resources()['CPU']} CPUs")
        
        # Initialize SignalTracker
        signal_tracker = SignalTracker.remote()
        
        # Create a single order manager actor
        order_manager = OrderManager.remote()
        
        # Create connection managers
        raw_conn = RedisConnectionManager(db=RAW_DB, pool_size=20)
        resampled_conn = RedisConnectionManager(db=RESAMPLED_DB, pool_size=20)
        
        logger.info("\nTrading system started...")

        # Batch the tokens to improve performance
        batch_size = 1  # Adjust based on token count and system capacity
        token_batches = batch_tokens(token_dict, batch_size)
        # logger.info(f"Processing {len(token_dict)} tokens in {len(token_batches)} batches")
        
        # Main trading loop
        while True:
            run_start = time.time()
            now = datetime.now()
            
            # Check if it's time to end
            if not is_market_open():
                print("\nMarket closing time reached. Generating final report...")

                # Get final statistics
                stats = ray.get(signal_tracker.get_summary.remote())

                print("\n=== TRADING STATISTICS ===")
                print(f"Total Trades: {stats['total_trades']}")
                print(f"Winning Trades: {stats['winning_trades']}")
                print(f"Losing Trades: {stats['losing_trades']}")
                print(f"Win Rate: {stats['win_rate']:.2f}%")
                print(f"Average P/L per Trade: {stats['avg_pl']:.2f}%")
                print(f"Risk/Reward Ratio: 1:{stats['risk_reward']}")

                if stats['best_trades']:
                    print("\nBest Trades:")
                    for trade in stats['best_trades']:
                        print(f"{trade['symbol']}: {trade['profit_loss']:.2f}% | "
                              f"Entry: {trade['entry_price']:.2f} ({trade['entry_type']}) | "
                              f"Exit: {trade['exit_price']:.2f} ({trade['exit_type']})")

                if stats['worst_trades']:
                    print("\nWorst Trades:")
                    for trade in stats['worst_trades']:
                        print(f"{trade['symbol']}: {trade['profit_loss']:.2f}% | "
                              f"Entry: {trade['entry_price']:.2f} ({trade['entry_type']}) | "
                              f"Exit: {trade['exit_price']:.2f} ({trade['exit_type']})")

                break

            # Increment run counter
            total_runs += 1
            
            try:
                # Get and print active trades
                active_trades = ray.get(signal_tracker.get_active_trades.remote())
                active_symbols = set(active_trades.keys())
                
                # if should_print_active_trades():
                print_active_trades(active_trades, raw_conn, resampled_conn)
                
                # Check for exits on active trades
                if active_trades:
                    exits = check_for_exits(active_trades, raw_conn, resampled_conn, signal_tracker, order_manager)
                    if exits > 0:
                        logger.info(f"Exited {exits} positions")
                
                # Process token batches with parallel execution
                process_start = time.time()
                # logger.info(f"Processing {len(token_batches)} batches of tokens...")
                
                # Launch all batch processing in parallel
                futures = [process_token_batch.remote(batch, signal_tracker, order_manager) for batch in token_batches]
                
                # Process results as they arrive
                all_signals = []
                for future in futures:
                    try:
                        batch_signals = ray.get(future)
                        all_signals.extend(batch_signals)
                    except Exception as e:
                        logger.error(f"Error processing batch: {str(e)}")
                
                # Process all signals in a batch on the signal tracker
                if all_signals:
                    new_signals = ray.get(signal_tracker.add_batch_signals.remote(all_signals))
                    
                    # Print only the new signals
                    for signal in new_signals:
                        print_signal(signal)
                        # Update symbol state for new entries
                        if signal['type'] == 'entry':
                            symbol = signal['token_name']
                            stop_loss = signal['data']['stop_loss']
                            update_symbol_state_after_entry(symbol, stop_loss)
                
                process_end = time.time()
                # Print summary statistics if it's time
                # if should_print_stats():
                run_duration = time.time() - run_start
                # print("\n")
                # logger.info(f"Statistics summary: Completed {total_runs} runs since last report")
                # logger.info(f"Last run duration: {run_duration:.2f} seconds")
                # print("\n")
                    
                # logger.info(f"Processed all batches in {process_end - process_start:.2f} seconds")
                
            except Exception as e:
                logger.error(f"Error in main processing loop: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                time.sleep(5)  # Brief pause before retrying
                continue
            
    except KeyboardInterrupt:
        logger.info("Trading system stopped by user")
    except Exception as e:
        logger.error(f"Fatal error in trading system: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        # Clean up resources
        try:
            logger.info("Cleaning up resources...")
            if 'raw_conn' in locals():
                raw_conn.close()
            if 'resampled_conn' in locals():
                resampled_conn.close()
            if ray.is_initialized():
                ray.shutdown()
            logger.info("Cleanup completed. System shutdown.")
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")

if __name__ == "__main__":

    try:
        global token_dict
        # Initialize tokens
        token_dict = load_tokens()
        # token_dict =  {"BANKBARODA25MAR200PE": "83648"}
        # token_dict = {"DRREDDY25MAR1200CE": "105433"}
        # token_dict = {"HINDPETRO25MAR340CE": "114877"}
         # If no tokens were loaded, try to generate them
        if not token_dict:
            logger.info("No tokens loaded....")
        
        # Run the main function
        main()
        
    except KeyboardInterrupt:
        logger.info("Script terminated by user")
        if 'ray' in sys.modules and ray.is_initialized():
            ray.shutdown()
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        if 'ray' in sys.modules and ray.is_initialized():
            ray.shutdown()
