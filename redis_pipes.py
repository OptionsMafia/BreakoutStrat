''' 
redis pipes v4

1. Adding cleanup for memory leaks


'''
import redis
from time import perf_counter
from datetime import datetime, timedelta
import time
import pandas as pd
from collections import OrderedDict
import numpy as np
import ray
import os
import json
import subprocess
import sys
# from pympler.tracker import SummaryTracker


start = perf_counter()

def run_tokengen():
    """
    Run the tokengen.py script to generate the token file with a simple
    progress indicator. Returns True if successful, False otherwise.
    """
    try:
        print("Running tokengen.py to generate fresh tokens...")
        script_dir = os.path.dirname(os.path.abspath(__file__))
        tokengen_path = os.path.join(script_dir, "tokengen.py")

        if not os.path.exists(tokengen_path):
            print(f"Error: tokengen.py not found at {tokengen_path}")
            return False

        print("Processing tokens ", end="")

        stop_indicator = False

        def show_indicator():
            indicators = ['|', '/', '-', '\\']
            i = 0
            while not stop_indicator:
                print(f"\rProcessing tokens {indicators[i % len(indicators)]}", end="")
                i += 1
                time.sleep(0.5)

        import threading
        indicator_thread = threading.Thread(target=show_indicator)
        indicator_thread.daemon = True
        indicator_thread.start()

        process = subprocess.run([sys.executable, tokengen_path],
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              text=True,
                              check=False)

        stop_indicator = True
        indicator_thread.join(1.0)

        print("\r" + " " * 50 + "\r", end="")

        if process.returncode == 0:
            print("Successfully generated tokens with tokengen.py")
            return True
        else:
            print(f"Error running tokengen.py: {process.stderr}")
            return False

    except Exception as e:
        print(f"Error executing tokengen.py: {str(e)}")
        return False
    finally:
        if 'indicator_thread' in locals():
            indicator_thread.join(1.0)

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

        print(f"Looking for token file: {filename}")

        if not os.path.exists(filename):
            print(f"Token file for today ({date_str}) not found, looking for the most recent file...")
            if os.path.exists(tokens_dir):
                files = [f for f in os.listdir(tokens_dir) if f.endswith('token.txt')]
                if files:
                    files.sort(key=lambda x: os.path.getmtime(os.path.join(tokens_dir, x)), reverse=True)
                    filename = os.path.join(tokens_dir, files[0])
                    print(f"Using the most recent token file: {files[0]}")
                else:
                    print("No token files found in the tokens directory.")
                    return {}
            else:
                print(f"Tokens directory not found. Run tokengen.py first.")
                return {}

        with open(filename, 'r') as file:
            token_dict = json.load(file)
            print(f"Successfully loaded {len(token_dict)} tokens from {filename}")
            return token_dict

    except Exception as e:
        print(f"Error loading token dictionary: {str(e)}")
        return {}

def get_redis_client():
    """Create a new Redis client"""
    return redis.Redis(
        host='localhost',
        port=6379,
        db=0,
        decode_responses=True
    )

@ray.remote
class SignalTracker:
    def __init__(self):
        self.processed_entries = {}
        self.processed_exits = {}
        self.active_trades = {}
        self.completed_trades = []

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
        for signal in batch_signals:
            token_name = signal['token_name']
            
            if signal['type'] == 'entry':
                entry = signal['data']
                entry_time = entry['entry_time']
                if token_name not in self.processed_entries or entry_time not in self.processed_entries[token_name]:
                    if token_name not in self.processed_entries:
                        self.processed_entries[token_name] = {}
                    self.processed_entries[token_name][entry_time] = entry
                    self.active_trades[token_name] = entry
                    results.append(signal)
                    
            elif signal['type'] == 'exit':
                exit_data = signal['data']
                exit_time = exit_data['exit_time']
                # Add validation to prevent duplicate exits
                if token_name not in self.active_trades:
                    continue  # Skip exits for positions that aren't active
                    
                if token_name in self.processed_exits and exit_time in self.processed_exits[token_name]:
                    continue  # Skip already processed exits
                
                if token_name not in self.processed_exits or exit_time not in self.processed_exits[token_name]:
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
                    
                    if token_name in self.active_trades:
                        del self.active_trades[token_name]
                    results.append(signal)
                    
        return results

class RedisConnectionManager:
    def __init__(self, pool_size=10):
        self.pool = redis.ConnectionPool(
            host='localhost',
            port=6379,
            db=0,
            decode_responses=True,
            max_connections=pool_size
        )
        self.client = None

    def get_client(self):
        """Get a Redis client from the connection pool"""
        if self.client is None:
            self.client = redis.Redis(connection_pool=self.pool)
        return self.client

    def get_data_batch(self, tokens, max_records=100):
        """
        Get data for multiple tokens at once using Redis pipeline
        
        Args:
            tokens (dict): Dictionary of token_name: token_value pairs
            max_records (int): Maximum number of records to fetch per token
            
        Returns:
            dict: Dictionary of token_name: data_list pairs
        """
        try:
            client = self.get_client()
            prefix = '13mar2025'
            result = {}
            
            # Create pipeline
            pipe = client.pipeline(transaction=False)
            
            # Add all index queries to pipeline
            for token_name, token_value in tokens.items():
                sorted_key = f"{prefix}:{token_value}:index"
                pipe.exists(sorted_key)
                
            # Execute pipeline for index existence check
            index_exists_results = pipe.execute()    
            
            # Reset pipeline for data fetching
            pipe = client.pipeline(transaction=False)
            
            # For each token, either use index or scan based on existence result
            for i, (token_name, token_value) in enumerate(tokens.items()):
                sorted_key = f"{prefix}:{token_value}:index"
                
                if index_exists_results[i]:
                    # Index exists, use zrange to get keys
                    pipe.zrange(sorted_key, 0, -1)
                else:
                    # No index, we'll handle these separately
                    result[token_name] = []
                    
            # Execute pipeline for retrieving keys
            key_results = pipe.execute()           
            
            # Reset pipeline for data retrieval
            pipe = client.pipeline(transaction=False)
            
            # Track which keys belong to which tokens
            key_mapping = {}
            
            # Process the results of key retrieval
            idx = 0
            for token_name, token_value in tokens.items():
                sorted_key = f"{prefix}:{token_value}:index"
                
                if idx < len(key_results) and index_exists_results[idx]:
                    data_keys = key_results[idx]
                    
                    # If no keys found, initialize empty result
                    if not data_keys:
                        result[token_name] = []
                    else:
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
                        for numeric_field in ['ltp', 'open', 'high', 'low', 'close', 'volume']:
                            if numeric_field in data:
                                try:
                                    data[numeric_field] = float(data[numeric_field])
                                except (ValueError, TypeError):
                                    data[numeric_field] = np.nan
                        
                        # Add timestamp
                        if 'timestamp' in data:
                            ts = int(float(data['timestamp']))/1000  # Convert to seconds
                            data['datetime'] = datetime.fromtimestamp(ts)
                        
                        result[token_name].append(data)
                        # time.sleep(1000)
            
            return result
                    
        except Exception as e:
            print(f"Error retrieving batch data from Redis: {str(e)}")
            return {}
        finally:
        # Ensure pipeline is explicitly closed
            if 'pipe' in locals():
                pipe.reset()

    def close(self):
        """Safely close the connection"""
        if self.client is not None:
            try:
                self.client.close()
            except:
                pass
            finally:
                self.client = None

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

@ray.remote
def process_token_batch(token_batch, signal_tracker):
    """
    Process a batch of tokens in parallel
    
    Args:
        token_batch (dict): Dictionary of token_name: token_value pairs
        signal_tracker: Ray actor for tracking signals
        
    Returns:
        list: List of signals generated from the batch
    """
    try:
        # Create a connection manager for this process
        conn_manager = RedisConnectionManager()
        all_signals = []
        
        # Get data for all tokens in the batch at once
        token_data = conn_manager.get_data_batch(token_batch)
        
        # Process each token's data
        for token_name, token_value in token_batch.items():
            try:
                # Convert data to DataFrame
                data = token_data.get(token_name, [])
                if not data:
                    continue
                    
                t_df = prepare_dataframe(data)            
                
                if len(t_df) > 0:
                    new_entries = strategy(t_df, token_name)
                    
                    for entry in new_entries:
                        entry_time = entry['entry_time']
                        is_new_entry = ray.get(signal_tracker.is_new_entry.remote(token_name, entry_time))
                        
                        if is_new_entry:
                            entry_signal = {
                                'type': 'entry',
                                'token_name': token_name,
                                'data': entry
                            }
                            all_signals.append(entry_signal)
                            
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
            except Exception as e:
                print(f"Error processing token {token_name}: {str(e)}")
                all_signals.append({
                    'type': 'error', 
                    'token_name': token_name, 
                    'message': str(e)
                })
                
        return all_signals
        
    except Exception as e:
        return [{'type': 'error', 'token_name': 'batch_processing', 'message': str(e)}]
    finally:
        # Clean up connection
        if 'conn_manager' in locals():
            conn_manager.close()

def prepare_dataframe(data):
    """
    Convert data list to DataFrame and apply necessary processing
    
    Args:
        data (list): List of dictionaries containing OHLCV data
        
    Returns:
        pd.DataFrame: Processed DataFrame ready for strategy application
    """
    if not data:
        return pd.DataFrame()
        
    # Convert to DataFrame
    df = pd.DataFrame(data)

    # Convert string values to numeric
    numeric_cols = ['ltp', 'open', 'high', 'low', 'close', 'volume']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Use datetime field directly if available, otherwise convert timestamp
    if 'datetime' in df.columns:
        df = df.set_index('datetime')
    elif 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'].astype(float)/1000, unit='s')
        df = df.set_index('timestamp')
    else:
        print(f"Error: No timestamp or datetime column found in data")
        return pd.DataFrame()
        
    # ClickHouse version sorted in descending order, so replicate that
    df = df.sort_index(ascending=False)
    
    # Calculate adjusted volume just like the ClickHouse version
    df['adjVol'] = df['volume'].diff(-1)
    
    # Sort again for resampling (ascending order)
    df = df.sort_index(ascending=True)
    
    # Now replicate the exact same resampling logic from ClickHouse version
    df = df.resample('5Min', closed='left', label='left').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'adjVol': 'sum'
    })
    
    # Reset index and rename to 'timestamp' to match the expected column name
    df = df.reset_index().rename(columns={'index': 'timestamp', 'datetime': 'timestamp'})
    
    # Calculate pivot points
    df['pivot'] = [pivotid(i, df, 3, 3) for i in range(len(df))]
    df['pointpos'] = df.apply(lambda row: pointpos(row), axis=1)
    
    # Calculate EMAs
    span10 = 2 / (10 + 1)
    span20 = 2 / (20 + 1)
    df['ema10'] = df['close'].ewm(alpha=span10, adjust=False).mean()
    df['ema20'] = df['close'].ewm(alpha=span20, adjust=False).mean()
    
    return df

def pivotid(row_idx, df1, n1, n2):
    """
    Detect pivot points by comparing with surrounding candles

    Args:
        row_idx: current row index
        df1: DataFrame containing OHLC data
        n1: number of candles to look back
        n2: number of candles to look forward
    """
    if row_idx - n1 < 0 or row_idx + n2 >= len(df1):
        return 0

    current_high = df1.iloc[row_idx]['high']

    # Check if current_high is NaN
    if pd.isna(current_high):
        return 0

    # Check previous n1 candles
    for i in range(row_idx - n1, row_idx):
        # Skip NaN values
        if pd.isna(df1.iloc[i]['high']):
            continue
        if current_high <= df1.iloc[i]['high']:
            return 0

    # Check next n2 candles
    for i in range(row_idx + 1, row_idx + n2 + 1):
        if pd.isna(df1.iloc[i]['high']):
            continue
        if current_high <= df1.iloc[i]['high']:
            return 0

    # If all checks passed, this is a pivot high
    return 2

def pointpos(x):
    if x['pivot']==1:
        return x['low']-1e-3
    elif x['pivot']==2:
        return x['high']+1e-3
    else:
        return np.nan

def strategy(t_df, stockname):
    """Implement the trading strategy"""

    entries = []
    # Get state for this symbol
    state = get_symbol_state(stockname)
    prev_pivot_high = state['prev_pivot_high']
    prev_day_pivot = state['prev_day_pivot']
    pivot_level = state['pivot_level']
    waiting_for_breakout = state['waiting_for_breakout']
    pivot_candle_index = state['pivot_candle_index']
    in_trade = state['in_trade']
    
    # Skip processing if already in a trade
    if in_trade:
        return []

    # Group by trading day
    t_df['trading_day'] = t_df['timestamp'].dt.normalize()
    t_df['price_volume'] = t_df['close'] * t_df['adjVol']
    grouped = t_df.groupby('trading_day')

    for day, day_df in grouped:
        # in_trade = False
        # stop_loss_price = None

        # Carry forward previous day's pivot
        if prev_day_pivot:
            first_open = day_df.iloc[0]['open']
            if first_open > prev_day_pivot:
                entry_price = first_open
                stop_loss_price = entry_price * 0.99
                entries.append({
                    'entry_time': day_df.iloc[0]['timestamp'],
                    'entry_price': entry_price,
                    'stop_loss': stop_loss_price,
                    'pivot_level': prev_day_pivot,
                    'entry_type': 'Gap Up',
                    'entry_reason': f'Gap up above previous day pivot {prev_day_pivot:.2f}',
                    'symbol': stockname,
                    'ema10': float(day_df.iloc[0]['ema10']) if 'ema10' in day_df.columns else None
                })
                in_trade = True
                state['stop_loss_price'] = stop_loss_price
                break  # Exit the loop since we found an entry
            prev_day_pivot = None
        
        # Skip the rest if we're already in a trade
        if in_trade:
            break

        # Process intraday candles
        for idx, row in day_df.iterrows():
            current_time = row['timestamp'].time()
            exit_time = datetime.strptime('15:20', '%H:%M').time()

            # Skip entry logic if too late in the day
            if current_time >= exit_time:
                continue

            # Process pivot points and look for entry opportunities
            if row['pivot'] == 2:
                prev_pivot_high = row['high']
                if row['ema10'] > row['ema20'] and idx >= 10:
                    pivot_level = row['high']
                    pivot_candle_index = idx
                    waiting_for_breakout = True

            # Check for breakout entry
            if waiting_for_breakout and not in_trade and pivot_level is not None and current_time < exit_time:
                if idx > pivot_candle_index:
                    entry_candle_index = idx
                    if entry_candle_index >= 11:
                        tenth_prev_close = float(t_df['close'].iloc[entry_candle_index - 11])
                        current_close = float(t_df['close'].iloc[entry_candle_index - 1])
                        movement_percentage = ((current_close - tenth_prev_close) / tenth_prev_close) * 100
                        recent_candles = t_df.iloc[max(0, entry_candle_index-6):entry_candle_index-1]
                        volume_price_sum = recent_candles['price_volume'].sum()

                        if movement_percentage > 20 and volume_price_sum > 1000000:
                            entry_price = None
                            if float(row['open']) > pivot_level:
                                entry_price = float(row['open'])
                            elif float(row['high']) > pivot_level:
                                entry_price = pivot_level

                            if entry_price:
                                percentage_diff = ((entry_price - pivot_level) / pivot_level) * 100
                                if percentage_diff <= 20:
                                    stop_loss_price = entry_price * 0.99
                                    entries.append({
                                        'entry_time': row['timestamp'],
                                        'entry_price': entry_price,
                                        'stop_loss': stop_loss_price,
                                        'pivot_level': pivot_level,
                                        'entry_type': 'Breakout',
                                        'entry_reason': f'Breakout above pivot {pivot_level:.2f} with {movement_percentage:.2f}% movement',
                                        'movement_percentage': movement_percentage,
                                        'symbol': stockname
                                    })
                                    in_trade = True
                                    state['stop_loss_price'] = stop_loss_price
                                    break
            
            # Exit the loop if we found an entry
            if in_trade:
                break

        # Store end of day pivot
        if len(day_df) > 0:
            last_row = day_df.iloc[-1]
            if last_row['pivot'] == 2:
                prev_day_pivot = last_row['high']
            else:
                prev_day_pivot = None

    # Update state
    state['prev_pivot_high'] = prev_pivot_high
    state['prev_day_pivot'] = prev_day_pivot
    state['pivot_level'] = pivot_level
    state['waiting_for_breakout'] = waiting_for_breakout
    state['pivot_candle_index'] = pivot_candle_index
    state['in_trade'] = in_trade

    return entries

def print_signal(signal):
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

def get_current_prices_batch(tokens, conn_manager):
    """
    Fetch current prices for multiple tokens at once
    
    Args:
        tokens (dict): Dictionary of token_name: token_value pairs
        conn_manager (RedisConnectionManager): Connection manager to fetch data
        
    Returns:
        dict: Dictionary of token_name: current_price pairs
    """
    try:
        client = conn_manager.get_client()
        prefix = '13mar2025'
        results = {}
        
        # Create pipeline
        pipe = client.pipeline(transaction=False)
        
        # For each token, query the latest data
        for token_name, token_value in tokens.items():
            # First check for an index
            sorted_key = f"{prefix}:{token_value}:index"
            pipe.exists(sorted_key)
            
        # Execute pipeline to check indices
        index_results = pipe.execute()
        
        # Reset pipeline for data retrieval
        pipe = client.pipeline(transaction=False)
        
        # Prepare to fetch latest data
        index_mapping = {}
        i = 0
        for token_name, token_value in tokens.items():
            sorted_key = f"{prefix}:{token_value}:index"
            
            if index_results[i]:
                # Get the latest key from sorted set
                pipe.zrange(sorted_key, -1, -1)
                index_mapping[len(index_mapping)] = token_name
                
            i += 1
        
        # Execute pipeline to get latest keys
        key_results = pipe.execute()
        
        # Reset pipeline for data values
        pipe = client.pipeline(transaction=False)
        
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
        
        return results
        
    except Exception as e:
        print(f"Error fetching batch current prices: {str(e)}")
        return {}

#tried to print the current EMA 10 value with every iteration
def print_active_trades(active_trades, conn_manager):
    """Print a summary of all currently active trades"""
    if not active_trades:
        print("\nNo active trades currently running")
        return

    print("\n=== ACTIVE TRADES ===")
    print(f"Total Active Trades: {len(active_trades)}")
    print("-" * 100)
    print(f"{'Symbol':<20} {'Entry Time':<25} {'Entry Price':>10} {'Current':>10} {'P/L %':>10} {'Stop Loss':>10} {'EMA10':>10}")
    print("-" * 100)
    
    # Build a batch query for current prices
    tokens_to_query = {symbol: fno_token.get(symbol) for symbol in active_trades.keys()}
    current_prices = get_current_prices_batch(tokens_to_query, conn_manager)
    
    
    for symbol, trade in active_trades.items():
        # Get current price from batch results
        current_price = current_prices.get(symbol)
        # Get the EMA10 value if it exists in the trade data
        ema10_value = trade.get('ema10', 'N/A')
        if isinstance(ema10_value, (int, float)):
            ema10_display = f"{ema10_value:.2f}"
        else:
            ema10_display = 'N/A'
        
        if current_price:
            pnl_percentage = ((current_price - trade['entry_price']) / trade['entry_price']) * 100
            print(f"{symbol:<20} {str(trade['entry_time']):<25} {trade['entry_price']:>10.2f} {current_price:>10.2f} {pnl_percentage:>10.2f} {trade['stop_loss']:>10.2f} {ema10_display:<10}")
        else:
            print(f"{symbol:<20} {str(trade['entry_time']):<25} {trade['entry_price']:>10.2f} {'N/A':>10} {'N/A':>10} {trade['stop_loss']:>10.2f} {ema10_display:<10}")
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

def check_for_exits(active_trades, conn_manager, signal_tracker):
    """
    Check if any active trades need to exit based on current prices
    
    Args:
        active_trades (dict): Dictionary of active trades
        conn_manager (RedisConnectionManager): Connection manager for data retrieval
        signal_tracker (SignalTracker): Ray actor for tracking signals
        
    Returns:
        int: Number of trades exited
    """
    if not active_trades:
        return 0
    
    trades_to_exit = []
    current_time = datetime.now().time()
    exit_time = datetime.strptime('15:20', '%H:%M').time()
    
    # Build a batch query for current prices - more efficient than individual queries
    tokens_to_query = {symbol: fno_token.get(symbol) for symbol in active_trades.keys()}
    # current_prices = get_current_prices_batch(tokens_to_query, conn_manager)
    # Get the latest data with indicators for each active trade symbol
    token_data = conn_manager.get_data_batch(tokens_to_query)
    
    # Process each active trade
    for symbol, trade in active_trades.items():
        # current_price = current_prices.get(symbol)
        data = token_data.get(symbol, [])
        if not data:
            # If we can't get complete data, at least try to get current price
            current_prices = get_current_prices_batch(tokens_to_query, conn_manager)
            current_price = current_prices.get(symbol)
            current_ema10 = None
        else:
            # Get the latest candle with full indicators
            t_df = prepare_dataframe(data)
            if len(t_df) == 0:
                continue
                
            # Get latest values from the dataframe
            latest_row = t_df.iloc[-1]
            current_price = latest_row['close']
            current_ema10 = latest_row['ema10'] if 'ema10' in latest_row else None
        
        if not current_price:
            continue
        
        # Vectorize exit conditions
        stop_loss_hit = current_price <= trade['stop_loss']
        ema_exit = current_price < current_ema10
        time_exit = current_time >= exit_time

        exit_data = None
        if stop_loss_hit or ema_exit or time_exit:
            # Check stop loss
            if current_price <= trade['stop_loss']:
                exit_data = {
                    'exit_time': datetime.now(),
                    'exit_price': trade['stop_loss'],
                    'exit_type': 'Stop Loss',
                    'exit_reason': f'Price hit stop loss at {trade["stop_loss"]:.2f} (1% below entry)',
                    'entry_time': trade['entry_time'],
                    'entry_price': trade['entry_price'],
                    'profit_loss': -1.00,
                    'entry_type': trade['entry_type']
                }
            # Check EMA exit - requires fetching the latest EMA
            elif current_price < current_ema10:
                profit_loss = ((current_price - trade['entry_price']) / trade['entry_price']) * 100
                exit_data = {
                    'exit_time': datetime.now(),
                    'exit_price': current_price,
                    'exit_type': 'EMA Exit',
                    'exit_reason': f'Price broke below EMA at {current_ema10:.2f}',
                    'entry_time': trade['entry_time'],
                    'entry_price': trade['entry_price'],
                    'profit_loss': profit_loss,
                    'entry_type': trade['entry_type']
                }
            # Check time exit
            elif current_time >= exit_time:
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

    return len(trades_to_exit)

def main():
    """
    Main function to run the trading system
    """
    try:
        # Initialize Ray and SignalTracker
        if not ray.is_initialized():
            ray.init()
        signal_tracker = SignalTracker.remote()
        # Create a connection manager for the main process
        main_conn_manager = RedisConnectionManager(pool_size=20)

        print("\nTrading system started...")
        # print(f"Processing {len(fno_token)} tokens in batches")

        # Batch the tokens to improve performance
        batch_size = 25  # Adjust based on token count and system capacity
        token_batches = batch_tokens(fno_token, batch_size)
        # print(f"Split into {len(token_batches)} batches of up to {batch_size} tokens each")

        batch_interval = 0.5  # Time in seconds between batch processing cycles

        # tracker = SummaryTracker()
        iteration = 0
        while True:
            iteration += 1
            current_time = datetime.now().time()
            market_end = datetime.strptime('15:25', '%H:%M').time()
            
            # Check if it's time to end
            if current_time >= market_end:
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

            # Print current status and time
            print(f"\nChecking for signals at {datetime.now()}")

            try:
                
                # Get and print active trades using optimized batch method
                active_trades = ray.get(signal_tracker.get_active_trades.remote())
                print_active_trades(active_trades, main_conn_manager)

                if active_trades : check_for_exits(active_trades, main_conn_manager, signal_tracker)
                

                # Process token batches with optimized parallel execution
                # process_start = time.time()
                # print(f"Processing {len(token_batches)} batches of tokens...")
                
                # Launch all batch processing in parallel
                futures = [process_token_batch.remote(batch, signal_tracker) for batch in token_batches]
                
                # Process results as they arrive
                all_signals = []
                for future in futures:
                    try:
                        batch_signals = ray.get(future)
                        all_signals.extend(batch_signals)
                    except Exception as e:
                        print(f"Error processing batch: {str(e)}")
                
                # Process all signals in a batch on the signal tracker
                if all_signals:
                    new_signals = ray.get(signal_tracker.add_batch_signals.remote(all_signals))
                    
                    # Print only the new signals
                    for signal in new_signals:
                        print_signal(signal)
                
                # process_end = time.time()
                # print(f"Processed all batches in {process_end - process_start:.2f} seconds")
                if iteration % 10 == 0:  # Every 10 iterations
                    import gc
                    collected = gc.collect()
                    print(f"Garbage collected {collected} objects")

            except Exception as e:
                print(f"Error in main processing loop: {str(e)}")
                import traceback
                traceback.print_exc()
                time.sleep(5)  # Brief pause before retrying
                continue

            # Sleep between iterations - we can use a longer interval due to batch processing
            # print(f"Waiting {batch_interval} seconds before next cycle...")
            time.sleep(batch_interval)
        # tracker.print_diff()
    except KeyboardInterrupt:
        print("\nReceived keyboard interrupt. Shutting down gracefully...")
    except Exception as e:
        print(f"\nFatal error in main function: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        try:
            # Clean shutdown procedures
            print("\nCleaning up resources...")
            if 'main_conn_manager' in locals():
                main_conn_manager.close()
            if ray.is_initialized():
                ray.shutdown()
            print("Cleanup completed. System shutdown.")
        except Exception as e:
            print(f"Error during cleanup: {str(e)}")

def check_token_file_exists():
    """
    Check if the token file for today already exists.
    Returns True if it exists, False otherwise.
    """
    try:
        # Get current date to find the token file
        current_date = datetime.now()
        month_abbr = current_date.strftime("%b").lower()
        day = str(current_date.day)  # No leading zero
        year = current_date.strftime("%Y")
        date_str = f"{month_abbr}{day}{year}"

        # Set the full path for the token file
        tokens_dir = os.path.join(os.getcwd(), "tokens")
        filename = os.path.join(tokens_dir, f"{date_str}token.txt")

        print(f"Checking for token file: {filename}")
        exists = os.path.exists(filename) and os.path.getsize(filename) > 0

        if exists:
            print(f"Found token file for today: {filename}")
        else:
            print(f"Token file not found or empty: {filename}")

        return exists

    except Exception as e:
        print(f"Error checking token file: {str(e)}")
        return False

def initialize_tokens():
    """Initialize the global fno_token dictionary"""
    global fno_token

    # Try to run tokengen and load tokens
    if run_tokengen():
        fno_token = load_tokens()

    # If token dictionary is empty, use a small default dictionary for testing
    if not fno_token:
        print("ERROR: Failed to generate or load tokens. Cannot proceed without valid token data.")
        print("Please ensure tokengen.py runs successfully and generates token files.")
        sys.exit(1)  # Exit with error code 1

    print(f"Loaded {len(fno_token)} tokens")

def setup_redis_data_structure():
    """
    Set up the Redis data structure for storing market data.
    """
    try:
        # Create a Redis client
        client = get_redis_client()

        # Check if Redis is running
        if client.ping():
            print("Connected to Redis server. Setting up data structures...")

            # Create a sample key with metadata to document the structure
            info_key = "13mar2025:info"
            if not client.exists(info_key):
                client.hset(info_key, mapping={
                    "description": "Market data storage for March 2025",
                    "structure": "13mar2025:{token}:{timestamp} -> Hash with OHLCV data",
                    "fields": "symbol, ltp, open, high, low, close, volume",
                    "created_at": str(datetime.now())
                })
                print("Created Redis data structure documentation")

            print("Redis setup complete")
            client.close()
            return True
        else:
            print("Failed to connect to Redis server")
            return False
    except Exception as e:
        print(f"Error setting up Redis: {str(e)}")
        return False

if __name__ == "__main__":
    try:
        # Set up Redis data structure
        if not setup_redis_data_structure():
            print("WARNING: Failed to set up Redis data structure. Continuing anyway...")

        # Check if token file already exists
        if not check_token_file_exists():
            print("Token file for today doesn't exist. Generating new tokens...")
            initialize_tokens()
        else:
            print("Token file for today already exists. Loading tokens...")
            fno_token = load_tokens()

            # Exit if no tokens were loaded
            if not fno_token:
                print("ERROR: No tokens available. Cannot proceed without token data.")
                print("Please ensure tokengen.py runs successfully and generates token files.")
                sys.exit(1)  # Exit with error code 1

        # Run the main function
        main()

    except KeyboardInterrupt:
        print("\nScript terminated by user")
        if 'ray' in sys.modules and ray.is_initialized():
            ray.shutdown()
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()
        if 'ray' in sys.modules and ray.is_initialized():
            ray.shutdown()
