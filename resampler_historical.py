'''
pivot sliding window
'''
import redis
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import json
import logging
import threading
import ray
import flattrade_config

# Get authenticated API and token
api, token = flattrade_config.get_flattrade_api()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("resampler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("RedisResampler")

def get_date_prefix():
    now = datetime.now()
    day = now.day
    month = now.strftime("%b").lower()
    year = now.strftime("%Y")
    return f"{day}{month}{year}"

# Constants
SOURCE_DB = 0  # Database index for raw ticker data (18mar2025)
RESAMPLED_DB = 1  # Database index for resampled data (19mar2025resampled)

RESAMPLE_INTERVAL = "5Min"

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


# Create a Redis connection manager for better connection handling
class RedisConnection:
    _pools = {}  # Class-level dictionary to store connection pools by DB
    
    @classmethod
    def get_pool(cls, db=0, max_connections=5):
        """Get or create a connection pool for the specified DB"""
        if db not in cls._pools:
            cls._pools[db] = redis.ConnectionPool(
                host='localhost',
                port=6379,
                db=db,
                decode_responses=True,
                max_connections=max_connections,
                socket_timeout=5.0,  # Add timeout
                socket_keepalive=True,  # Enable keepalive
                health_check_interval=30  # Add health checks
            )
        return cls._pools[db]
    
    def __init__(self, db=0):
        self.db = db
        self.client = None
        
    def __enter__(self):
        """Allow using with 'with' statement"""
        self.client = redis.Redis(connection_pool=self.get_pool(self.db))
        return self.client
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensure connection is returned to pool when exiting context"""
        if self.client:
            try:
                self.client.connection_pool.disconnect()
            except Exception as e:
                logger.warning(f"Error closing Redis connection: {str(e)}")
            finally:
                self.client = None
                
    def get_client(self):
        """Get a Redis client from the appropriate connection pool"""
        if self.client is None:
            self.client = redis.Redis(connection_pool=self.get_pool(self.db))
        return self.client
        
    def close(self):
        """Safely close the connection"""
        if self.client is not None:
            try:
                self.client.close()
            except Exception as e:
                logger.warning(f"Error closing Redis connection: {str(e)}")
            finally:
                self.client = None

def prepare_dataframe(data):
    """
    Convert data list to DataFrame and resample it
    
    Args:
        data (list): List of dictionaries containing OHLCV data
        
    Returns:
        pd.DataFrame: Resampled DataFrame
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
    df['adjVol'] = df['volume']
    
    # Sort again for resampling (ascending order)
    df = df.sort_index(ascending=True)
    
    df = df.resample('5Min', closed='left', label='left').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    })
    
    df['close'] = df['close'].replace(0, np.nan).ffill()
    
    # Then replace zeros in other columns with the corresponding close value
    for col in ['open', 'high', 'low']:
        # Replace zeros with NaN first
        df[col] = df[col].replace(0, np.nan)
        # Then fill NaN with the corresponding close value
        df[col] = df[col].fillna(df['close'])
    
    
    # Calculate adjusted volume just like the ClickHouse version
    df['adjVol'] = df['volume']

    if 'volume' in df.columns and 'adjVol' not in df.columns:
        df = df.rename(columns={'volume': 'adjVol'})
    
    
    # Reset index and rename to 'timestamp' to match the expected column name
    df = df.reset_index().rename(columns={'index': 'timestamp', 'datetime': 'timestamp'})
    
    # Calculate pivot points
    df['pivot'] = optimize_pivot_calculations(df)
    df['pointpos'] = df.apply(lambda row: pointpos(row), axis=1)
    
    # Calculate EMAs
    # span10 = 2 / (10 + 1)
    # span20 = 2 / (20 + 1)
    # df['ema10'] = df['close'].ewm(alpha=span10, adjust=False).mean()
    # df['ema20'] = df['close'].ewm(alpha=span20, adjust=False).mean()
    df['ema10'] = calculate_ema(df['close'].values, 10)
    df['ema20'] = calculate_ema(df['close'].values, 20)
    
    return df

def optimize_pivot_calculations(df):
    """
    Efficiently calculate pivot points using a sliding window approach.
    Returns a numpy array of pivot values (0, 1, or 2) for each row.
    """
    n_rows = len(df)
    lookback = 3  # Number of candles to look back
    lookforward = 3  # Number of candles to look forward
    
    # Pre-allocate the results array
    pivot_values = np.zeros(n_rows, dtype=np.int8)
    
    # Extract high and low values as numpy arrays for fast comparison
    highs = df['high'].values
    lows = df['low'].values
    
    # We can only calculate pivots for rows that have enough data before and after
    for i in range(lookback, n_rows - lookforward):
        # Check if current high is greater than all highs in the window
        current_high = highs[i]
        is_pivot_high = True
        
        # Check previous candles
        for j in range(i - lookback, i):
            if current_high <= highs[j]:
                is_pivot_high = False
                break
                
        # If still potentially a pivot high, check forward candles
        if is_pivot_high:
            for j in range(i + 1, i + lookforward + 1):
                if current_high <= highs[j]:
                    is_pivot_high = False
                    break
        
        # If it's a pivot high, mark it as 2 in our results
        if is_pivot_high:
            pivot_values[i] = 2
            
        # Similar logic for pivot lows can be added here if needed
    
    return pivot_values

def get_lot_sizes_from_json(token_dict):
    """
    Get lot sizes from Angel Broking's master contract JSON file for specific tokens
    
    Args:
        token_dict (dict): Dictionary of token_name: token_value pairs
        
    Returns:
        dict: Dictionary of token_value: lot_size pairs
    """
    import requests
    import json
    
    lot_sizes = {}
    url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    
    # Get a list of token values we need
    token_values = list(token_dict.values())
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            
            # Filter for only the tokens we need
            tokens_found = 0
            for instrument in data:
                if 'token' in instrument and 'lotsize' in instrument and instrument['token'] in token_values:
                    token = instrument['token']
                    lot_size = int(instrument['lotsize'])
                    lot_sizes[token] = lot_size
                    tokens_found += 1
                    
                    # If we found all our tokens, we can stop processing
                    if tokens_found == len(token_values):
                        break
            
            # logger.info(f"Loaded lot sizes for {len(lot_sizes)} out of {len(token_values)} tokens")
            
            # Log any tokens we couldn't find
            missing_tokens = [token for token in token_values if token not in lot_sizes]
            if missing_tokens:
                logger.warning(f"Could not find lot sizes for {len(missing_tokens)} tokens: {missing_tokens[:5]}...")
        else:
            logger.warning(f"Failed to fetch lot sizes. Status code: {response.status_code}")
    
    except Exception as e:
        logger.error(f"Error fetching lot sizes from JSON: {str(e)}")
    
    return lot_sizes

def update_historical_pivot_values(token_name, token_value, resampled_client):
    """
    Update pivot values for previously stored candles with efficient Redis pipelining
    """
    prefix = RESAMPLED_PREFIX
    
    # Get all candles from Redis for this token
    index_key = f"{prefix}:{token_value}:index"
    candle_keys = resampled_client.zrange(index_key, 0, -1)
    
    if len(candle_keys) < 7:  # need at least 7 candles (3 before + current + 3 after)
        return  # Not enough candles to perform update
    
    # Use pipeline to fetch all data at once
    pipe = resampled_client.pipeline(transaction=False)
    for key in candle_keys:
        # pipe.hmget(key, ['timestamp', 'high', 'low'])
        pipe.hgetall(key)
    
    # Execute pipeline once to get all data
    all_data = pipe.execute()
    
    # Process the results
    candle_data = []
    timestamp_to_key = {}
    
    for i, key in enumerate(candle_keys):
        data = all_data[i]
        if data:
            # Convert string fields to appropriate types
            try:
                data['timestamp_ms'] = int(data.get('timestamp', 0))
                data['high'] = float(data.get('high', 0))
                data['low'] = float(data.get('low', 0))
                data['open'] = float(data.get('open', 0))
                data['close'] = float(data.get('close', 0))
                data['timestamp'] = pd.to_datetime(int(data['timestamp_ms'])/1000, unit='s')
                candle_data.append(data)
                timestamp_to_key[data['timestamp_ms']] = key
            except (ValueError, TypeError) as e:
                continue
    
    # Sort by timestamp
    df = pd.DataFrame(candle_data).sort_values('timestamp')
    df = df.reset_index(drop=True)  # Reset index for proper pivotid calculation
    
    # Track the latest pivot high
    latest_pivot_index = None
    latest_pivot_timestamp = None
    
    # Prepare pipeline for updates
    update_pipe = resampled_client.pipeline(transaction=False)
    pivot_updates = 0
    pivot_values = optimize_pivot_calculations(df)
    
    # Recalculate pivot values for all applicable candles
    for i in range(len(df)):
        if i < 3 or i >= len(df) - 3:
            continue  # Skip candles at edges that don't have enough neighbors
            
        pivot_value = pivot_values[i]
        row = df.iloc[i]
        ts_ms = row['timestamp_ms']
        redis_key = timestamp_to_key.get(ts_ms)
        
        if not redis_key:
            continue
            
        # Add pivot update to pipeline
        update_pipe.hset(redis_key, 'pivot', str(int(pivot_value)))
        pivot_updates += 1
        
        # Update pivot_candle_index
        if pivot_value > 0:
            update_pipe.hset(redis_key, 'pivot_candle_index', str(i))
            
            # Track latest pivot high
            if pivot_value == 2 and (latest_pivot_timestamp is None or ts_ms > latest_pivot_timestamp):
                latest_pivot_index = i
                latest_pivot_timestamp = ts_ms
        else:
            update_pipe.hset(redis_key, 'pivot_candle_index', '')
    
    # Update pivot tracking for the token
    if latest_pivot_index is not None:
        pivot_key = f"{prefix}:pivot_index:{token_name}"
        updated_data = {
            'token_name': token_name,
            'pivot_index': str(latest_pivot_index),
            'timestamp': str(latest_pivot_timestamp),
            'last_updated': str(int(time.time() * 1000))
        }
        update_pipe.hset(pivot_key, mapping=updated_data)
    
    # Execute all updates in a single pipeline
    if pivot_updates > 0:
        update_pipe.execute()

def load_historical_data(token_dict, days_back=10):
    """
    Fetches historical data for tokens and adds them to the resampled database
    
    Args:
        token_dict (dict): Dictionary of token_name: token_value pairs
        days_back (int): Number of days to look back for historical data
    """
    logger.info(f"Loading historical data for {len(token_dict)} tokens")
  
    api.get_limits()

    
    # Get lot sizes for volume calculation
    lot_sizes = get_lot_sizes_from_json(token_dict)
    
    # Find the last business day with data
    BusDay = 1
    feed_json = {}
    
    # Keep going back until we find a day with sufficient data
    while True:
        lastBusDay = datetime.today()-timedelta(days=BusDay)
        start_lastBusDay = lastBusDay.replace(hour=9, minute=15, second=0, microsecond=0)
        end_lastBusDay = lastBusDay.replace(hour=15, minute=30, second=0, microsecond=0)

        # Try to get data for a reference token to check if the day has data

        try:            
            ret = api.get_time_price_series(exchange='NSE', token="26000", 
                                            starttime=start_lastBusDay.timestamp(),endtime = end_lastBusDay.timestamp(), 
                                            interval="5")
            df = pd.DataFrame(ret)
            df = df[::-1]

            if len(df) > 10:
                break

        except Exception as e:
            logger.warning(f"Error checking business day: {str(e)}")
        
        BusDay = BusDay + 1
        if BusDay > days_back:  # Avoid infinite loop
            logger.error(f"Could not find a valid business day within {days_back} days")
            return

    for token_name,token_value in token_dict.items():
        try:
            ret = api.get_time_price_series(exchange='NFO', token=token_value,
                                            starttime=start_lastBusDay.timestamp(),
                                            endtime = end_lastBusDay.timestamp(), interval="5")
            df = pd.DataFrame(ret)
            df = df[::-1]
            if len(df) > 1:
                feed_json[token_name] = df
                logger.info(f"Fetched {len(df)} candles for {token_name}")
            else:
                logger.warning(f"Insufficient data for {token_name}: only {len(df)} candles found")
        except Exception as e:
            logger.error(f"Error fetching data for {token_name}: {str(e)}")
    
    # Process the fetched data
    with RedisConnection(db=RESAMPLED_DB) as resampled_client:
        for token_name, df in feed_json.items():
            token_value = token_dict[token_name]
            lot_size = lot_sizes.get(token_value, 550)  # Default lot size if not found
            
            try:
                # Convert DataFrame to format expected by prepare_dataframe
                data_list = []
                for _, row in df.iterrows():
                    entry = {
                        'open': row.get('into', 0),
                        'high': row.get('inth', 0),
                        'low': row.get('intl', 0),
                        'close': row.get('intc', 0),
                        'volume': row.get('intv', 0),
                        'timestamp': int(float(row.get('ssboe')) * 1000)  # Convert to milliseconds
                    }
                    data_list.append(entry)
                
                # Prepare and resample data
                resampled_df = prepare_dataframe(data_list)
                
                
                if not resampled_df.empty:
                    # Store the resampled data
                    stored_count = store_historical_resampled_data(resampled_df, token_name, token_value, 
                                                                  resampled_client, lot_size)
                    logger.info(f"Loaded historical data for {token_name}: {stored_count} candles")
                    
                    # Update pivot values
                    update_historical_pivot_values(token_name, token_value, resampled_client)
                else:
                    logger.warning(f"No data after resampling for {token_name}")
            
            except Exception as e:
                logger.error(f"Error processing data for {token_name}: {str(e)}")
    
    logger.info(f"Completed loading historical data for {len(feed_json)} tokens")

@ray.remote
def get_data_batch(tokens, source_db=0, since_timestamp=None):
    """
    Get raw data for multiple tokens at once using Redis pipeline
    
    Args:
        tokens (dict): Dictionary of token_name: token_value pairs
        source_db (int): Redis database index
        since_timestamp (float, optional): Only fetch data newer than this timestamp
            
    Returns:
        dict: Dictionary of token_name: data_list pairs
    """
    try:
    
        # Create Redis client for this Ray task
        client = redis.Redis(host='localhost', port=6379, db=source_db, decode_responses=True)
        # conn = RedisConnection(db=source_db)
        # client = conn.get_client()
        prefix = SOURCE_PREFIX
        result = {}

        if since_timestamp is not None:
            # Convert to milliseconds if it's in seconds
            redis_timestamp = int(since_timestamp * 1000)
        else:
            redis_timestamp = None
        
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
        
        # For each token, either use index or scan
        for i, (token_name, token_value) in enumerate(tokens.items()):
            sorted_key = f"{prefix}:{token_value}:index"
            
            if index_exists_results[i]:
                if redis_timestamp is None:
                    # Get all keys from the index
                    pipe.zrange(sorted_key, 0, -1)
                else:
                    # Get only keys with timestamps greater than since_timestamp
                    # Converting timestamp to Redis score format if needed
                    pipe.zrangebyscore(sorted_key, redis_timestamp, "+inf")
            else:
                # No index, initialize with empty list
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
        
        # Close the Redis client
        try:
            client.close()
        except:
            pass
            
        return result
        

    except Exception as e:
        error_msg = f"Error retrieving batch data from Redis: {str(e)}"
        print(error_msg)  # Print for Ray worker visibility
        return {'error': error_msg}
    
    finally:
        if client:
            client.close()

def update_pivot_values(token_name, token_value, resampled_client):
    """
    Update pivot values for previously stored candles with efficient Redis pipelining
    """
    prefix = RESAMPLED_PREFIX

    # First, check if we have a latest pivot index already stored
    pivot_key = f"{prefix}:pivot_index:{token_name}"
    latest_pivot_data = resampled_client.hgetall(pivot_key)
    
    # Get all candles from Redis for this token
    index_key = f"{prefix}:{token_value}:index"
    candle_keys = resampled_client.zrange(index_key, 0, -1)
    
    if len(candle_keys) < 7:  # need at least 7 candles (3 before + current + 3 after)
        return  # Not enough candles to perform update
    
    # Determine start index based on latest pivot
    start_index = 0
    if latest_pivot_data and 'pivot_index' in latest_pivot_data:
        latest_pivot_index = int(latest_pivot_data['pivot_index'])
        # We only need to check from (latest_pivot_index + 3) onwards
        # Since a new pivot can only form after at least 3 candles from the previous one
        start_index = latest_pivot_index + 3
        
        # If we're already at the end, there's nothing new to process
        if start_index >= len(candle_keys) - 3:
            return
    
    # Use pipeline to fetch all data at once
    pipe = resampled_client.pipeline(transaction=False)
    for key in candle_keys:
        # pipe.hmget(key, ['timestamp', 'high', 'low'])
        pipe.hgetall(key)
    
    # Execute pipeline once to get all data
    all_data = pipe.execute()
    
    # Process the results
    candle_data = []
    timestamp_to_key = {}
    
    for i, key in enumerate(candle_keys):
        data = all_data[i]
        if data:
            # Convert string fields to appropriate types
            try:
                data['timestamp_ms'] = int(data.get('timestamp', 0))
                data['high'] = float(data.get('high', 0))
                data['low'] = float(data.get('low', 0))
                data['open'] = float(data.get('open', 0))
                data['close'] = float(data.get('close', 0))
                data['timestamp'] = pd.to_datetime(int(data['timestamp_ms'])/1000, unit='s')
                candle_data.append(data)
                timestamp_to_key[data['timestamp_ms']] = key
            except (ValueError, TypeError) as e:
                continue
    
    # Sort by timestamp
    df = pd.DataFrame(candle_data).sort_values('timestamp')
    df = df.reset_index(drop=True)  # Reset index for proper pivotid calculation
    
    # Track the latest pivot high
    latest_pivot_index = None
    latest_pivot_timestamp = None
    
    # Prepare pipeline for updates
    update_pipe = resampled_client.pipeline(transaction=False)
    pivot_updates = 0
    pivot_values = optimize_pivot_calculations(df)
    
    # Recalculate pivot values for all applicable candles
    for i in range(len(df)):
        if i < 3 or i >= len(df) - 3:
            continue  # Skip candles at edges that don't have enough neighbors
            
        pivot_value = pivot_values[i]
        row = df.iloc[i]
        ts_ms = row['timestamp_ms']
        redis_key = timestamp_to_key.get(ts_ms)
        
        if not redis_key:
            continue
            
        # Add pivot update to pipeline
        update_pipe.hset(redis_key, 'pivot', str(int(pivot_value)))
        pivot_updates += 1
        
        # Update pivot_candle_index
        if pivot_value > 0:
            update_pipe.hset(redis_key, 'pivot_candle_index', str(i))
            
            # Track latest pivot high
            if pivot_value == 2 and (latest_pivot_timestamp is None or ts_ms > latest_pivot_timestamp):
                latest_pivot_index = i
                latest_pivot_timestamp = ts_ms
        else:
            update_pipe.hset(redis_key, 'pivot_candle_index', '')
    
    # Update pivot tracking for the token
    if latest_pivot_index is not None:
        pivot_key = f"{prefix}:pivot_index:{token_name}"
        updated_data = {
            'token_name': token_name,
            'pivot_index': str(latest_pivot_index),
            'timestamp': str(latest_pivot_timestamp),
            'last_updated': str(int(time.time() * 1000))
        }
        update_pipe.hset(pivot_key, mapping=updated_data)
    
    # Execute all updates in a single pipeline
    if pivot_updates > 0:
        update_pipe.execute()

def calculate_ema(prices, period):
    """
    Fast EMA calculation using the recursive formula that matches
    how trading platforms calculate EMAs.
    
    Args:
        prices (list or numpy array): List of price values
        period (int): EMA period
    
    Returns:
        numpy array: Array of EMA values with proper initialization
    """
    import numpy as np
    
    prices = np.array(prices, dtype=np.float32)
    alpha = 2 / (period + 1)
    ema = np.zeros_like(prices)
    
    # Initialize with SMA for the first period value
    if len(prices) >= period:
        ema[period-1] = np.mean(prices[:period])
    else:
        # Not enough data to compute a proper EMA
        return np.full_like(prices, np.nan)
    
    # Calculate EMA using the recursive formula from period onwards
    for i in range(period, len(prices)):
        ema[i] = prices[i] * alpha + ema[i-1] * (1 - alpha)
    
    # Set values before period to NaN or you could backfill them
    ema[:period-1] = np.nan
    
    return ema

def calculate_ongoing_ema(prices, period, last_ema=None):
    """
    Calculate EMA with continuity from last known value
    
    Args:
        prices (numpy array): Array of price values
        period (int): EMA period
        last_ema (float): Last known EMA value
        
    Returns:
        numpy array: Array of EMA values
    """
    import numpy as np
    
    prices = np.array(prices, dtype=np.float32)
    ema = np.zeros_like(prices)
    alpha = 2 / (period + 1)
    
    if last_ema is not None and last_ema > 0:
        # Use last known EMA value for first calculation
        ema[0] = prices[0] * alpha + last_ema * (1 - alpha)
    else:
        # Standard initialization if no seed value
        if len(prices) >= period:
            ema[period-1] = np.mean(prices[:period])
        else:
            # Not enough data
            return np.full_like(prices, np.nan)
    
    # Calculate remaining EMAs
    for i in range(1 if last_ema else period, len(prices)):
        ema[i] = prices[i] * alpha + ema[i-1] * (1 - alpha)
    
    # Set values before period to NaN if no seed
    if not last_ema and period > 1:
        ema[:period-1] = np.nan
    
    return ema

def prepare_new_data(data, token_name, token_value, resampled_client):
    """
    Prepare and resample only the new data, merging with existing data if needed
   
    Args:
        data (list): List of new data dictionaries
        token_name (str): Symbol/name of the token
        token_value (str): Token value/ID
        resampled_client: Redis client for resampled DB
       
    Returns:
        pd.DataFrame: Resampled DataFrame with only new/updated intervals
    """
    if not data:
        return pd.DataFrame()
   
    # Convert new data to DataFrame
    new_df = pd.DataFrame(data)
   
    # Convert string values to numeric
    numeric_cols = ['ltp', 'open', 'high', 'low', 'close', 'volume']
    # dtype_dict = {col: 'float32' for col in numeric_cols}
    # new_df = pd.DataFrame(data).astype(dtype_dict, errors='ignore')

    for col in numeric_cols:
        if col in new_df.columns:
            new_df[col] = pd.to_numeric(new_df[col], errors='coerce')
   
    # Process timestamps
    if 'datetime' in new_df.columns:
        new_df = new_df.set_index('datetime')
    elif 'timestamp' in new_df.columns:
        new_df['timestamp'] = pd.to_datetime(new_df['timestamp'].astype(float)/1000, unit='s')
        new_df = new_df.set_index('timestamp')
    else:
        print(f"Error: No timestamp or datetime column found in data")
        return pd.DataFrame()
   
    # Sort index
    new_df = new_df.sort_index(ascending=False)  # Sort descending first like in prepare_dataframe
   
    # Calculate adjusted volume like in the original prepare_dataframe
    new_df['adjVol'] = new_df['volume'].diff(-1).abs()
   
    # Sort again for resampling (ascending order)
    new_df = new_df.sort_index(ascending=True)
   
    # Get the time range of new data to determine which intervals need updating
    if not new_df.empty:

        # Resample only the new data
        resampled_df = new_df.resample('5Min', closed='left', label='left').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'adjVol': 'sum'
        })

        prev_close = resampled_df['close'].replace(0, np.nan).ffill()
    
        # Replace zeros in one vectorized operation for each column
        for col in ['open', 'high', 'low', 'close']:
            resampled_df[col] = resampled_df[col].mask(resampled_df[col] == 0, prev_close)
        
       
        # Reset index and rename
        resampled_df = resampled_df.reset_index().rename(columns={'index': 'timestamp', 'datetime': 'timestamp'})

        if resampled_df.empty:
            return pd.DataFrame()
            
        # Calculate interval timestamps - needed for checking if they already exist
        interval_timestamps = [int(ts.timestamp() * 1000) for ts in resampled_df['timestamp']]
        
        # Check which intervals already exist in Redis
        prefix = RESAMPLED_PREFIX
        pipe = resampled_client.pipeline(transaction=False)
        for ts in interval_timestamps:
            key = f"{prefix}:{token_value}:{ts}"
            pipe.exists(key)
        
        exist_results = pipe.execute()
        
        # Filter to include only new intervals
        new_intervals = []
        for i, exists in enumerate(exist_results):
            if not exists:  # Only include intervals that don't exist yet
                new_intervals.append(i)
                
        # Filter the resampled dataframe to include only new intervals
        if new_intervals:
            resampled_df = resampled_df.iloc[new_intervals]
       
        # Calculate pivot points
        # resampled_df['pivot'] = [pivotid(i, resampled_df, 3, 3) for i in range(len(resampled_df))]
        resampled_df['pivot'] = optimize_pivot_calculations(resampled_df)
        resampled_df['pointpos'] = resampled_df.apply(lambda row: pointpos(row), axis=1)
       
        # Calculate EMAs
        # span10 = 2 / (10 + 1)
        # span20 = 2 / (20 + 1)
        # resampled_df['ema10'] = resampled_df['close'].ewm(alpha=span10, adjust=False).mean()
        # resampled_df['ema20'] = resampled_df['close'].ewm(alpha=span20, adjust=False).mean()  
        #       
        # resampled_df['ema10'] = calculate_ema(resampled_df['close'].values, 10)
        # resampled_df['ema20'] = calculate_ema(resampled_df['close'].values, 20)

        # Get last EMA values
        last_emas = None
        last_record = resampled_client.zrange(f"{RESAMPLED_PREFIX}:{token_value}:index", -1, -1)
        if last_record:
            data = resampled_client.hgetall(last_record[0])
            if 'ema10' in data and 'ema20' in data:
                try:
                    last_ema10 = float(data['ema10'])
                    last_ema20 = float(data['ema20'])
                    last_emas = (last_ema10, last_ema20)
                except:
                    pass

        # Then calculate EMAs with continuity
        close_values = resampled_df['close'].values
        if last_emas:
            resampled_df['ema10'] = calculate_ongoing_ema(close_values, 10, last_emas[0])
            resampled_df['ema20'] = calculate_ongoing_ema(close_values, 20, last_emas[1])
        else:
            resampled_df['ema10'] = calculate_ongoing_ema(close_values, 10)
            resampled_df['ema20'] = calculate_ongoing_ema(close_values, 20)

        return resampled_df
   
    return pd.DataFrame()

@ray.remote
def process_token_batch(token_batch, last_run_time=None):
    """
    Process a batch of tokens: get data, resample, and store
    
    Args:
        token_batch (dict): Dictionary of token_name: token_value pairs
        last_run_time (float, optional): Timestamp of last run to get only new data
        
    Returns:
        dict: Summary of processing results
    """
    results = {}
    
    try:
        # Get raw data for all tokens in batch
        raw_data = get_data_batch.remote(token_batch, SOURCE_DB, last_run_time)
        raw_data = ray.get(raw_data)
        
        # Create Redis client for resampled database
        resampled_client = redis.Redis(host='localhost', port=6379, db=RESAMPLED_DB, decode_responses=True)
        # conn = RedisConnection(db=RESAMPLED_DB)
        # resampled_client = conn.get_client()
        
        # Process each token
        for token_name, token_value in token_batch.items():
            try:
                # Get data for this token
                data = raw_data.get(token_name, [])
                
                if not data:
                    results[token_name] = {'status': 'no_data', 'count': 0}
                    continue
                
                # Prepare and resample data
                # resampled_df = prepare_dataframe(data)
                resampled_df = prepare_new_data(data, token_name, token_value, resampled_client)

                
                if resampled_df.empty:
                    results[token_name] = {'status': 'empty_after_resample', 'count': 0}
                    continue
                
                # Store resampled data directly (no additional processing)
                stored_count = store_resampled_data(resampled_df, token_name, token_value, resampled_client)
                
                # Update pivot values for this token
                update_pivot_values(token_name, token_value, resampled_client)

                #FOR DEBUG PURPOSES TO PRINT THE ENTIRE DATAFRAME
                # pivot_data = get_latest_pivot_index(token_name)
                # import pandas as pd
                # pd.set_option('display.max_rows', None)
                # pd.set_option('display.max_columns', None)
                # pd.set_option('display.width', None)
                # pd.set_option('display.max_colwidth', None)
                
                # # Print the complete dataframe
                # print("\nComplete DataFrame:")
                # print(resampled_df)
                
                # # Reset display options to default
                # pd.reset_option('display.max_rows')
                # pd.reset_option('display.max_columns')
                # pd.reset_option('display.width')
                # pd.reset_option('display.max_colwidth')

                # if pivot_data:
                #     print(f"Latest pivot for {token_name}:")
                #     print(f"  Pivot Index: {pivot_data['pivot_index']}")
                #     print(f"  Timestamp: {pivot_data['timestamp']}")
                #     print(f"  Last Updated: {pivot_data['last_updated']}")
                # else:
                #     print(f"No pivot data available for {token_name}")

                # time.sleep(1000)
                
                results[token_name] = {
                    'status': 'success', 
                    'count': stored_count,
                    'raw_count': len(data),
                    'resampled_count': len(resampled_df)
                }
                
                # Add code to print comparison for debugging
                # print(f"\nStored {stored_count} candles for {token_name}")
                
            except Exception as e:
                error_msg = f"Error processing token {token_name}: {str(e)}"
                print(error_msg)
                results[token_name] = {'status': 'error', 'message': error_msg}
        
        # Close the Redis client
        try:
            resampled_client.close()
        except:
            pass
        
        del raw_data            
        return results
    
    except Exception as e:
        error_msg = f"Error in batch processing: {str(e)}"
        print(error_msg)
        return {'batch_error': error_msg}
    finally:
        if resampled_client:
            resampled_client.close()

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

def store_historical_resampled_data(resampled_data, token_name, token_value, resampled_client, lot_size):
    """
    Store or update resampled dataframe in Redis
    
    Args:
        resampled_data (pd.DataFrame): Resampled DataFrame with new/updated intervals
        token_name (str): Symbol/name of the token
        token_value (str): Token value/ID
        resampled_client: Redis client for resampled DB
        
    Returns:
        int: Number of entries stored/updated
    """
    if resampled_data.empty:
        return 0
    
    prefix = RESAMPLED_PREFIX
    stored_count = 0
    
    # Create a pipeline for bulk operations
    pipe = resampled_client.pipeline(transaction=False)
    
    # Prepare index key
    index_key = f"{prefix}:{token_value}:index"
    
    # Process each row of the resampled dataframe
    for idx, row in resampled_data.iterrows():
        # Get timestamp and format it
        timestamp = int(row['timestamp'].timestamp() * 1000)  # Convert to milliseconds
        # print(f"DEBUG: Storing row with timestamp={timestamp}, adjVol={row['adjVol']}")
        
        # Create hash key for this data point
        data_key = f"{prefix}:{token_value}:{timestamp}"
        
        # Check if this interval already exists
        existing_data = resampled_client.exists(data_key)
        
        # Build data dictionary
        data = {
            'symbol': token_name,
            'timestamp': str(timestamp)
        }
        # print(f"DEBUG: Data dictionary adjVol value: {data.get('adjVol', 'NOT_PRESENT')}")
        
        # Add all numeric columns (convert to string)
        for col in ['open', 'high', 'low', 'close', 'adjVol', 'ltp', 'pivot', 'ema10', 'ema20']:
            if col in row and not pd.isna(row[col]):
                data[col] = str(float(row[col]))
                # print(f"DEBUG: Converting {col} from {row[col]} to {data[col]}")
            else:
                # Use default values if missing
                if col == 'ltp' and 'close' in row and not pd.isna(row['close']):
                    data[col] = str(float(row['close']))                
                elif not existing_data:  # Only set defaults for new records
                    data[col] = '0.0'
                if col == 'adjVol' and 'volume' in row and not pd.isna(row['volume']):
                    data[col] = str(float(row['volume']) * lot_size)
        
        # Add pivot_candle_index based on pivot value
        pivot_value = float(data.get('pivot', 0))
        if pivot_value > 0:
            data['pivot_candle_index'] = str(idx)
        else:
            data['pivot_candle_index'] = ''
        
        # Store the data in Redis
        pipe.hset(data_key, mapping=data)
        
        # Add to sorted set index if it's a new record
        if not existing_data:
            pipe.zadd(index_key, {data_key: timestamp})
        
        stored_count += 1
    
    # Execute pipeline
    pipe.execute()
    
    return stored_count

_FIRST_RUN_COMPLETED = False

def store_resampled_data(resampled_data, token_name, token_value, resampled_client):
    """
    Store or update resampled dataframe in Redis
    
    Args:
        resampled_data (pd.DataFrame): Resampled DataFrame with new/updated intervals
        token_name (str): Symbol/name of the token
        token_value (str): Token value/ID
        resampled_client: Redis client for resampled DB
        
    Returns:
        int: Number of entries stored/updated
    """

    if resampled_data.empty:
        return 0
    
    prefix = RESAMPLED_PREFIX
    stored_count = 0
    global _FIRST_RUN_COMPLETED
    
    # Create a pipeline for bulk operations
    pipe = resampled_client.pipeline(transaction=False)
    
    # Prepare index key
    index_key = f"{prefix}:{token_value}:index"
    
    # Process each row of the resampled dataframe
    for idx, row in resampled_data.iterrows():
        # Get timestamp and format it
        timestamp = int(row['timestamp'].timestamp() * 1000)  # Convert to milliseconds
        # print(f"DEBUG: Storing row with timestamp={timestamp}, adjVol={row['adjVol']}")

        candle_time = row['timestamp'].time()
        # if (candle_time.hour == 9 and candle_time.minute == 5) or (candle_time.hour == 9 and candle_time.minute == 10):
        #     continue
        # To skip the time before 9;15
        if not _FIRST_RUN_COMPLETED and candle_time.hour == 9 and candle_time.minute in (5, 10):
            continue
        
        # Create hash key for this data point
        data_key = f"{prefix}:{token_value}:{timestamp}"
        
        # Check if this interval already exists
        existing_data = resampled_client.exists(data_key)
        
        # Build data dictionary
        data = {
            'symbol': token_name,
            'timestamp': str(timestamp)
        }
        # print(f"DEBUG: Data dictionary adjVol value: {data.get('adjVol', 'NOT_PRESENT')}")
        
        # Add all numeric columns (convert to string)
        for col in ['open', 'high', 'low', 'close', 'adjVol', 'ltp', 'pivot', 'ema10', 'ema20']:
            if col in row and not pd.isna(row[col]):
                data[col] = str(float(row[col]))
                # print(f"DEBUG: Converting {col} from {row[col]} to {data[col]}")
            else:
                # Use default values if missing
                if col == 'ltp' and 'close' in row and not pd.isna(row['close']):
                    data[col] = str(float(row['close']))
                elif not existing_data:  # Only set defaults for new records
                    data[col] = '0.0'
        
        # Add pivot_candle_index based on pivot value
        pivot_value = float(data.get('pivot', 0))
        if pivot_value > 0:
            data['pivot_candle_index'] = str(idx)
        else:
            data['pivot_candle_index'] = ''
        
        # Store the data in Redis
        pipe.hset(data_key, mapping=data)
        
        # Add to sorted set index if it's a new record
        if not existing_data:
            pipe.zadd(index_key, {data_key: timestamp})
        
        stored_count += 1
    
    # Mark first run as completed
    _FIRST_RUN_COMPLETED = True

    # Execute pipeline
    pipe.execute()
    
    return stored_count

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

def setup_resampled_db():
    """
    Set up the structure for the resampled Redis database
    """
    try:
        with RedisConnection(db=RESAMPLED_DB) as resampled_client:
        # Create Redis client
        # resampled_client = redis.Redis(host='localhost', port=6379, db=RESAMPLED_DB, decode_responses=True)
        
            # Create info key with metadata
            info_key = f"{RESAMPLED_PREFIX}:info"
            if not resampled_client.exists(info_key):
                resampled_client.hset(info_key, mapping={
                    "description": f"Resampled {RESAMPLE_INTERVAL} market data",
                    "structure": f"{RESAMPLED_PREFIX}:{{token}}:{{timestamp}} -> Hash with OHLCV data",
                    "fields": "symbol, timestamp, open, high, low, close, adjVol, ltp",
                    "pivot_index_key": f"{RESAMPLED_PREFIX}:pivot_index:{{token_name}}",
                    "source_db": SOURCE_PREFIX,
                    "created_at": str(datetime.now())
                })
                logger.info(f"Created resampled DB structure documentation")
            
            # Create pivot index info key
            pivot_info_key = f"{RESAMPLED_PREFIX}:pivot_index:info"
            if not resampled_client.exists(pivot_info_key):
                resampled_client.hset(pivot_info_key, mapping={
                    "description": "Latest pivot candle indices for each token",
                    "structure": f"{RESAMPLED_PREFIX}:pivot_index:{{token_name}} -> Hash with pivot information",
                    "fields": "token_name, pivot_index, timestamp, last_updated",
                    "created_at": str(datetime.now())
                })
                logger.info(f"Created pivot index structure documentation")
            
            resampled_client.close()
            return True
        
    except Exception as e:
        logger.error(f"Error setting up resampled database: {str(e)}")
        return False

def get_next_run_time(interval_minutes=5):
    """
    Calculate the next run time based on 5-minute intervals
    
    Returns:
        tuple: (next_run_datetime, seconds_to_wait)
    """
    now = datetime.now()
    
    # Calculate the next 5-minute mark
    minutes = now.minute
    next_5min = ((minutes // interval_minutes) + 1) * interval_minutes
    
    if next_5min >= 60:  # Handle hour rollover
        next_hour = now.hour + 1
        next_5min = 0
    else:
        next_hour = now.hour
        
    next_run = now.replace(hour=next_hour, minute=next_5min, second=0, microsecond=0)
    seconds_to_wait = (next_run - now).total_seconds()
    
    # Ensure we wait at least 10 seconds
    if seconds_to_wait < 10:
        next_run = next_run + timedelta(minutes=interval_minutes)
        seconds_to_wait = (next_run - now).total_seconds()
        
    return next_run, seconds_to_wait

def is_market_open():
    """Check if the market is currently open"""
    now = datetime.now()

    # Check if it's a weekend
    if now.weekday() >= 5:  # 5 is Saturday, 6 is Sunday
        return False

    # Check market hours (9:00 to 15:30)
    current_time = now.time()
    market_start = datetime.strptime('09:15', '%H:%M').time()
    market_end = datetime.strptime('15:30', '%H:%M').time()

    return market_start <= current_time <= market_end

def start_periodic_summary():
    """Start a background thread that periodically logs summary statistics"""
    def summary_reporter():
        # resampled_client = redis.Redis(host='localhost', port=6379, db=RESAMPLED_DB, decode_responses=True)
        with RedisConnection(db=RESAMPLED_DB) as resampled_client:
            while True:
                try:
                    # Get all keys matching the index pattern
                    index_keys = resampled_client.keys(f"{RESAMPLED_PREFIX}:*:index")
                    total_tokens = len(index_keys)
                    
                    # Sample a few tokens to get record counts
                    record_counts = []
                    for key in index_keys[:5]:  # Check first 5 tokens
                        count = resampled_client.zcard(key)
                        record_counts.append(count)
                    
                    # Calculate average records per token
                    avg_records = sum(record_counts) / len(record_counts) if record_counts else 0
                    
                    logger.info(f"Database summary: {total_tokens} tokens with ~{avg_records:.1f} records per token")
                    
                except Exception as e:
                    logger.error(f"Error in summary reporter: {str(e)}")
                finally:
                    # Sleep for 1 hour before next report
                    time.sleep(3600)
        
    reporter_thread = threading.Thread(target=summary_reporter, daemon=True)
    reporter_thread.start()
    return reporter_thread

import gc
def run_resampler():
    """
    Main function to run the resampling process using Ray for parallel processing
    Only processes new data since the last run
    """
    logger.info("Starting Redis ticker data resampler with Ray - Incremental Mode")
    
    # Initialize Ray if not already initialized
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)
        logger.info(f"Initialized Ray with {ray.cluster_resources()['CPU']} CPUs")
    
    # Set up resampled database structure
    if not setup_resampled_db():
        logger.error("Failed to set up resampled database. Exiting.")
        return
    
    # Load tokens
    tokens = load_tokens()
    # tokens = {"ADANIGREEN25APR900PE": "64625"}
    if not tokens:
        logger.error("No tokens loaded. Exiting.")
        return
    
    now = datetime.now()

    # Skip processing if market is closed
    if not (9 <= now.hour and now.minute <= 10):   
        try:
            with RedisConnection(db=RESAMPLED_DB) as resampled_client:
                # Check if there are any keys matching the resampled data pattern
                index_keys = resampled_client.keys(f"{RESAMPLED_PREFIX}:*:index")
                
                # If no index keys exist, the database is empty
                if len(index_keys) == 0:
                    logger.info("Loading historical data before starting incremental updates")
                    load_historical_data(tokens, days_back=10) 

        except Exception as e:
            logger.error(f"Error: {str(e)}")   
    else:
        logger.info("Market is closed. Bye")      
 
            
    # Initialize last_run_time to current time minus 5 minutes to get recent data on first run
    last_run_time = time.time() - 43200    # 12 hr ago

    print("\n")

    logger.info("Starting Live Data Resampling")
    
    try:
        market_open_time = datetime.strptime('09:15', '%H:%M').time()

        while True:    
            now = datetime.now()   
            current_time = now.time()       
            if current_time >= market_open_time:
                break
            

        while True:                        

            # Skip processing if market is closed
            # if not is_market_open() or not (9 <= now.hour <= 15 and now.minute > 30):
            #     logger.info("Market is closed. Bye")
            #     break

            # Calculate next run time (aligned to 5-minute intervals)
            next_run, wait_seconds = get_next_run_time(5)

            logger.info(f"Next resampling run scheduled at {next_run} ({wait_seconds:.0f} seconds from now)")
            
            # Wait until next run time
            # time.sleep(wait_seconds)
            
            run_start = time.time()
            now = datetime.now()
  
            logger.info(f"\nStarting incremental resampling run at {now}")

            # Process tokens in batches
            token_batches = batch_tokens(tokens, batch_size=50)
            logger.info(f"Processing {len(token_batches)} batches of tokens in parallel - Using data since {datetime.fromtimestamp(last_run_time)}")
            
            # Submit all batch processing tasks to Ray with the last run time
            futures = [process_token_batch.remote(batch, last_run_time) for batch in token_batches]
            
            # Process results as they complete
            total_stored = 0
            completed_batches = 0
            
            # Wait for all futures to complete
            batch_results = []
            while futures:
                done_futures, futures = ray.wait(futures, timeout=5.0)
                
                if done_futures:
                    for future in done_futures:
                        try:
                            result = ray.get(future)
                            batch_results.append(result)
                            
                            # Count stored items
                            batch_stored = sum(r.get('count', 0) for r in result.values() 
                                             if isinstance(r, dict) and 'count' in r)
                            total_stored += batch_stored
                            completed_batches += 1
                            
                            # Log errors
                            errors = [f"{token}: {data['message']}" for token, data in result.items() 
                                    if isinstance(data, dict) and data.get('status') == 'error']
                            if errors:
                                logger.warning(f"Errors in batch {completed_batches}: {', '.join(errors)}")
                                
                        except Exception as e:
                            logger.error(f"Error processing batch result: {str(e)}")
            
            run_duration = time.time() - run_start
            logger.info(f"Completed incremental resampling run in {run_duration:.2f} seconds. Stored/updated {total_stored} records.")
            
            # Update last run time to the start of this run
            last_run_time = run_start
            time.sleep(1000)
            gc.collect()
            
    except KeyboardInterrupt:
        logger.info("Resampler stopped by user")
    except Exception as e:
        logger.error(f"Fatal error in resampler: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        # Clean up resources
        if ray.is_initialized():
            ray.shutdown()
        logger.info("Resampler shutdown complete")

def get_latest_pivot_index(token_name):
    """
    Get the latest pivot index for a specific token
    
    Args:
        token_name (str): The name of the token (e.g., 'KEI25MAR2700PE')
        
    Returns:
        dict: Information about the latest pivot, or None if not found
    """
    with RedisConnection(db=1) as client:
    # client = redis.Redis(host='localhost', port=6379, db=1, decode_responses=True)
    # Get the pivot index record
        pivot_key = f"{RESAMPLED_PREFIX}:pivot_index:{token_name}"
        pivot_data = client.hgetall(pivot_key)
        
        if not pivot_data:
            print(f"No pivot data found for {token_name}")
            return None
        
        # Convert pivot index to int
        if 'pivot_index' in pivot_data:
            pivot_data['pivot_index'] = int(pivot_data['pivot_index'])
    
        return pivot_data

def print_token_data(token_name, token_value):
    """
    Print data for a specific token from both databases
    
    Args:
        token_name (str): Symbol/name of the token
        token_value (str): Token value/ID
    """
    print(f"\n{'='*80}")
    print(f"DATA FOR TOKEN: {token_name}")
    print(f"{'='*80}")
    
    # Connect to both databases
    source_client = redis.Redis(host='localhost', port=6379, db=SOURCE_DB, decode_responses=True)
    resampled_client = redis.Redis(host='localhost', port=6379, db=RESAMPLED_DB, decode_responses=True)
    with RedisConnection(db=SOURCE_DB) as source_client, RedisConnection(db=RESAMPLED_DB) as resampled_client:
    
        # Set pandas display options
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        
        try:
            # Get raw data
            raw_data = []
            source_index_key = f"{SOURCE_PREFIX}:{token_value}:index"
            if source_client.exists(source_index_key):
                raw_keys = source_client.zrange(source_index_key, 0, -1)
                for key in raw_keys:
                    data = source_client.hgetall(key)
                    if data:
                        # Convert numeric fields
                        for field in ['ltp', 'open', 'high', 'low', 'close', 'volume', 'ema10', 'ema20', 'pivot_candle_index', 'pivot']:
                            if field in data:
                                try:
                                    data[field] = float(data[field])
                                except:
                                    pass
                        raw_data.append(data)
            
            # Get resampled data
            resampled_data = []
            resampled_index_key = f"{RESAMPLED_PREFIX}:{token_value}:index"
            if resampled_client.exists(resampled_index_key):
                resampled_keys = resampled_client.zrange(resampled_index_key, 0, -1)
                for key in resampled_keys:
                    data = resampled_client.hgetall(key)
                    if data:
                        # Convert numeric fields
                        for field in ['open', 'high', 'low', 'close', 'adjVol', 'ema10', 'ema20', 'pivot']:
                            if field in data:
                                try:
                                    data[field] = float(data[field])
                                except:
                                    pass
                        resampled_data.append(data)
            
            # Get pivot data
            pivot_key = f"{RESAMPLED_PREFIX}:pivot_index:{token_name}"
            pivot_data = resampled_client.hgetall(pivot_key)
            
            # Print raw data
            if raw_data:
                print(f"\nRAW DATA FROM SOURCE DB ({SOURCE_DB}):")
                raw_df = pd.DataFrame(raw_data)
                # print(raw_df)
            else:
                print(f"\nNo raw data found for {token_name} in SOURCE DB ({SOURCE_DB})")
            
            # Print resampled data
            if resampled_data:
                print(f"\nRESAMPLED DATA FROM RESAMPLED DB ({RESAMPLED_DB}):")
                resampled_df = pd.DataFrame(resampled_data)
                print(resampled_df)
            else:
                print(f"\nNo resampled data found for {token_name} in RESAMPLED DB ({RESAMPLED_DB})")
            
            # Print pivot data
            if pivot_data:
                print(f"\nPIVOT DATA:")
                for key, value in pivot_data.items():
                    print(f"  {key}: {value}")
            else:
                print(f"\nNo pivot data found for {token_name}")
        
        finally:
            # Reset pandas options
            pd.reset_option('display.max_rows')
            pd.reset_option('display.max_columns')
            pd.reset_option('display.width')
            
            # Close Redis connections
            source_client.close()
            resampled_client.close()

#Use this function if you want to clear the database but not delete it
def clear_resampled_database():
    """
    Clear only the resampled data from Redis database (DB index 1).
    This function deliberately does not allow clearing the source database (DB index 0).
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        with RedisConnection(db=RESAMPLED_DB) as client:
        # Only clear the resampled database (DB index 1)
        # client = redis.Redis(host='localhost', port=6379, db=RESAMPLED_DB, decode_responses=True)
        
            # Print what we're about to do as an extra safety check
            logger.info(f"Clearing the resampled database (DB index {RESAMPLED_DB})")
            
            # FLUSHDB command clears the entire current database
            client.flushdb()
            
            logger.info(f"Successfully cleared resampled data (DB index {RESAMPLED_DB})")
            client.close()
            return True
            
    except Exception as e:
        logger.error(f"Error clearing resampled database: {str(e)}")
        return False

last_run_time = None

if __name__ == "__main__":
    # Command line arguments
    import argparse
    global SOURCE_PREFIX # "18mar2025"
    global RESAMPLED_PREFIX # "19mar2025resampled" 

    # Use for clearing the database
    # clear_resampled_database()


    SOURCE_PREFIX = get_date_prefix()
    RESAMPLED_PREFIX = get_date_prefix() + "resampled"

    # tokens = {"ADANIGREEN25APR900PE": "64625"}
    # # tokens = load_tokens() 
    
    # if tokens:
    #     # Print data for the first token
    #     for token_name, token_value in tokens.items():
    #         print_token_data(token_name, token_value)

    # time.sleep(1000)

    parser = argparse.ArgumentParser(description='Redis Ticker Data Resampler with Ray')
    parser.add_argument('--debug', action='store_true', help='Run in debug mode (process single token)')
    args = parser.parse_args()
    
    if args.debug:
        # Initialize Ray
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)
            
        # Process a single token for debugging
        tokens = load_tokens()
        if tokens:
            sample_token = dict([next(iter(tokens.items()))])
            logger.info(f"Debug mode: Processing sample token {list(sample_token.keys())[0]}")
            
            result = ray.get(process_token_batch.remote(sample_token))
            logger.info(f"Debug result: {result}")
            
            ray.shutdown()
        else:
            logger.error("No tokens available for debug run")
    else:
        # Run the main resampler
        run_resampler()
