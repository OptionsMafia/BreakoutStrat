'''
redis_resampler.py - Resamples ticker data every 5 minutes using Ray for parallel processing
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

#################pivot is not being calculated correctly******************


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

class RedisConnectionManager:
    def __init__(self, db=0, pool_size=10):
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
        prefix = SOURCE_PREFIX
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
        
        # For each token, either use index or scan
        for i, (token_name, token_value) in enumerate(tokens.items()):
            sorted_key = f"{prefix}:{token_value}:index"
            
            if index_exists_results[i]:
                if since_timestamp is None:
                    # Get all keys from the index
                    pipe.zrange(sorted_key, 0, -1)
                else:
                    # Get only keys with timestamps greater than since_timestamp
                    # Converting timestamp to Redis score format if needed
                    pipe.zrangebyscore(sorted_key, since_timestamp, "+inf")
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

def update_pivot_values(token_name, token_value, resampled_client):
    """
    Update pivot values for previously stored candles once we have enough data
    to determine pivots correctly
    """
    prefix = RESAMPLED_PREFIX
    
    # Get all candles from Redis for this token
    index_key = f"{prefix}:{token_value}:index"
    candle_keys = resampled_client.zrange(index_key, 0, -1)
    
    if len(candle_keys) < 7:  # need at least 7 candles (3 before + current + 3 after)
        return  # Not enough candles to perform update
    
    # Get data for these candles
    candle_data = []
    timestamp_to_key = {}
    
    for key in candle_keys:
        data = resampled_client.hgetall(key)
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
                logger.error(f"Error converting data for key {key}: {e}")
                continue
    
    # Sort by timestamp
    df = pd.DataFrame(candle_data).sort_values('timestamp')
    df = df.reset_index(drop=True)  # Reset index for proper pivotid calculation
    
    # Track the latest pivot high
    latest_pivot_index = None
    latest_pivot_timestamp = None
    
    # Recalculate pivot values for all applicable candles
    for i in range(len(df)):
        if i < 3 or i >= len(df) - 3:
            continue  # Skip candles at edges that don't have enough neighbors
            
        pivot_value = pivotid(i, df, 3, 3)
        row = df.iloc[i]
        ts_ms = row['timestamp_ms']
        redis_key = timestamp_to_key.get(ts_ms)
        
        if not redis_key:
            continue
            
        # Always update the pivot value
        resampled_client.hset(redis_key, 'pivot', str(int(pivot_value)))
        
        # Update pivot_candle_index
        if pivot_value > 0:
            resampled_client.hset(redis_key, 'pivot_candle_index', str(i))
            
            # Track latest pivot high
            if pivot_value == 2 and (latest_pivot_timestamp is None or ts_ms > latest_pivot_timestamp):
                latest_pivot_index = i
                latest_pivot_timestamp = ts_ms
        else:
            resampled_client.hset(redis_key, 'pivot_candle_index', '')
    
    # Update pivot tracking for the token
    if latest_pivot_index is not None:
        pivot_key = f"{prefix}:pivot_index:{token_name}"
        updated_data = {
            'token_name': token_name,
            'pivot_index': str(latest_pivot_index),
            'timestamp': str(latest_pivot_timestamp),
            'last_updated': str(int(time.time() * 1000))
        }
        resampled_client.hset(pivot_key, mapping=updated_data)
        
        # Log for debugging
        logger.info(f"Updated pivot tracking for {token_name}: index={latest_pivot_index}, timestamp={latest_pivot_timestamp}")


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
        
        # Process each token
        for token_name, token_value in token_batch.items():
            try:
                # Get data for this token
                data = raw_data.get(token_name, [])
                
                if not data:
                    results[token_name] = {'status': 'no_data', 'count': 0}
                    continue
                
                # Prepare and resample data
                resampled_df = prepare_dataframe(data)

                
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
            
        return results
    
    except Exception as e:
        error_msg = f"Error in batch processing: {str(e)}"
        print(error_msg)
        return {'batch_error': error_msg}


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

def store_resampled_data(resampled_data, token_name, token_value, resampled_client):
    """
    Store resampled dataframe in Redis directly without additional processing
    
    Args:
        resampled_data (pd.DataFrame): Resampled DataFrame
        token_name (str): Symbol/name of the token
        token_value (str): Token value/ID
        resampled_client: Redis client for resampled DB
        
    Returns:
        int: Number of entries stored
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
        
        # Create hash key for this data point
        data_key = f"{prefix}:{token_value}:{timestamp}"
        
        # Build data dictionary directly from the DataFrame row
        data = {
            'symbol': token_name,
            'timestamp': str(timestamp)
        }
        
        # Add all numeric columns (convert to string)
        for col in ['open', 'high', 'low', 'close', 'adjVol', 'ltp', 'pivot', 'ema10', 'ema20']:
            if col in row and not pd.isna(row[col]):
                data[col] = str(float(row[col]))
            else:
                # Use default values if missing
                if col == 'ltp' and 'close' in row and not pd.isna(row['close']):
                    data[col] = str(float(row['close']))
                else:
                    data[col] = '0.0'
        
        # Add pivot_candle_index based on pivot value
        pivot_value = float(data['pivot'])
        if pivot_value > 0:
            data['pivot_candle_index'] = str(idx)
        else:
            data['pivot_candle_index'] = ''
        
        # Store the data in Redis
        pipe.hset(data_key, mapping=data)
        
        # Add to sorted set index
        pipe.zadd(index_key, {data_key: timestamp})
        stored_count += 1
    
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
        # Create Redis client
        resampled_client = redis.Redis(host='localhost', port=6379, db=RESAMPLED_DB, decode_responses=True)
        
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
    market_start = datetime.strptime('09:00', '%H:%M').time()
    market_end = datetime.strptime('15:30', '%H:%M').time()

    return market_start <= current_time <= market_end

def start_periodic_summary():
    """Start a background thread that periodically logs summary statistics"""
    def summary_reporter():
        resampled_client = redis.Redis(host='localhost', port=6379, db=RESAMPLED_DB, decode_responses=True)
        
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

def run_resampler():
    """
    Main function to run the resampling process using Ray for parallel processing
    """
    logger.info("Starting Redis ticker data resampler with Ray")
    
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
    # tokens = {"BAJAJ-AUTO25MAR8000CE": "80094"}
    if not tokens:
        logger.error("No tokens loaded. Exiting.")
        return
    
    logger.info(f"Loaded {len(tokens)} tokens")
    
    # Track the last run time
    last_run_time = None
    
    try:
        while True:
            # Calculate next run time (aligned to 5-minute intervals)
            next_run, wait_seconds = get_next_run_time(5)
            
            logger.info(f"Next resampling run scheduled at {next_run} ({wait_seconds:.0f} seconds from now)")
            
            # Wait until next run time
            time.sleep(wait_seconds)
            
            run_start = time.time()
            now = datetime.now()
            
            logger.info(f"\nStarting resampling run at {now}")
            
            # Skip processing if market is closed
            if not is_market_open() and not (9 <= now.hour <= 16):  # Allow some buffer after market close
                logger.info("Market is closed. Skipping this run.")
                last_run_time = run_start
                break
            
            # Process tokens in batches
            token_batches = batch_tokens(tokens, batch_size=20)  # Slightly smaller batches for Ray
            logger.info(f"Processing {len(token_batches)} batches of tokens in parallel")
            
            # Submit all batch processing tasks to Ray
            futures = [process_token_batch.remote(batch, last_run_time) for batch in token_batches]
            
            # Process results as they complete
            total_stored = 0
            completed_batches = 0
            
            # Wait for all futures to complete with progress updates
            batch_results = []
            while futures:
                # Use ray.wait to get completed tasks
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
                            
                            # Log progress
                            # logger.info(f"Completed batch {completed_batches}/{len(token_batches)} - Stored {batch_stored} records")
                            
                            # Log any errors
                            errors = [f"{token}: {data['message']}" for token, data in result.items() 
                                    if isinstance(data, dict) and data.get('status') == 'error']
                            if errors:
                                logger.warning(f"Errors in batch {completed_batches}: {', '.join(errors)}")
                                
                        except Exception as e:
                            logger.error(f"Error processing batch result: {str(e)}")
                
                # If there are still futures, show progress
                if futures:
                    logger.info(f"Waiting for {len(futures)} remaining batches to complete...")
            
            run_duration = time.time() - run_start
            logger.info(f"Completed resampling run in {run_duration:.2f} seconds. Stored {total_stored} new records.")
            
            # Update last run time
            last_run_time = run_start
            
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
    client = redis.Redis(host='localhost', port=6379, db=1, decode_responses=True)
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
        # Only clear the resampled database (DB index 1)
        client = redis.Redis(host='localhost', port=6379, db=RESAMPLED_DB, decode_responses=True)
        
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

if __name__ == "__main__":
    # Command line arguments
    import argparse
    global SOURCE_PREFIX # "18mar2025"
    global RESAMPLED_PREFIX # "19mar2025resampled" 

    # Use for clearing the database
    # clear_resampled_database()

    SOURCE_PREFIX =  get_date_prefix()
    RESAMPLED_PREFIX = get_date_prefix() + "resampled"

    # tokens = {"CIPLA25MAR1240PE": "94179"}
    # # # # tokens = load_tokens()
    
    # if tokens:
    #     # Print data for the first token
    #     for token_name, token_value in tokens.items():
    #         print_token_data(token_name, token_value)

    # time.sleep(1000)

    # tokens = load_tokens()
    # for token_name in tokens.keys():
    #     pivot_data = get_latest_pivot_index(token_name)
    #     if pivot_data:
    #         print(f"Latest pivot for {token_name}:")
    #         print(f"  Pivot Index: {pivot_data['pivot_index']}")
    #         print(f"  Timestamp: {pivot_data['timestamp']}")
    #         print(f"  Last Updated: {pivot_data['last_updated']}")
    #     else:
    #         print(f"No pivot data available for {token_name}")
    
    parser = argparse.ArgumentParser(description='Redis Ticker Data Resampler with Ray')
    parser.add_argument('--debug', action='store_true', help='Run in debug mode (process single token)')
    args = parser.parse_args()
    
    # Start summary reporter in background
    summary_thread = start_periodic_summary()
    
    if args.debug:
        # Initialize Ray
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)
            
        # Process a single token for debugging
        tokens = load_tokens()
        # tokens = {"BAJAJ-AUTO25MAR8000CE": "80094"}
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
