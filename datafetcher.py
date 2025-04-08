''' fetches historical data '''
from SmartApi import SmartConnect
import pandas as pd
from datetime import datetime, timedelta
import time
import logging
import config
import pyotp

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

# Initialize SmartAPI
api_key = config.API_KEY
client_id = config.USERNAME
password = config.PIN
totp = pyotp.TOTP(config.TOKEN).now()

# Create a SmartConnect object
smart_api = SmartConnect(api_key)

# Login
data = smart_api.generateSession(client_id, password, totp)
refreshToken = data['data']['refreshToken']

def get_historical_data_with_rate_limit(option_tokens, from_date, to_date, interval):
    all_data = {}
    
    # Define rate limit parameters
    batch_size = 5  # Process tokens in batches of 5
    batch_delay = 2  # 1 second delay between tokens in a batch
    batch_cooldown = 5  # 60 seconds cooldown between batches
    
    # Process tokens in batches
    for i in range(0, len(option_tokens), batch_size):
        batch = option_tokens[i:i+batch_size]
        logger.info(f"Processing batch {i//batch_size + 1}/{(len(option_tokens)-1)//batch_size + 1}")
        
        # Process each token in the current batch
        for token in batch:
            try:
                logger.info(f"Fetching data for token: {token}")
                
                historical_data = smart_api.getCandleData({
                    "exchange": "NFO",
                    "symboltoken": token,
                    "interval": interval,
                    "fromdate": from_date,
                    "todate": to_date
                })
                
                if historical_data['status']:
                    df = pd.DataFrame(historical_data['data'], 
                                     columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    # Filter for only the most recent trading day in the data
                    if not df.empty:
                        # Sort by timestamp to ensure proper ordering
                        df = df.sort_values('timestamp')
                        
                        # Get the most recent date in the data
                        latest_date = df['timestamp'].dt.date.max()
                        
                        # Filter to keep only the most recent trading day
                        df = df[df['timestamp'].dt.date == latest_date]
                        
                        # Set timestamp as index
                        df.set_index('timestamp', inplace=True)
                        
                        all_data[token] = df
                        logger.info(f"Successfully fetched most recent day's data for token: {token}")
                    else:
                        logger.warning(f"No data found for token: {token}")

                    # df.set_index('timestamp', inplace=True)
                    
                    # all_data[token] = df
                    # logger.info(f"Successfully fetched data for token: {token}")

                else:
                    logger.warning(f"Failed to fetch data for token: {token}")
                    
            except Exception as e:
                logger.error(f"Error fetching data for token {token}: {str(e)}")
                
                # If we get a rate limit error, take a longer break
                if "rate" in str(e).lower():
                    logger.warning("Rate limit hit. Taking a longer break...")
                    time.sleep(120)  # 2 minute break if rate limited
            
            # Delay between tokens in a batch
            time.sleep(batch_delay)
        
        # After processing a batch, take a cooldown period if not the last batch
        if i + batch_size < len(option_tokens):
            logger.info(f"Batch complete. Cooling down for {batch_cooldown} seconds...")
            time.sleep(batch_cooldown)
    
    return all_data


def save_data_incrementally(token, data, folder="data_today"):
    import os
    # import json
    
    # Create tokens directory if it doesn't exist
    tokens_dir = os.path.join(os.getcwd(), folder)
    if not os.path.exists(tokens_dir):
        os.makedirs(tokens_dir)
        print(f"Created directory: {tokens_dir}")
    
    # Save data as CSV file
    filename = f"{tokens_dir}/{token}.csv"
    data.to_csv(filename)
    logger.info(f"Saved data for token {token} to {filename}")
    
    # Optional: Also save metadata about the data
    # metadata = {
    #     "token": token,
    #     "records": len(data),
    #     "start_date": str(data.index.min()) if not data.empty else None,
    #     "end_date": str(data.index.max()) if not data.empty else None,
    #     "columns": list(data.columns),
    #     "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # }
    
    # meta_filename = f"{tokens_dir}/{token}_meta.json"
    # with open(meta_filename, 'w') as f:
    #     json.dump(metadata, f, indent=2)
    
    return filename


def process_data_with_indicators(token, data, symbol_name):
    # Convert DataFrame index to milliseconds timestamp
    data = data.reset_index()
    data['timestamp'] = data['timestamp'].astype(int) // 10**6  # Convert to milliseconds

    # Add symbol column
    data['symbol'] = symbol_name
    
    # Calculate EMA 10 and EMA 20
    data['ema10'] = data['close'].ewm(span=10, adjust=False).mean()
    data['ema20'] = data['close'].ewm(span=20, adjust=False).mean()
    
    # Add empty pivot columns
    data['ltp'] = data['close']  # Last traded price = close
    data['pivot'] = 0.0
    data['pivot_candle_index'] = None
    
    # Calculate volume change (adjVol) - assuming you want volume differences
    data['adjVol'] = data['volume'].diff().fillna(0)
    
    # Reorder columns to match your desired format
    data = data[['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'adjVol', 'ltp', 'pivot', 'ema10', 'ema20', 'pivot_candle_index']]
    
    return data

def main():
    asd = {"CHAMBLFERT25APR700CE": "43301"}
    # Your list of option tokens
    option_tokens = list(asd.values())
    symbol_names = list(asd.keys())
    
    # Create a dictionary to map token to symbol name
    token_to_symbol = dict(zip(option_tokens, symbol_names))
    
    # Define date range
    from_date = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d %H:%M")
    to_date = datetime.now().strftime("%Y-%m-%d %H:%M")
    interval = "FIVE_MINUTE"
    
    # Create an empty DataFrame to store all results
    all_results_df = pd.DataFrame()
    
    # Process in smaller chunks to manage memory
    chunk_size = 50  # Process 50 tokens at a time to avoid memory issues
    
    for i in range(0, len(option_tokens), chunk_size):
        chunk = option_tokens[i:i+chunk_size]
        logger.info(f"Processing chunk {i//chunk_size + 1}/{(len(option_tokens)-1)//chunk_size + 1}")
        
        # Get data for this chunk
        chunk_data = get_historical_data_with_rate_limit(chunk, from_date, to_date, interval)
        
        # Process each result and add to the main DataFrame
        for token, data in chunk_data.items():
            symbol_name = token_to_symbol.get(token, token)  # Use token as fallback if symbol name not found
            processed_df = process_data_with_indicators(token, data, symbol_name)
            all_results_df = pd.concat([all_results_df, processed_df], ignore_index=True)
        
        # Clear memory
        del chunk_data
    
    # Print the results
    pd.set_option('display.max_rows', 20)  # Show more rows
    print(all_results_df)
    
    logger.info("All data processing complete!")
    
    return all_results_df  # Return the DataFrame for further use if needed


if __name__ == "__main__":
    main()
    # Logout when done
    smart_api.terminateSession(client_id)
