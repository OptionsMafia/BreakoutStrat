from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import threading
import pyotp, time
import config
from datetime import datetime
import os
import json
import subprocess
import sys
import redis
 
# logging ----------------------------  #
import logging
import sys
td = datetime.today().date()
logging.basicConfig(filename=f"ANGEL_WEBSOCKET_{td}.log", format='%(asctime)s - %(levelname)s - %(message)s')
# Initialize global dictionary
# fno_token = {}

# fno_token = {"BPCL25MAR250CE": "88450", "BPCL25MAR250PE": "88451", "BHARTIARTL25MAR1700CE": "95552", "BHARTIARTL25MAR1580PE": "95545", "CHOLAFIN25MAR1500CE": "99380", "CHOLAFIN25MAR1300PE": "99371", "INDUSINDBK25MAR1100CE": "120365", "INDUSINDBK25MAR1000PE": "120346", "TATASTEEL25MAR150CE": "150508", "TATASTEEL25MAR150PE": "150509", "HAL25MAR4000CE": "111826", "HAL25MAR3300PE": "111788", "ICICIGI25MAR2000CE": "116473", "ICICIGI25MAR1700PE": "116867", "IGL25MAR210CE": "117917", "IGL25MAR190PE": "117908", "LAURUSLABS25MAR600CE": "125933", "LAURUSLABS25MAR530PE": "128030", "HCLTECH25MAR1700CE": "111254", "HCLTECH25MAR1400PE": "112487", "M&MFIN25MAR300CE": "128912", "M&MFIN25MAR250PE": "128903", "LTF25MAR140CE": "127594", "LTF25MAR165PE": "127605", "TATAPOWER25MAR350CE": "150074", "TATAPOWER25MAR400PE": "150085", "MANAPPURAM25MAR200CE": "129226", "MANAPPURAM25MAR195PE": "129225", "KPITTECH25MAR1300CE": "127293", "KPITTECH25MAR1200PE": "125447", "KALYANKJIL25MAR500CE": "126604", "KALYANKJIL25MAR440PE": "124736", "TCS25MAR4000CE": "150646", "TCS25MAR3500PE": "150637", "BHEL25MAR200CE": "86924", "BHEL25MAR170PE": "86919", "ASTRAL25MAR1400CE": "76651", "ASTRAL25MAR1400PE": "76652", "INDIANB25MAR620CE": "119227", "INDIANB25MAR480PE": "119201", "SBICARD25MAR900CE": "148024", "SBICARD25MAR800PE": "148009", "APOLLOHOSP25MAR7000CE": "74143", "APOLLOHOSP25MAR6000PE": "73982", "PNB25MAR100CE": "146600", "PNB25MAR90PE": "146591", "LTTS25MAR5000CE": "128074", "LTTS25MAR4500PE": "128055", "CIPLA25MAR1500CE": "99538", "CIPLA25MAR1240PE": "94179", "GLENMARK25MAR1400CE": "108464", "GLENMARK25MAR1300PE": "108227", "LTIM25MAR5200CE": "127640", "LTIM25MAR4000PE": "127629", "LUPIN25MAR2100CE": "130170", "LUPIN25MAR1900PE": "130161", "HINDUNILVR25MAR2300CE": "115576", "HINDUNILVR25MAR2200PE": "115252", "MPHASIS25MAR2600CE": "132096", "MPHASIS25MAR2300PE": "132091", "TVSMOTOR25MAR2500CE": "155689", "TVSMOTOR25MAR2200PE": "151605", "PIIND25MAR3100CE": "146442", "PIIND25MAR3000PE": "146441", "POLICYBZR25MAR1700CE": "146686", "POLICYBZR25MAR1450PE": "139546", "OIL25MAR450CE": "136351", "OIL25MAR330PE": "136244", "HDFCLIFE25MAR620CE": "113484", "HDFCLIFE25MAR520PE": "113351", "JSWENERGY25MAR500CE": "124004", "JSWENERGY25MAR400PE": "123970", "HFCL25MAR100CE": "114127", "HFCL25MAR75PE": "114118", "RELIANCE25MAR1300CE": "147718", "RELIANCE25MAR1200PE": "147709", "GODREJCP25MAR1100CE": "109497", "GODREJCP25MAR1000PE": "109455", "BOSCHLTD25MAR30000CE": "88154", "BOSCHLTD25MAR30000PE": "88155", "ITC25MAR400CE": "122610", "ITC25MAR400PE": "122611", "ADANIENSOL25MAR700CE": "85009", "ADANIENSOL25MAR660PE": "85007", "BAJAJFINSV25MAR1940CE": "92501", "BAJAJFINSV25MAR1800PE": "80490"}
fno_token = {}

def run_tokengen():
    """
    Run the tokengen.py script to generate the token file with a simple
    progress indicator. Returns True if successful, False otherwise.
    """
    try:
        print("Running tokengen.py to generate fresh tokens...")
        # Get the path to the tokengen.py script (assuming it's in the same directory)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        tokengen_path = os.path.join(script_dir, "tokengen.py")

        # Check if tokengen.py exists
        if not os.path.exists(tokengen_path):
            print(f"Error: tokengen.py not found at {tokengen_path}")
            return False

        # Create a simple progress indicator
        print("Processing tokens ", end="")

        # Start a thread to show an animated indicator while tokengen runs
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
        indicator_thread.daemon = True  # Thread will exit when main program exits
        indicator_thread.start()

        # Run the tokengen.py script as a subprocess
        process = subprocess.run([sys.executable, tokengen_path],
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              text=True,
                              check=False)

        # Stop the indicator thread
        stop_indicator = True
        indicator_thread.join(1.0)  # Wait for thread to finish, timeout after 1 second

        # Clear the progress indicator line
        print("\r" + " " * 50 + "\r", end="")

        # Check if the process was successful
        if process.returncode == 0:
            print("Successfully generated tokens with tokengen.py")
            return True
        else:
            print(f"Error running tokengen.py: {process.stderr}")
            return False

    except Exception as e:
        print(f"Error executing tokengen.py: {str(e)}")
        return False

def load_tokens():
    """
    Load token dictionary from the most recent token file in the tokens directory.
    Returns the token dictionary or empty dict if no file is found.
    """
    try:
        # Get current date to find the token file
        current_date = datetime.now()
        # Format date as "mar022025" (lowercase month, no leading zero for day)
        month_abbr = current_date.strftime("%b").lower()
        day = str(current_date.day)  # No leading zero
        year = current_date.strftime("%Y")
        date_str = f"{month_abbr}{day}{year}"

        # Set the full path for the token file
        tokens_dir = os.path.join(os.getcwd(), "tokens")
        filename = os.path.join(tokens_dir, f"{date_str}token.txt")

        print(f"Looking for token file: {filename}")

        # If the exact date file doesn't exist, try to find the most recent one
        if not os.path.exists(filename):
            print(f"Token file for today ({date_str}) not found, looking for the most recent file...")
            if os.path.exists(tokens_dir):
                files = [f for f in os.listdir(tokens_dir) if f.endswith('token.txt')]
                if files:
                    # Sort files by modification time (most recent first)
                    files.sort(key=lambda x: os.path.getmtime(os.path.join(tokens_dir, x)), reverse=True)
                    filename = os.path.join(tokens_dir, files[0])
                    print(f"Using the most recent token file: {files[0]}")
                else:
                    print("No token files found in the tokens directory.")
                    return {}
            else:
                print(f"Tokens directory not found. Run tokengen.py first.")
                return {}

        # Read token dictionary from the file
        with open(filename, 'r') as file:
            token_dict = json.load(file)
            print(f"Successfully loaded {len(token_dict)} tokens from {filename}")
            return token_dict

    except Exception as e:
        print(f"Error loading token dictionary: {str(e)}")
        return {}

 # fno_token = {}

token_content = list(fno_token.values())[:100]
mcx_token = ["438577","438893"]
logger=logging.getLogger()
logger.setLevel(logging.INFO)

stdout_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(stdout_handler)

def login():
    obj=SmartConnect(api_key=config.API_KEY)
    data = obj.generateSession(config.USERNAME,config.PIN,pyotp.TOTP(config.TOKEN).now())
    #print(data)
    AUTH_TOKEN = data['data']['jwtToken']
    refreshToken= data['data']['refreshToken']
    FEED_TOKEN=obj.getfeedToken()
    res = obj.getProfile(refreshToken)
    logger.info(f'{res["data"]["products"]}')

    sws = SmartWebSocketV2(AUTH_TOKEN, config.API_KEY, config.USERNAME, FEED_TOKEN ,max_retry_attempt=5)
    return obj, sws


# Initialize Redis client
redis_client = redis.Redis(
    host='localhost',  # replace with your Redis server host
    port=6379,         # replace with your Redis server port
    db=0,              # database number
    decode_responses=True  # automatically decode responses to Python strings
)

# Store the table/prefix name in a global variable
REDIS_PREFIX = None

def generate_db_name():
    """
    Generate a database name based on the current date in format: 3mar2025
    Returns the database name as a string.
    """
    from datetime import datetime

    current_date = datetime.now()

    # Format the date components
    day = str(current_date.day)  # Day without leading zero
    month = current_date.strftime("%b").lower()  # Month abbreviated and lowercase
    year = current_date.strftime("%Y")  # 4-digit year

    # Combine to create the database name
    db_name = f"{day}{month}{year}"

    return db_name


def initialize_redis():
    """Initialize Redis connection and create metadata for the data structure"""
    try:
        # Test connection
        if redis_client.ping():
            print("Connected to Redis server. Server info:", redis_client.info()['redis_version'])

            # Generate prefix in format 3mar2025
            prefix = generate_db_name()

            # Create metadata entry with table schema info
            info_key = f"{prefix}:info"
            redis_client.hset(info_key, mapping={
                "description": f"Market data storage for {prefix}",
                "structure": f"{prefix}:{{token}}:{{timestamp}} -> Hash with OHLCV data",
                "fields": "token,timestamp,last_traded_price,open_price,high_price,low_price,close_price,volume",
                "created_at": datetime.now().isoformat()
            })

            # Store the prefix in a global variable to use in store_data_in_redis
            global REDIS_PREFIX
            REDIS_PREFIX = prefix

            print(f"Redis initialized with prefix: {REDIS_PREFIX}")
            return True
        else:
            print("Failed to connect to Redis server")
            return False
    except Exception as e:
        print(f"Error initializing Redis: {e}")
        return False

def store_data_in_redis(data):
    """Store market data in Redis"""
    try:
        # Create a unique key for this data point
        timestamp_ms = int(datetime.now().timestamp() * 1000)  # Millisecond precision
        key = f"{REDIS_PREFIX}:{data['token']}:{timestamp_ms}"

        # Store data as a hash
        redis_client.hset(key, mapping={
            "token": data['token'],
            "timestamp": timestamp_ms,
            "ltp": data['last_traded_price'],
            "open": data['open_price_of_the_day'],
            "high": data['high_price_of_the_day'],
            "low": data['low_price_of_the_day'],
            "close": data['closed_price'],
            "volume": data['volume_trade_for_the_day']
        })

        # Set expiry for the data (optional, adjust based on your retention needs)
        # For example, keep data for 7 days
        redis_client.expire(key, 60 * 60 * 24 * 7)

        # Also maintain a sorted set for easier time-based queries
        sorted_key = f"{REDIS_PREFIX}:{data['token']}:index"
        redis_client.zadd(sorted_key, {key: timestamp_ms})

        # Also set expiry for the sorted set
        redis_client.expire(sorted_key, 60 * 60 * 24 * 7)

    except Exception as e:
        print(f"Error storing data in Redis: {e}")
        # Continue without failing, maybe log to a temporary file
        with open('failed_data.log', 'a') as f:
            f.write(f"{datetime.now()}: {str(data)}\n")

def process_and_store_data(data):
    try:
        adjusted_data = {
            'token': str(data['token']),
            'last_traded_price': float(data['last_traded_price']) / 100,
            'open_price_of_the_day': float(data.get('last_traded_price', 0)) / 100,
            'high_price_of_the_day': float(data.get('last_traded_price', 0)) / 100,
            'low_price_of_the_day': float(data.get('last_traded_price', 0)) / 100,
            'closed_price': float(data.get('last_traded_price', 0)) / 100,
            'volume_trade_for_the_day': float(data.get('volume_trade_for_the_day', 0))
        }

        store_data_in_redis(adjusted_data)

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        minute = datetime.now().strftime("%H:%M")

        output = f"[{current_time}] {adjusted_data['token']} - minute: {minute} | "
        output += f"open: {adjusted_data['open_price_of_the_day']:.1f} | "
        output += f"close: {adjusted_data['last_traded_price']:.1f} | "
        output += f"high: {adjusted_data['high_price_of_the_day']:.1f} | "
        output += f"low: {adjusted_data['low_price_of_the_day']:.1f} | "
        output += f"volume: {adjusted_data['volume_trade_for_the_day']:.3f}"

        print(output, flush=True)
    except Exception as e:
        print(f"Error in process_and_store_data: {e}")
        print(f"Error occurred with data: {data}")
#------- Websocket code -------#


def on_data(wsapp, msg):
    try:
        print("Ticks: {}".format(msg))
        process_and_store_data(msg)
        #config.LIVE_FEED_JSON[msg['token']] = {'token' :msg['token'] , 'ltp':msg['last_traded_price']/100 , 'exchange_timestamp':  datetime.fromtimestamp(msg['exchange_timestamp']/1000,config.TZ_INFO)}
        #print(config.LIVE_FEED_JSON ,"\n \n ")
        #if datetime.now().second < 1:
            #logger.info(f"{config.LIVE_FEED_JSON}")

    except Exception as e:
        print(e)

def on_error(wsapp, error):
    logger.error(f"---------Connection Error {error}-----------")

def on_close(wsapp):
    logger.info("---------Connection Close-----------")

def close_connection(sws):
    sws.MAX_RETRY_ATTEMPT = 0
    sws.close_connection()

def subscribeSymbol(token_list,sws):
    logger.info(f'Subscribe -------  {token_list}')
    sws.subscribe(config.CORRELATION_ID, config.FEED_MODE, token_list)

def connectFeed(sws,tokeList =None):
    def on_open(wsapp):
        logger.info("on open")
        token_list = [
            {
                "exchangeType": 1,
                "tokens": ["26009"]
            }
        ]
        if tokeList: token_list.append(tokeList)
        sws.subscribe(config.CORRELATION_ID, config.FEED_MODE, token_list)

    sws.on_open = on_open
    sws.on_data = on_data
    sws.on_error = on_error
    sws.on_close = on_close
    threading.Thread(target=sws.connect, daemon=True).start()

def check_token_file_exists():
    """
    Check if the token file for today already exists.
    Returns True if it exists, False otherwise.
    """
    try:
        # Get current date to find the token file
        current_date = datetime.now()
        # Format date as "mar022025" (lowercase month, no leading zero for day)
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
    # Try to run tokengen and load tokens
    if run_tokengen():
        fno_token = load_tokens()

    # If token dictionary is empty, use a small default dictionary for testing
    if not fno_token:
        print("ERROR: Failed to generate or load tokens. Cannot proceed without valid token data.")
        print("Please ensure tokengen.py runs successfully and generates token files.")
        sys.exit(1)  # Exit with error code 1

    print(f"Loaded {len(fno_token)} tokens")

if __name__ == "__main__":
    try:
        # #Check if token file already exists
        if not check_token_file_exists():
            print("Token file for today doesn't exist. Generating new tokens...")
            initialize_tokens()
        else:
            print("Token file for today already exists. Loading tokens...")
            # global fno_token
            fno_token = load_tokens()

            # Exit if no tokens were loaded
            if not fno_token:
                print("ERROR: No tokens available. Cannot proceed without token data.")
                print("Please ensure tokengen.py runs successfully and generates token files.")
                sys.exit(1)  # Exit with error code 1

        # Initialize Redis instead of ClickHouse
        if not initialize_redis():
            print("ERROR: Failed to initialize Redis. Cannot proceed.")
            sys.exit(1)

        config.SMART_API_OBJ, config.SMART_WEB = login()

        connectFeed(config.SMART_WEB)
        time.sleep(5)
        logger.info("-----------Subscribe--------")
        # subscribeList  = [{"exchangeType": 2, "tokens":list(fno_token.values())[:100]}]
        subscribeList = [{"exchangeType": 2, "tokens": list(fno_token.values())}]
        # subscribeList  = [{"exchangeType": 5, "tokens":mcx_token}]
        subscribeSymbol(subscribeList, config.SMART_WEB)
        i = 1
        while True:
            time.sleep(0.1)
            if i == 10000:
                break

        logger.info("-----------Exit--------")

    except KeyboardInterrupt:
        print("\nScript terminated by user")

    except Exception as e:
        print(f"Fatal error: {str(e)}")
