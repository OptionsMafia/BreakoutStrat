import requests
import zipfile
import io
import pandas as pd
import os
import json
import urllib
from datetime import datetime as dt
from datetime import timedelta as td
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry # type: ignore
inst_url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
resp = urllib.request.urlopen(inst_url)
inst_list = json.loads(resp.read())

# Fetches Token For Equity,Fno
def fetch_token(query, exchange):
    for items in inst_list:
        temp_exchange = items['exch_seg']
        temp_symbol = items['symbol']
        # if temp_symbol == query:
        #     print("hi")
        
        if query in temp_symbol and temp_exchange == exchange:
            token = items['token']
            return token

def download_and_extract_nse_data(url, output_dir='.', max_retries=5):
    """
    Download NSE F&O Bhav Copy zip file, extract it, and return the DataFrame.

    Args:
        url (str): URL of the zip file to download
        output_dir (str): Directory to save extracted files
        max_retries (int): Maximum number of retry attempts

    Returns:
        pandas.DataFrame: DataFrame containing the CSV data
    """
    try:
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"Created directory: {output_dir}")

        # Set up a session with retry mechanism
        session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Add headers to mimic a browser
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://www.nseindia.com/',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0'
        }

        # Download the zip file
        print(f"Downloading from: {url}")
        print("Attempting download with browser-like headers...")

        response = session.get(url, headers=headers, timeout=30)
        print(response)
        response.raise_for_status()

        # Extract the zip file
        print("Extracting zip file...")
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
            # Print the list of files in the zip
            file_list = zip_ref.namelist()
            print(f"Files in zip: {file_list}")

            # Extract all files
            zip_ref.extractall(output_dir)

        # Read the first CSV file (assuming there's only one or we want the first)
        csv_file = os.path.join(output_dir, file_list[0])
        print(f"Reading CSV file: {csv_file}")

        # Read the CSV into a pandas DataFrame
        df = pd.read_csv(csv_file)
        # Return both the dataframe and the path to the extracted file
        return df, csv_file

    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")
        return None, None
    except zipfile.BadZipFile:
        print("Error: The downloaded file is not a valid zip file")
        return None, None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None, None

def try_alternative_download(url, output_dir='.'):
    """
    Try an alternative method to download NSE data when direct download fails
    """
    print("\nAttempting alternative download method...")

    try:
        # Create a filename from the URL
        filename = url.split('/')[-1]
        output_path = os.path.join(output_dir, filename)

        # Use curl command through os.system
        import subprocess
        print(f"Executing curl command to download {url}")

        # Build the curl command with appropriate headers
        curl_cmd = [
            'curl', '-L', '-o', output_path,
            '-H', 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            '-H', 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            '-H', 'Referer: https://www.nseindia.com/',
            url
        ]

        # Execute the command
        result = subprocess.run(curl_cmd, capture_output=True, text=True)

        if result.returncode != 0:
            print(f"Curl command failed: {result.stderr}")
            return None, None

        print(f"File downloaded to {output_path}")

        # Extract the zip file
        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            with zipfile.ZipFile(output_path) as zip_ref:
                file_list = zip_ref.namelist()
                print(f"Files in zip: {file_list}")
                zip_ref.extractall(output_dir)

            # Read the CSV file
            csv_file = os.path.join(output_dir, file_list[0])
            df = pd.read_csv(csv_file)
            return df, csv_file
        else:
            print(f"Downloaded file is empty or doesn't exist")
            return None, None

    except Exception as e:
        print(f"Alternative download method failed: {e}")
        return None, None

from datetime import datetime

def get_fourth_week_thursday(b_df):

    # Get current date
    today = datetime.now()
    t_df = b_df[b_df["TckrSymb"].str.startswith("RELIANCE")]
    t_df = t_df["XpryDt"]
    expiry = sorted(set(t_df.tolist()))[0]
    # Generate the different date formats
    month_names = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
                   "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
   

    expiry = "2025-05-29"  # YYYY-MM-DD
    t_exp = expiry.split("-")
    month_name = month_names[int(t_exp[1]) - 1]
    format2 = f"{str(t_exp[0])[2:4]}{month_name}"  # YYMM (25MAR)
    format3 = f"{t_exp[2]}{month_name}{str(str(t_exp[0]))[2:4]}"  # DDMMYY (27MAR25)
    return expiry, format2, format3


def generate_token_filename():
    """
    Generate filename in the format 'mar022025token.txt'
    """
    today = dt.today()
    month_abbr = today.strftime('%b').lower()  # Get lowercase month abbreviation (mar, apr, etc.)
    day = today.strftime('%d')  # Get day with leading zero
    year = today.strftime('%Y')  # Get full year

    # Remove leading zero from day if present
    if day.startswith('0'):
        day = day[1:]

    filename = f"{month_abbr}{day}{year}token.txt"
    return filename

token_dict = {}
def main():
    i = 0
    while i < 10:
        # NSE F&O Bhav Copy URL
        today = (dt.today())
        st_date = (today-td(i))
        st_date_temp = dt(st_date.year, st_date.month, st_date.day, 9, 15)
        st_date = st_date_temp.strftime("%Y-%m-%d")
        temp_date = st_date.split("-")
        url = "https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_"+temp_date[0]+temp_date[1]+temp_date[2]+"_F_0000.csv.zip"
        output_dir = "nse_data"

        # Try the primary download method first
        df, csv_path = download_and_extract_nse_data(url, output_dir)
        i = i + 1
        if df is not None and len(df) > 10:
            break

    expiry, yearmonth, datemonthyear = get_fourth_week_thursday(df)
    temp_df  = df[df["XpryDt"].str.startswith(expiry)]
    temp_df = temp_df[temp_df["FinInstrmTp"].str.startswith("STO")]

    symbol_list = set(temp_df["TckrSymb"].tolist())
    for symbol in symbol_list:
        bhavDf = df
        bhavDf = bhavDf[bhavDf["TckrSymb"].str.startswith(symbol)]
        bhavDf = bhavDf[bhavDf["XpryDt"].str.startswith(expiry)]
        bhavDf = bhavDf[bhavDf["FinInstrmTp"].str.startswith("STO")]
        # print(symbol)
        if len(bhavDf) < 5:
            return {}

        ce_strike_df = bhavDf[bhavDf["FinInstrmNm"].str.contains("CE")].copy()
        ce_strike_df["OpnIntrst"] = ce_strike_df["OpnIntrst"].astype("int64")
        ce_strike_df = ce_strike_df[ce_strike_df['OpnIntrst'] != 0]
        ce_strike_idx = int(ce_strike_df["OpnIntrst"].idxmax())
        ce_strike = ce_strike_df["FinInstrmNm"].loc[ce_strike_idx]
        pe_strike_df = bhavDf[bhavDf["FinInstrmNm"].str.contains("PE")].copy()
        pe_strike_df["OpnIntrst"] = pe_strike_df["OpnIntrst"].astype("int64")
        pe_strike_idx = int(pe_strike_df["OpnIntrst"].idxmax())
        pe_strike = pe_strike_df["FinInstrmNm"].loc[pe_strike_idx]
        strikes_list = [ce_strike, pe_strike]
        for strikes in strikes_list:
            temp_query = strikes.split(yearmonth)
            query = temp_query[0]+datemonthyear+temp_query[1]
            token = fetch_token(query, "NFO")
            token_dict[strikes] = token

    print(token_dict)
    
    # Sort the token dictionary by token value (numerical order)
    sorted_tokens = dict(sorted(token_dict.items(), key=lambda item: int(item[1]) if item[1] else 0))
    
    # Take only the tokens after the first 200
    limited_tokens = dict(list(sorted_tokens.items())[:200])
    # limited_tokens = dict(list(sorted_tokens.items()))

    # Create tokens directory if it doesn't exist
    tokens_dir = os.path.join(os.getcwd(), "tokens")
    if not os.path.exists(tokens_dir):
        os.makedirs(tokens_dir)
        print(f"Created directory: {tokens_dir}")

    # Generate filename with current date
    filename = generate_token_filename()
    file_path = os.path.join(tokens_dir, filename)

    # Write token dictionary to file
    with open(file_path, 'w') as convert_file:
        # convert_file.write(json.dumps(token_dict))
        #savinf the first 200 tokens only
        convert_file.write(json.dumps(limited_tokens))
        

    print(f"Token data saved to: {file_path}")

if __name__ == "__main__":
    main()

