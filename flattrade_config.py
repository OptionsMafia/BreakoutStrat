import pandas as pd
import time
import pyotp
import requests
import json
from urllib.parse import parse_qs, urlparse
import hashlib
from NorenRestApiPy.NorenApi import NorenApi # type: ignore

class FlatTradeApiPy(NorenApi):
    def __init__(self):
        NorenApi.__init__(self, host='https://piconnect.flattrade.in/PiConnectTP/', 
                           websocket='wss://piconnect.flattrade.in/PiConnectWSTp/', 
                           eodhost='https://web.flattrade.in/chartApi/getdata/')

def get_flattrade_api():
    # APIKEY = '9d87fcbbb8eb47b6b6d577acf3882266' #Naveen
    # APIKEY = '5b6a1c26fa6c477eb7a952e85f437080' #Raga
    APIKEY = 'a3f9e4125db14f55b63adc48203a28c2' #Sneha

    # secretKey = '2025.09e4f220bec24ced931170e7ee7ba611c8517eb1705b65ac' #Naveen
    # secretKey = '2025.4d2983767a4349308837c84a64c76cb2e5b7537b45497933' #Raga
    secretKey = '2025.2ff09a964f4a4ca18dc8eca42ca9858fa3b7d13dbea3850c' #Sneha

    # totp_key = 'F4A3KMU5W4L6P6IQ2LV6J467S4VQTA7Q' #Naveen
    # totp_key = '2RGIA2N5WQXPF7G3H2P6UAB44F6742E6' #Raga
    totp_key = 'CQ5W4324346R6QJ6F3N75C6N64Z5752R' #Sneha

    # password = 'Godmode@6' #Nav
    password = 'Hellokitty@1' #raga and sneha same pass

    # userid = 'FZ11934' #Nav
    # userid = 'FZ16084' #Raga
    userid = 'FZ16825' #Sneha
    
    headerJson = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.74 Safari/537.36", 
                  "Referer": "https://auth.flattrade.in/"}
    
    # Create session and get SID
    ses = requests.Session()
    sesUrl = 'https://authapi.flattrade.in/auth/session'
    passwordEncrpted = hashlib.sha256(password.encode()).hexdigest()
    res_pin = ses.post(sesUrl, headers=headerJson)
    sid = res_pin.text
    print(f'sid {sid}')
    
    # Get auth code
    url2 = 'https://authapi.flattrade.in/ftauth'
    payload = {"UserName": userid, "Password": passwordEncrpted, "PAN_DOB": pyotp.TOTP(totp_key).now(),
               "App": "", "ClientID": "", "Key": "", "APIKey": APIKEY, "Sid": sid,
               "Override": "Y", "Source": "AUTHPAGE"}
    res2 = ses.post(url2, json=payload)
    reqcodeRes = res2.json()
    print(reqcodeRes)
    
    # Parse code from redirect URL
    parsed = urlparse(reqcodeRes['RedirectURL'])
    reqCode = parse_qs(parsed.query)['code'][0]
    
    # Get API token
    api_secret = APIKEY + reqCode + secretKey
    api_secret = hashlib.sha256(api_secret.encode()).hexdigest()
    payload = {"api_key": APIKEY, "request_code": reqCode, "api_secret": api_secret}
    url3 = 'https://authapi.flattrade.in/trade/apitoken'
    res3 = ses.post(url3, json=payload)
    token_response = res3.json()
    print(token_response)
    token = token_response['token']
    
    # Initialize API
    api = FlatTradeApiPy()
    api.set_session(userid=userid, password=password, usertoken=token)
    
    return api, token
