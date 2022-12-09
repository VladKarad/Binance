from binance.client import Client
from config import apiKey, secretKey
import talib
import numpy as np
import pandas as pd
import time
import json

import asyncio
import websockets
import nest_asyncio
nest_asyncio.apply()

from datetime import datetime

kline_interval = Client.KLINE_INTERVAL_1MINUTE
kline_interval_format = '3m'   #1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 3d 1w 1M
sma_period = 8
angle_control = 50



client = Client(apiKey, secretKey)
all_tickers = pd.DataFrame(client.get_ticker())
usdt = all_tickers[all_tickers.symbol.str.contains('USDT')]
work = usdt[~((usdt.symbol.str.contains('UP')) | (usdt.symbol.str.contains('DOWN'))) ]
symbol_wss = []
my_dict = {}


for i in work["symbol"]:
    my_dict[i] = pd.DataFrame()
    symbol_wss.append(i.lower()+'@kline_'+kline_interval_format)
    my_dict[i][i] = pd.DataFrame(client.get_klines(symbol= i, interval=kline_interval, limit=sma_period + 2 )).loc[:,4]
    my_dict[i][i+"_sma"] = talib.SMA(my_dict[i][i], sma_period)



symbol_wss_dop = json.dumps({"method": "SUBSCRIBE", "params": symbol_wss[1:],  "id": 1})

async def candle_stick_data():

    url = "wss://stream.binance.com:9443/ws/" 
    first_pair = symbol_wss[0] #first pair

    async for sock in websockets.connect(url+first_pair):

        try:
            
            pairs = str(symbol_wss_dop)
                   
            await sock.send(pairs)
            
            while True:
            
                resp = await sock.recv()                
                
                
                try:
                    pair_id = str((json.loads(resp.replace("'",'"')).get('k')).get('s'))
                    pair_price = float((json.loads(resp.replace("'",'"')).get('k')).get('o'))
                    ts = int(((json.loads(resp.replace("'",'"')).get('E'))))/1000.0
                    if my_dict[pair_id].loc[:,pair_id].iloc[-1]  != pair_price:
                        dop = pd.DataFrame({pair_id : [pair_price], pair_id+"_sma" : [0]  })
                        my_dict[pair_id] = pd.concat((my_dict[pair_id], dop), ignore_index=True)
                        my_dict[pair_id][pair_id+"_sma"] = talib.SMA(my_dict[pair_id][pair_id], sma_period)
                        slope = my_dict[pair_id][pair_id+"_sma"].iloc[-1] - my_dict[pair_id][pair_id+"_sma"].iloc[-2] 
                        angle = np.rad2deg(np.arctan2(slope,1))
                        my_dict[pair_id] = my_dict[pair_id].iloc[-sma_period-2:]
                        if angle > angle_control or angle < - angle_control:
                            print('Time Frame: ', kline_interval_format, 'All Pairs: ', len( my_dict),'Datetime: ',\
                                datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'), 'Pair: ',pair_id , 'Angle: ',angle.round(2) )
                    
                
                except:
                    continue     
                       
        except websockets.ConnectionClosed:
            continue

asyncio.run(candle_stick_data())