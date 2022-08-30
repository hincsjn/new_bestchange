import pandas as pd
import requests
import time
from datetime import datetime
import pytz

import asyncio
from aiogram import Bot

import httplib2
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials

import zipfile, io
import os
import json

# from flask import Flask


start_time = time.time()

to_json = {
  "type": "service_account",
  "project_id": "sstesting",
  "private_key_id": "1e1d26c21d2b468a0400d0927012ab00e83c644d",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCHA8TW+716rtkn\n6mW5w4sPHjlDdcetJoDnZHdu85fVbZsN5M3sDrxL5J7GjZv1Aaegw15xRg+0LDXP\n8lFuFgqmH+uulAPyhv0Se44N6Doce9BjiSJZLkpgvJGEEBeZKRbshzLotrDwJ9Y/\nWufG8bW8scoxbvER+39z5yxmhZqJWmy7UIbLTfo6HENmB2n4oZjctY04dIjN9z2M\nfqc8al+SAJ7qi6NrdwZMZFuDroy+z0BzJbvkrmD0PTSC4hGqiw2prTjLFR4dp1Vv\ni2Rnxk+om6OnK3nsBKoro8YXApYTuqBLJ4zdWJF7KrTg3PHb8thvbZsUyR+aLrA/\nGsYJvzx1AgMBAAECggEAA3FGKJEcoK8qnEjutRDWT9lebmjnYXPU35GBNhQB8BAu\nXulRks5BYNIAdmGP82xKYN/6XXsC1X43FQlBqEPpm5i+wqHFg/6LH1iFI7ejG6zQ\ngGkztgBnJxZHw41BfMc+bWN5GdPmqQjq/oyL0lfBYYFK/X2tqd62vjbLvAV2DkB1\nf/tAUbjS8LqNlSAVozKlx7ze7Z2WzrvVE0wKXQC5wseG9iBffmxbCnd/B50U8XyH\n8xpXPrVejGOLxpDRuEOd3CsFMGlrPoN6YnyyC86DJwav3FyWXkoqNJH0yH2r0kAg\n5YA2iOi2K6xvzmrOtaqJbowITK1J0QgbUgkLOyL8dQKBgQC8l1/s9vYGJPSuhYkJ\nSJ6KuR7GgRNxJu0dVZklgT16DrFNEHOr0jIiNauTnulbyJVuXcsrkhHjgM0KdqYe\nxxiM67LbeKzdTiNfIeyh6GHz2J+FsHDRRiwEp1jHhRbymx3gzavBH5WgTPA5aZ31\nchg8qaJl+24IN/ZbyRkn2j0D/wKBgQC3Rf0eC+8P4XM3KrG8kLcSEC+RyYfw19AX\ndk3jntHHGgQjLbDEdecPeNd/kMx1qY1m5fhO5eWuW/vb5de2DfxdiehiniYfbEiJ\nE2Qnr98UVoU/95RmVJFU+Sk8ZR2wC7GPmEbvkl4fiqtRgsubDtB7byno2WFzQXJW\n4/MF4B3viwKBgCHd6TsLqmi2ED6a+l3xbY8p6U3qdgxW2jPvYD4s9FZL9ykIsE0F\nxT0BeFtdKTjzT2pva4HajF3Xjnq3jeNvC4ia9xaUmC5xzsZRuEXnDlgU6ai/Y7Mh\nL9xyFO5XhyRwGLB7HsHioyMTTfxxbA1cvN9/8wrvWPYe3p3jAiJ2/YgPAoGBAJ3/\nrXYgzakAMKbHnNC2Zc0hvRDPD+3278O6Tu3DtpASAq0dL74+8sLo58dm2o05bdje\nu1GxanAFhryNioi9x+oQARI7yxvd6y6ZVAfO29+Zs2hxFTOfBmeeIgmaFpz1h88G\ndWkF4zUIBCfSPZtgiyVOsW+3MAb/zgXQoGtZShV/AoGBAI4ckDle8OxzcQQuScMh\ndbOhg0rBRiqCmVUC9v40ISO/Nu0+9YVTV5gtm3oGJMIQ5okVZalBGNjq0At1GcqK\np7YFNcUkdgjZhLzldp6dqgC5HhX0HOqLZf/CTxMu6CyudxM5EyDsv0eeyis7T6kW\nDKMqPUxwmag4ekgJYzPmidMg\n-----END PRIVATE KEY-----\n",
  "client_email": "account@sstesting.iam.gserviceaccount.com",
  "client_id": "109287569938384590418",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/account%40sstesting.iam.gserviceaccount.com"
}

with open('test.json', 'w') as f:
    f.write(json.dumps(to_json))
CREDENTIALS_FILE = 'test.json'
# Переменные Google sheets API
spreadsheet_id = '1dba0Xik4GJKlZvM8o2gUapNu95drAZShtrs9MNSYWPA' 
# spreadsheet_id = '1i5HmnZbcxpjRcGs2pU2GVYaEB6Z3OjjvO0t4uKXHgKY' 


credentials = ServiceAccountCredentials.from_json_keyfile_name(
    CREDENTIALS_FILE,
    ['https://www.googleapis.com/auth/spreadsheets',
     'https://www.googleapis.com/auth/drive'])
httpAuth = credentials.authorize(httplib2.Http())
service = apiclient.discovery.build('sheets', 'v4', http = httpAuth)


def get_gs_vals(adress) -> list([]):
    val = service.spreadsheets().values().get(   
        spreadsheetId=spreadsheet_id,
                range=adress,
                majorDimension="ROWS",
        ).execute()

    return val['values'] 


def send__to_gs(val, adress):
    service.spreadsheets().values().batchUpdate(   
        spreadsheetId=spreadsheet_id,
        body={
            "valueInputOption": "USER_ENTERED",
            "data": [
                {'range':adress,
                "majorDimension": "ROWS",
                "values": val}]
            }
        ).execute()


def get_cur_time():
    moscow_time = str(datetime.now(pytz.timezone('Europe/Moscow')))[:19]
    return moscow_time


def bot_send(table_name):
    
    token = '5592191973:AAEGYW4yfGG2nxaZ86uHTSU1pOVS3kOMIZE' 

    bot = Bot(token)

    def get_gs_vals(address, type='str'):
        val = service.spreadsheets().values().get(   
            spreadsheetId=spreadsheet_id,
                    range=table_name+address,
                    majorDimension="ROWS",
            ).execute()
        
        if type == 'list':
            return val['values']
        elif type == 'str':
            return val['values'][0][0] 


    # отправка сообщения // для тестов этот чат -648363105
    async def send_msg(id=-794947816, msg='упс! что-то не так'):
        await bot.send_message(id, msg, parse_mode='Markdown')


    def vals_notif(address='A6:D'):
        pairs = get_gs_vals(address, type='list')
        return pairs


    def extract_from_brackets(s):
        s = s.split('(')[-1].split(')')[0]
        return s


    def main_bot():
        notif_usdt = get_gs_vals('H1')
        print(notif_usdt)
        if notif_usdt in ['True', 'Истина', 't', 'T', 'ИСТИНА', 'TRUE']:
            address = 'G6:J'
        else:
            address = 'A6:D'
        rows = vals_notif(address=address)
        msg = '*BestChange все:*\n'

        for row in rows:
            pair = row[0].split(' => ')
            # print(pair)
            val1 = pair[0]
            val2 = pair[1]
            spread = row[3]

            min_spread = get_gs_vals('F1')
            if ',' in min_spread:
                min_spread = min_spread.replace(',', '.')
            if min_spread == '':
                min_spread = 0
            min_spread = float(min_spread)
            

            if '%' in spread:
                spread = spread[:-1]
            if ',' in spread:
                spread = spread.replace(',', '.')
            spread = float(spread)
            if spread >= min_spread and not(extract_from_brackets(val2) in msg):
                val1 = extract_from_brackets(val1)
                val2 = extract_from_brackets(val2)
                msg += f'{val1} => {val2}: {spread}%\n'
            elif spread < min_spread:
                break


        if msg != '*BestChange все:*\n':
            asyncio.run(send_msg(msg=msg))
            print(msg)
        else:
            print('Ничего не отправляем')


    main_bot()
# /////////////////// Main ///////////////////

def extract_code(s):
        s = s.split('(')[-1].split(')')[0]
        return s

        
def get_pairs(ids):
    
    def get_id(name):   
        for row in ids:
            val = row[0].split(';')
            if val[2] == name:
                return str(val[0])

    vals = get_gs_vals('тех BestChange!A2:A')
    pairs_bestchange = []
    pairs_binance = []
    for i in vals:
        for j in vals:
            val1 = i[0]
            val2 = j[0]
            if val1 != val2:
                pairs_bestchange.append((val1, get_id(val1), val2, get_id(val2)))
                pairs_binance.append((extract_code(val1), extract_code(val2)))
    # print(len(pairs))

    return pairs_bestchange, pairs_binance


def download_bestchange(update=True):
    if update:
        r = requests.get('http://api.bestchange.ru/info.zip')
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall("improving/bestchange_zip")
    file_rates = 'improving/bestchange_zip/bm_rates.dat'
    file_ids = 'improving/bestchange_zip/bm_cy.dat'
    enc = 'windows-1251'

    df_rates = pd.read_csv(file_rates, header=None)
    rates = df_rates.values.tolist()
    
    df_ids = pd.read_csv(file_ids, header=None, encoding=enc)
    ids = df_ids.values.tolist()
    return rates, ids


def get_name(id):
    for row in ids:
        val = row[0].split(';')
        if val[0] == id:
            return val[2]

        
def get_all_rates(rates):
    data = {}
    for row in rates:
        val = row[0].split(';')
        try:
            data[f'{val[0]}/{val[1]}'] = {
                'exchange_id': int(val[2]),
                'rate': float(val[3]) / float(val[4]),
                'rate - 3': float(val[3]),
                'rate - 4': float(val[4]),
                'reserve': float(val[5]),
                'min_sum': float(val[8]),
                'max_sum': float(val[9]),
                }
        except ZeroDivisionError:
            pass
    
    return data


def get_binance_rates(pairs):

    def get_trading_rates():
        info_url = 'https://api3.binance.com/api/v3/exchangeInfo'
        pairs_info = requests.get(info_url)  
        pairs_info = pairs_info.json()['symbols']

        prices_dic = {}
        prices_url = 'https://api.binance.com/api/v3/ticker/price'
        prices = requests.get(prices_url).json()

        for row in prices:
            prices_dic[row['symbol']] = row['price']

        rates = {}
        for pair_info in pairs_info:
            if pair_info['status'] == 'TRADING':
                price = prices_dic[pair_info['symbol']]
                rates[pair_info['symbol']] = float(price)

        return rates

    trading_rates = get_trading_rates()
    trading_pairs = trading_rates.keys()

    not_found = []
    rates = {}
    for pair in pairs:
        val1 = pair[0]
        val2 = pair[1]


        if val1+val2 in trading_pairs:
            price = trading_rates[val1+val2]
        elif val2+val1 in trading_pairs:
            price = 1 / trading_rates[val2+val1]
        else:
            try:
                price = trading_rates[val1+'USDT'] * 1 / trading_rates[val2+'USDT']
            except:
                price = ''
        rates[val1+val2] = price

    return rates


def sort_table(table):
    df = pd.DataFrame(table, columns = ['Pair', 'BestChange_rate', 'Binance_rate', 'Spread'])
    df_without_empties = df[df['Spread'] != ''].drop_duplicates(keep='first')
    sorted_df = df_without_empties.sort_values('Spread', axis=0, ascending=False)
    df = sorted_df.loc[:, ['Pair', 'BestChange_rate', 'Binance_rate', 'Spread']]
    res = df.values.tolist()

    return res


def main():
    send__to_gs([['Идет обновление..']], 'BestChange!A3')
    bestchange_data = download_bestchange(update=True)
    bestchange_rates = get_all_rates(bestchange_data[0])
    all_pairs = get_pairs(bestchange_data[1])
    bestchange_pairs = all_pairs[0]
    binance_pairs = all_pairs[1]
    binance_rates = get_binance_rates(binance_pairs)
    res = []
    for i in range(len(bestchange_pairs)):
        row = bestchange_pairs[i]
        bestchange_val1 = row[0]
        bestchange_val2 = row[2]
        id1 = row[1]
        id2 = row[3]
        try:
            bestchange_price = 1 / bestchange_rates[f'{id1}/{id2}']['rate']
        except:
            bestchange_price = ''

        binance_val1 = extract_code(bestchange_val1)
        binance_val2 = extract_code(bestchange_val2)
        try:
            binance_price = binance_rates[binance_val1 + binance_val2]
        except KeyError:
            binance_price = ''

        if bestchange_price == '' or binance_price == '':
            row = [bestchange_val1 + ' => ' + bestchange_val2, bestchange_price, binance_price, '']
        else:
            spread = (float(bestchange_price) - float(binance_price)) / float(binance_price)
            row = [bestchange_val1 + ' => ' + bestchange_val2, bestchange_price, binance_price, spread]

        
        res.append(row)

    

    res = sort_table(res)
    usdt_list = []
    for i in res:
        if 'USDT' in i[0].split('=>')[0]:
            usdt_list.append(i)
    rows = get_gs_vals('BestChange!C2')[0][0]

    res = res[:int(rows)+1]
    usdt_list = usdt_list[:int(rows)+1]

    send__to_gs(res, 'BestChange!A6')
    send__to_gs(usdt_list, 'BestChange!G6')

    send__to_gs([[get_cur_time()]], 'BestChange!A2')
    send__to_gs([['']], 'BestChange!A3')
    bot_send('BestChange!')


main()
# bot_send('BestChange!')
