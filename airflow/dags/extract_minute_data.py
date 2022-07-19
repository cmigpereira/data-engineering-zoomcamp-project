import argparse
import requests
import pandas as pd
import os


def get_price_api(coin, currency):
    '''
    Gets the last minute values (in currency) of a given coin
    '''
    api_url = f'https://min-api.cryptocompare.com/data/v2/histominute?fsym={coin}&tsym={currency}&limit=1'
    raw = requests.get(api_url).json()

    # raise exception if API calls are not working
    if raw['Response'] != 'Success':
        raise Exception("Error in the API call. Check rate limit or other causes.")

    # only use certain columns
    df = pd.DataFrame(raw['Data']['Data'])[['time', 'high', 'low', 'open']].set_index('time')
    df['coin'] = coin
    df['currency'] = currency
    df.index = pd.to_datetime(df.index, unit = 's')
    df = df.rename_axis('time').reset_index()
    df = df.tail(1)

    return df


def get_crypto_price(currency):
    '''
    Gets 'BTC', 'ETH', 'USDT', 'USDC', and 'BNB' last minute values (in currency)
    '''
    df = pd.DataFrame()

    for coin in ['BTC', 'ETH', 'USDT', 'USDC', 'BNB']:
        df = pd.concat([df, get_price_api(coin, currency)])

    return df

def save_output(df, output):
    '''
    Saves the minute values extraction to the output folder
    '''
    if not os.path.exists(os.path.dirname(output)):
        try:
            os.makedirs(os.path.dirname(output))
        except:
            raise PermissionError("Folder does not exist or there are no permissions to write to the folder.")
    
    df.to_parquet(output)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument('--currency', required=True)
    parser.add_argument('--output', required=True)

    args = parser.parse_args()

    currency = args.currency
    output = args.output

    try: 
        dataset_minute = get_crypto_price(currency)
    except:
        print('Exception occurred in getting the data.')

    try: 
        save_output(dataset_minute, output)
    except:
        print("Exception occured in saving the data.")