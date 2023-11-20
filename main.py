# DEPENDENCIES
import datetime as dt
import asyncio
import websockets as wss
import hmac
import hashlib
import time
import json
import numpy as np
import random
import matplotlib.pyplot as plt
import scipy
import math


# CONSTANTS
API_KEY = "[REMOVED FOR SECURITY]"  # CoinBase api key
API_SECRET = "[REMOVED FOR SECURITY]"  # CoinBase api secret

WSS_ENDPOINT = "wss://advanced-trade-ws.coinbase.com"  # CoinBase WebSocket endpoint
WSS_PRODUCTS = ["BTC-USD"]  # List of product(s) to monitor
WSS_CHANNEL = "ticker_batch"  # WebSocket channel to subscribe to


# FUNCTIONS
def c_print(content: str,
            ticker: str = "info",
            timestamp: bool = True,
            verbose: bool = True):

    # FUNCTION: console print

    # PARAM (required): content: str: text to print
    # PARAM: ticker: type of alert
    # PARAM: timestamp: bool: whether to include timestamp
    # PARAM: verbose: bool: whether to print or not

    # RETURN: (print) output

    if verbose:  # If enabled
        output = f"[{ticker.upper()}]\t"  # uppercase ticker
        output += f"({dt.datetime.now()})\t" if timestamp else ""  # if timestamp is enabled, add timestamp
        output += f"{content}"  # add content

        print(output)  # print output


# FUNCTIONS
def a_timestamp():

    # FUNCTION: Generate a UNIX timestamp of the current time

    # RETURN: timestamp: str: unix timestamp

    timestamp = int(time.time())  # Get current time as unix timestamp

    return str(timestamp)  # Return timestamp as str


def a_wss_signature(timestamp: str = a_timestamp(),
                    channel: str = WSS_CHANNEL,
                    products: list[str] = WSS_PRODUCTS,
                    secret: str = API_SECRET):

    # FUNCTION: Generate a signature for WebSocket API calls.

    # PARAM: timestamp: str: UNIX timestamp.
    # PARAM: channel: str: WebSocket channel name.
    # PARAM: products: str: Currency pairs.
    # PARAM: secret: str: API secret key.

    # RETURN: signature: str: signature for use in WebSocket API calls.

    products_str = ""  # String to concatenate product ids to
    for product in products:  # Iterate over products list
        products_str += f"{product},"  # Append current product to string with a comma
    products_str = products_str[:-1]  # Remove last comma

    plaintext = f"{timestamp}{channel}{products_str}"  # Concatenate relevant fields

    plaintext_bytes = plaintext.encode("utf-8")  # Encode plaintext to utf-8
    secret_bytes = secret.encode("utf-8")  # Encode secret to utf-8

    hashed = hmac.new(secret_bytes, plaintext_bytes, hashlib.sha256)  # Hash plaintext using secret
    signature = hashed.hexdigest()  # Convert to hex

    return str(signature)  # Return result, ensuring str type


async def a_wss_subscribe(results: list[str] = [],
                          endpoint: str = WSS_ENDPOINT,
                          channel: str = WSS_CHANNEL,
                          products: list[str] = WSS_PRODUCTS,
                          key: str = API_KEY,
                          secret: str = API_SECRET,
                          iterations: int = -1,
                          verbose: bool = True):

    # FUNCTION: Subscribe to a data feed.

    # PARAM: results: list[str]: list to output data to, live
    # PARAM: endpoint: str: WSS endpoint
    # PARAM: channel: str: WebSocket channel name.
    # PARAM: products: list[str]: Currency pairs.
    # PARAM: key: str: API key.
    # PARAM: secret: str: API secret key.
    # PARAM: iterations: int: number of times to collect data points - -1 for infinite
    # PARAM: verboseL bool: whether to print events or not

    # RETURN: (no explicit return) appends data live into outer 'results' list

    async with wss.connect(endpoint) as ws:  # Connect to WebSocket
        c_print(ticker="api",
                content="Connecting to WebSocket...",
                verbose=verbose)  # Console print

        timestamp = a_timestamp()  # Get timestamp
        signature = a_wss_signature(timestamp=timestamp,  # Generate signature for heartbeat
                                    channel="heartbeats",
                                    products=products,
                                    secret=secret)
        products_str = "["  # String to concatenate product ids to
        for product in products:  # Iterate over products list
            products_str += f"\"{product}\","  # Append current product to string with a comma and quotes
        products_str = products_str[:-1] + "]"  # Remove last comma and add ending bracket
        request = f'''{{
                    "type": "subscribe",
                    "product_ids": {products_str},
                    "channel": "heartbeats",
                    "api_key": "{key}",
                    "timestamp": "{timestamp}",
                    "signature": "{signature}"
                }}'''  # Heartbeat request body
        await ws.send(request)  # Send request (subscribe to heartbeats)

        timestamp = a_timestamp()  # Get timestamp
        signature = a_wss_signature(timestamp=timestamp,  # Generate signature
                                    channel=channel,
                                    products=products,
                                    secret=secret)
        products_str = "["  # String to concatenate product ids to
        for product in products:  # Iterate over products list
            products_str += f"\"{product}\","  # Append current product to string with a comma and quotes
        products_str = products_str[:-1] + "]"  # Remove last comma and add ending bracket
        request = f'''{{
            "type": "subscribe",
            "product_ids": {products_str},
            "channel": "{channel}",
            "api_key": "{key}",
            "timestamp": "{timestamp}",
            "signature": "{signature}"
        }}'''  # Request body
        await ws.send(request)  # Send request (subscribe to feed)
        c_print(ticker="api",
                content="Connected to WebSocket.",
                verbose=verbose)  # Console print

        iteration = 0  # Iteration counter
        while iteration < iterations or iterations == -1:  # Loop specified times, or infinitely if -1
            try:
                response = await ws.recv()  # Wait until response arrives - should be every 5 seconds
            except:
                c_print(ticker="api",
                        content="WebSocket connection closed.",
                        verbose=verbose)  # Console print
                await ws.send(request)  # Send request (re-subscribe to feed)
            parsed_dict = json.loads(response)  # Parse to dict
            if parsed_dict["channel"] == channel:  # If data point is in desired channel
                results.append(response)  # Append received data to specified output list
                c_print(ticker="api",
                        content=f"Received data point {iteration+1} of {iterations}.",
                        verbose=verbose)  # Console print
                iteration += 1  # Increment iteration counter
            else:  # If data point is not in desired channel (usually from heartbeat subscription)
                c_print(ticker="api",
                        content=f"Extraneous data received; skipped.",
                        verbose=verbose)  # Console print

        c_print(ticker="api",
                content="Finished collecting data points.",
                verbose=verbose)  # Console print

        return results  # Return array of raw json strings


def d_collect_raw(points: int,
                  currencies: list[str],
                  verbose: bool = True):

    # FUNCTION: collect raw dataset (wrapper for api)

    # PARAM: points: int: number of data points to collect
    # PARAM: currencies: list[str]: currencies to pull data from
    # PARAM: verbose; whether to print logs.

    # RETURN: points: list[str]: list of raw json points in str format

    c_points = asyncio.get_event_loop().run_until_complete(a_wss_subscribe(results=[],
                                                                           iterations=points,
                                                                           products=currencies,
                                                                           verbose=verbose))  # Run api collection

    return c_points  # Return gathered points


def d_parse(data: str,
            currency: str,
            verbose: bool = True):

    # FUNCTION: Parse a single data point

    # PARAM (required): data: str: raw str of data point (from api call)
    # PARAM: currency: str: what currency to parse
    # PARAM: verbose: bool: whether to print logs

    # RETURN: point[0]: dict: data point in dict form

    parsed_dict = json.loads(data)  # Parse to dict

    events = parsed_dict["events"][0]["tickers"]  # Events included in data point
    point = events[0]  # Point

    c_print(ticker="data",
            content="Parsed data point.",
            verbose=verbose)  # Console print

    return point  # Return result


def d_parse_all(data: list[str],
                currency: str,
                verbose: bool = True):

    # FUNCTION: parse all data in raw str from api call (d_collect_raw())

    # PARAM (required): data: list[str]: raw data from api call
    # PARAM: currency: str: what currency to parse
    # PARAM: verbose: bool: whether to print logs

    # RETURN: points: list[dict]: parsed data

    c_print(ticker="data",
            content=f"Parsing {len(data)} data points...",
            verbose=verbose)  # Console print
    points = []  # Initialize data list
    if verbose:  # If verbose
        count = 0  # Iteration count
    for point in data:  # Iterate over raw data (note: point is raw, not parsed)
        #try:
        points.append(d_parse(data=point,
                              currency=currency,
                              verbose=False))  # Parse point and append to points
        c_print(ticker="data",
                content=f"Parsed data point {count+1} of {len(data)}.",
                verbose=verbose)  # Console print

        #except:
        #    i.c_print(ticker="data",
        #              content=f"Could not parse data point {count+1} of {len(data)}; skipping.",
        #              verbose=verbose)  # Console print
        if verbose:  # If verbose
            count += 1  # Increment count
    c_print(ticker="data",
            content=f"Parsed {count} data points.",
            verbose=verbose)  # Console print

    return points  # Return result


def d_process(data: dict,
              verbose: bool = True):

    # FUNCTION: Perform feature engineering on parsed data

    # PARAM (required): data: dict: data point
    # PARAM: verbose: whether to print logs

    # RETURN: point: ndarray: array of found values

    point_arr = []  # list of values in point
    skip = ["type", "product_id"]  # list of keys to skip
    for key in data:  # iterate over dict
        if key not in skip:  # if wanted key
            point_arr.append(float(data[key]))  # add value to array
    point = np.array(point_arr)  # Create numpy array

    c_print(ticker="data",
            content=f"Processed data point.",
            verbose=verbose)  # Console print

    return point  # Return numpy point


def d_process_all(data: list[dict],
                  verbose: bool = True):

    # FUNCTION: process all data from raw dict from output of d_parse_all()

    # PARAM: data: list[dict]: list of data points (output of d_parse_all())
    # PARAM: verbose: whether to print logs

    # RETURN: points: ndarray: 2d array of found values

    c_print(ticker="data",
            content=f"Processing {len(data)} data points...",
            verbose=verbose)  # Console print
    points_raw = []  # list of data points
    count = 0  # iteration counter
    for point_raw in data:  # iterate over data
        point = d_process(data=point_raw,
                          verbose=False)  # process data point
        points_raw.append(point)  # add data point to list
        c_print(ticker="data",
                content=f"Processed data point {count+1} of {len(data)}.",
                verbose=verbose)  # Console print
        count += 1  # Increment count

    points = np.array(points_raw)  # create ndarray of processed points

    c_print(ticker="data",
            content=f"Processed {len(data)} data points.",
            verbose=verbose)  # Console print

    return points  # return result


def d_export(data: np.ndarray,
             path: str = "datasets/export.csv",
             verbose: bool = True):

    # FUNCTION: Export dataset to csv

    # PARAM (required): data: ndarray: dataset (output of d_process_all())
    # PARAM: path: path of csv file to save to
    # PARAM: verbose: bool: whether to print logs

    # RETURN: (file) write to file specified by path param

    c_print(ticker="data",
            content=f"Exporting dataset to '{path}'...",
            verbose=verbose)  # Console print
    np.savetxt(path, data, delimiter=",")  # Save to file
    c_print(ticker="data",
            content=f"Exported dataset to '{path}'.",
            verbose=verbose)  # Console print


def d_import(path: str,
             verbose: bool = True):

    # FUNCTION: import dataset from csv

    # PARAM(required): path: str: path to csv file
    # PARAM: verbose: bool: whether to print logs

    # RETURN: imported: ndarray: dataset from csv file

    c_print(ticker="data",
            content=f"Importing dataset from '{path}'...",
            verbose=verbose)  # Console print
    imported = np.genfromtxt(path, delimiter=",")  # Import from csv
    c_print(ticker="data",
            content=f"Imported dataset from '{path}'.",
            verbose=verbose)  # Console print

    return imported  # Return dataset


def d_collect(points: int,
              currencies: list[str],
              path: str,
              batch: int,
              verbose: bool = True):

    # FUNCTION: Collect dataset

    # PARAM: points: int: number of points to collect
    # PARAM: currencies: list[str]: currencies to monitor
    # PARAM: path: str: path of savefile
    # PARAM: batch: int: at how many points to save the array
    # PARAM: verbose: bool: whether to print logs

    # RETURN: (file) dataset csv export referenced in path param
    # RETURN: data: ndarray: data collected

    iterations = int(points/batch)  # how many iterations to loop over
    remainder = int(points % batch)  # remainder after iterations

    c_print(ticker="data",
            content=f"Collecting {points} data points in {iterations} batches...",
            verbose=verbose)  # Console print

    c_print(ticker="data",
            content=f"Collecting batch 1 of {iterations}...",
             verbose=verbose)  # Console print
    raw = d_collect_raw(points=batch,
                        currencies=currencies,
                        verbose=verbose)  # collect one batch of data
    parsed = d_parse_all(data=raw,
                         currency=currencies[0],
                         verbose=verbose)  # parse data
    data = d_process_all(data=parsed,
                         verbose=verbose)  # process data points
    c_print(ticker="data",
            content=f"Collected batch 1 of {iterations}.",
            verbose=verbose)  # Console print
    d_export(data=data,
             path=path,
             verbose=verbose)  # save dataset

    for iteration in range(iterations-1):  # loop calculated number of times minus the one iteration already performed
        c_print(ticker="data",
                content=f"Collecting batch {iteration + 2} of {iterations}...",
                verbose=verbose)  # Console print
        raw = d_collect_raw(points=batch,
                            currencies=currencies,
                            verbose=verbose)  # collect one batch of data
        parsed = d_parse_all(data=raw,
                             currency=currencies[0],
                             verbose=verbose)  # parse data
        processed = d_process_all(data=parsed,
                                  verbose=verbose)  # process data points
        data = np.vstack((data, processed))  # add new data to bottom of dataset
        c_print(ticker="data",
                content=f"Collected batch {iteration + 2} of {iterations}.",
                verbose=verbose)  # Console print
        d_export(data=data,
                 path=path,
                 verbose=verbose)  # save dataset


# MAIN
if __name__ == "__main__":
    #d_collect(  # Collect data points
    #    points=5000,
    #    path="collected.csv",
    #    currencies=["BTC-USD"],
    #    batch=100
    #)

    data = d_import("collected.csv")[:, 0]  # Get spot prices only

    #data = (data - np.mean(data))/np.std(data)

    n = 49  # number of samples

    samples = []
    for i in range(n):
        samples.append(data[random.randint(0, len(data)-1)])

    print(samples)

    plt.hist(samples, 20)
    plt.savefig("histogram.png")

    # Hard coded the dataset in my report as the calculated once changes randomly each time
    example = np.array([29145.6, 29186.18, 29129.38, 29130.5, 29128.66, 29128.02, 29140.0, 29173.74, 29138.42, 29169.73, 29133.59, 29152.81, 29174.25, 29135.73, 29139.8, 29131.5, 29119.77, 29151.42, 29127.46, 29145.45, 29156.36, 29129.97, 29135.74, 29162.35, 29141.12, 29157.28, 29138.76, 29152.17, 29152.27, 29150.42, 29169.33, 29137.24, 29147.81, 29155.69, 29137.16, 29141.12, 29139.52, 29119.78, 29143.38, 29159.9, 29145.3, 29143.38, 29145.13, 29142.5, 29128.7, 29137.81, 29120.88, 29148.69, 29143.01])

    mean = np.mean(data)  # Mean
    std = np.std(data)  # Standard deviation

    s_mean = np.mean(example)  # Sample mean
    s_std = np.std(example)  # Sample std deviation

    confidence = 1.0 - 0.05/2  # Confidence level

    z = scipy.stats.norm.ppf(confidence)
    u = s_mean + z*(std/math.sqrt(n))  # Upper bound
    l = s_mean - z*(std / math.sqrt(n))  # Upper bound

    print(l, u)