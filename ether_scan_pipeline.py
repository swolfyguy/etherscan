import datetime

import requests
from pymongo import MongoClient

BASE_URL = "https://api.etherscan.io/api/"
API_KEY = "EM2BMZ6JTD8HDWIIJECEY31DDH9FBGJ1EG"

import logging

current_date = datetime.datetime.now()


def setup_logger():
    logger = logging.getLogger("logs")
    formatter = logging.Formatter('%(message)s')
    filehandler = logging.FileHandler(f"{current_date.date()}.txt")
    filehandler.setFormatter(formatter)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    logger.addHandler(filehandler)
    logger.addHandler(streamHandler)
    return logger


class EtherScan:
    def __init__(self):
        self.base_url = BASE_URL
        self.api_key = API_KEY
        self.client = MongoClient("mongodb://localhost:27017/")
        self.mydatabase = self.client["etherscan"]
        logger.info("creating MOngo db client")

    def entry_data_into_mongo(self, collection_name: str, result: list):
        mycol = self.mydatabase[collection_name]
        x = mycol.insert_many(result)
        print('ids of inserted documents\n---------------------')
        for id in x.inserted_ids:
            print(id)
        pass

    def fetch_transaction_details(self, page: int):
        param = {
            "module": "account",
            "action": "txlist",
            "address": "0xddbd2b932c763ba5b1b7ae3b362eac3e8d40121a",
            "startblock": 0,
            "endblock": 99999999,
            "page": page,
            "offset": 1000,
            "sort": "ASC",
            "apikey": self.api_key
        }
        data = requests.request("GET", url=self.base_url, params=param)
        return data.json()["result"]


if __name__ == "__main__":
    logger = setup_logger()
    ether_obj = EtherScan()
    page = 0
    while True:
        try:
            result = ether_obj.fetch_transaction_details(page=page)
            if not result:
                break
            ether_obj.entry_data_into_mongo("collection_ether", result)
            logger.info(f"result found for offset {page} is {result}")
            page = page + 1
        except Exception as e:
            logger.error(f"error while getting details from page {e}")
            break
