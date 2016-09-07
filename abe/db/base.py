"""
Base class for classes that need modular mongodb access.
"""

from abe import flags
from abe import utils
from abe import dbproxy
import pymongo

FLAGS = flags.FLAGS

class Base(object):
    def __init__(self):
        self.db_proxy = dbproxy.MongoDBProxy()

    def add_indexes(self, block_height):
        blocks_indexs = [
            [("number", pymongo.ASCENDING)],
            [("hash", pymongo.ASCENDING)],
            [("miner", pymongo.ASCENDING)],
        ]
        for index in blocks_indexs:
            self.db_proxy.add_index(FLAGS.blocks, index, block_height = block_height)

        tx_indexes = [
            [("blockNumber", pymongo.ASCENDING)],
            [("hash", pymongo.ASCENDING)],
            [("from", pymongo.ASCENDING)],
            [("to", pymongo.ASCENDING)],

        ]
        for index in tx_indexes:
            self.db_proxy.add_index(FLAGS.txs, index, block_height = block_height)

        accounts_indexs = [
            [("address", pymongo.ASCENDING)],
        ]
        for index in accounts_indexs:
            self.db_proxy.add_index(FLAGS.accounts, index)

        uncles_indexs = [
            [("mainNumber", pymongo.ASCENDING),("hash", pymongo.ASCENDING)],
            [("hash", pymongo.ASCENDING)],
            [("miner", pymongo.ASCENDING)],
        ]
        for index in uncles_indexs:
            self.db_proxy.add_index(FLAGS.uncles, index, block_height = block_height)

    def add_indexes_for_token(self, name):
        token_indexs = [
            [("account", pymongo.ASCENDING)],
        ]
        for index in token_indexs:
            self.db_proxy.add_index(FLAGS.balance_prefix + name, index)

        token_indexs = [
            [("transactionHash", pymongo.ASCENDING)],
            [("from", pymongo.ASCENDING)],
            [("to", pymongo.ASCENDING)],
        ]
        for index in token_indexs:
            self.db_proxy.add_index(FLAGS.token_prefix + name, index)

