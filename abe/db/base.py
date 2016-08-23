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
            ("number", pymongo.ASCENDING),
            ("hash", pymongo.ASCENDING),
        ]
        self.db_proxy.add_index(FLAGS.blocks, blocks_indexs, block_height = block_height)

        txs_indexs = [
            ("blockNumber", pymongo.ASCENDING),
            ("hash", pymongo.ASCENDING),
        ]
        self.db_proxy.add_index(FLAGS.txs, txs_indexs, block_height = block_height)

        accounts_indexs = [
            ("address", pymongo.ASCENDING),
        ]
        self.db_proxy.add_index(FLAGS.accounts, accounts_indexs, block_height = block_height)

        uncles_indexs = [
            ("mainNumber", pymongo.ASCENDING),
            ("hash", pymongo.ASCENDING),
        ]
        self.db_proxy.add_index(FLAGS.uncles, uncles_indexs, block_height = block_height)
    
    def add_indexes_for_token(self, name):
        token_indexs = [
            ("account", pymongo.ASCENDING),
        ]
        self.db_proxy.add_index(FLAGS.balance_prefix + name, token_indexs)
        token_indexs = [
            ("from", pymongo.ASCENDING),
            ("to", pymongo.ASCENDING),
        ]
        self.db_proxy.add_index(FLAGS.token_prefix + name, token_indexs)

