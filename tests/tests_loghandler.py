from abe import utils
from abe import dbproxy
from abe.block.driver import builtin
from abe.block.driver import log_handler
import unittest
import os
import glob
from abe import flags
FLAGS = flags.FLAGS

class LoghandlerTest(unittest.TestCase):
    
    def setUp(self):
        self.dbproxy = dbproxy.MongoDBProxy()
        self.handler = log_handler.LogHandler(self.dbproxy)
        FLAGS.table_capacity = 100000
    def test_sync_it(self):
        block_heights = [470168, 470169, 482014, 482015,482025,482026, 500000]
        
        # first block of log
        for block_height in block_heights:
            self.handler.sync_internal_transaction(block_height)
            
           


    def tearDown(self):
        #self.dbproxy.mongo_cli.mc.drop_collection("block_it")
        #self.dbproxy.mongo_cli.mc.drop_collection("internaltxs_4")
        #self.dbproxy.mongo_cli.mc.drop_collection("meta")
        pass
        
        