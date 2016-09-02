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
        
    def test_sync_it(self):
        block_heights = [470168, 470169, 470173, 482025, 482026]
        
        # first block of log
        block_height = 470168
        self.handler.sync_it(block_height)
        res=self.dbproxy.get(FLAGS.block_it, {"block_number":block_height}, multi=False)
        self.assertNotEqual(res, None)
        self.assertEqual("0xb813782614a12660f88d8ebb7a0aaa4f23c77a6934b5061023e5122fce01122d", res["txs"][0]['tx_hash'])
        self.assertEqual(12, len(res['txs'][0]['internal_txs']))


    def tearDown(self):
        #self.dbproxy.mongo_cli.mc.drop_collection("block_it")
        #self.dbproxy.mongo_cli.mc.drop_collection("internaltxs_4")
        #self.dbproxy.mongo_cli.mc.drop_collection("meta")
        pass
        
        