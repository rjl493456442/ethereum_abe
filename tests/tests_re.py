from abe import utils
from abe import dbproxy
from abe.block.driver import builtin
import unittest
import os
import glob

class ReTest(unittest.TestCase):
    
    def setUp(self):
        pass
    
    def test_re(self):
        for filename in glob.glob("tx.log*"):
            f = open(filename, "r")
            for line in f.readlines():
                info = utils.regular_extract(line)
                if info['type'] == 0:
                    self.assertEqual(len(info["blockhash"]), 64+2)
                    self.assertEqual(len(info["txhash"]), 64+2)
                    self.assertEqual(len(info["from"]), 40+2)
                    self.assertEqual(len(info["to"]), 40+2)
                if info['type'] == 1:
                    self.assertEqual(len(info["blockhash"]), 64+2)
                    self.assertEqual(len(info["txhash"]), 64+2)
            
    
        
        