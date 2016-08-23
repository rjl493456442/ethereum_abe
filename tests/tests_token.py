import unittest
from abe import flags
from pyethapp.rpc_client import JSONRPCClient
from abe import logger
FLAGS = flags.FLAGS
from abe.token.driver.DCS.builtin import BuiltinDriver

class TokenTest(unittest.TestCase):
    
    def setUp(self):
        self.rpc_cli = JSONRPCClient(host = FLAGS.rpc_host, port = FLAGS.rpc_port)
        
    
    
