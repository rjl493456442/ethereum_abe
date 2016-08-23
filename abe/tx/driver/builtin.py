from abe import logger
from abe import flags
from abe import constant
from abe.db import base
from pyethapp.rpc_client import JSONRPCClient
import time
FLAGS = flags.FLAGS

class BuiltinDriver(base.Base):
	
	@property
	def type(self):
		return 'tx_driver'

	def __init__(self):
		self.logger = logger
		self.rpc_cli = JSONRPCClient(host = FLAGS.rpc_host, port = FLAGS.rpc_port, print_communication = False)
		super(BuiltinDriver, self).__init__()

	def pending_txs(self):
		self.filter_id = self.rpc_cli.call(constant.METHOD_NEW_PENDING_TX_FILTER)
		while True:
			res = self.rpc_cli.call(constant.METHOD_GET_FILTER_CHANGES, self.filter_id)
			if res:
				print res
			else:
				time.sleep(FLAGS.poll_interval)




