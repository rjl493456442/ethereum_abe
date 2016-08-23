from abe import flags, utils
FLAGS = flags.FLAGS

class TxAPI(object):
	def __init__(self, tx_driver = None, *args, **kwargs):
		if not tx_driver:
			tx_driver = FLAGS.tx_driver
		self.driver = utils.import_object(tx_driver)

	def get_pending_txs(self):
		self.driver.pending_txs()