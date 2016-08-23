from abe import flags, utils
FLAGS = flags.FLAGS


class BlockAPI(object):
	def __init__(self, block_driver = None, *args, **kwargs):
		if not block_driver:
			block_driver = FLAGS.block_driver
		self.driver = utils.import_object(block_driver)

	def synchronize(self):
		'''
		(1)get the block number since last execution
		(2)retrieve the miss blocks in range [last_block - greenlet_num, last_block) with repeat check
    	(3)retrieve the  blocks in range (last_block, greenlet_num + last_block] with repeat check
		(4)obtain blocks from the last block + greenlet_num + 1 to the newest one in the net
		(5)synchronize the new arrival blocks with fork-check
		'''
		self.driver.synchronize()

