from abe import dbproxy
from abe import flags
import time
FLAGS = flags.FLAGS
class Tool(object):
    def __init__(self):
        self.db_proxy = dbproxy.MongoDBProxy()

    def service(self, service_name):
        if service_name == "add_tx_num":
            cnt = self.db_proxy.get_table_count(FLAGS.txs)
            for idx in range(cnt):
                time_start = time.time()
                down_limit = FLAGS.table_capacity * idx
                up_limit = FLAGS.table_capacity * (idx+1)
                for height in range(down_limit, up_limit):
                    res = self.db_proxy.get(FLAGS.txs+str(idx), {"blockNumber" : hex(height)}, multi = True)
                    if res and len(res) > 0:
                        self.db_proxy.update(FLAGS.blocks, {"number": height}, {"$set": {"txs_num":len(res)}}, block_height = height)
                    else:
                        self.db_proxy.update(FLAGS.blocks, {"number": height}, {"$set": {"txs_num":0}}, block_height = height)    
                print "process %d to %d blocks finish, elapsed %f" % (down_limit, up_limit-1, time.time() - time_start)
