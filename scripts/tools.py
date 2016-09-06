from abe import dbproxy
from abe import flags
import time
import pymongo
FLAGS = flags.FLAGS
class Tool(object):
    def __init__(self):
        self.db_proxy = dbproxy.MongoDBProxy()

    def service(self, service_name):
        if service_name == "merge":
            cnt = self.db_proxy.get_table_count(FLAGS.txs)
            for idx in range(cnt):
                time_start = time.time()
                self.db_proxy.insert(FLAGS.txs+"test"+str(idx/10), self.db_proxy.get(FLAGS.txs+str(idx), None, multi=True))
                print "elapsed %f" % (time.time() - time_start)
                        
