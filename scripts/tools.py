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
            cnt = self.db_proxy.get_table_count(FLAGS.blocks)
            for idx in range(cnt):
                if idx % 10 == 0:
                    blocks_indexs = [
                        [("number", pymongo.ASCENDING)],
                        [("hash", pymongo.ASCENDING)],
                        [("miner", pymongo.ASCENDING)],
                    ]
                    for index in blocks_indexs:
                        self.db_proxy.add_index(FLAGS.blocks+"test"+str(idx/10), index)
                
                time_start = time.time()
                self.db_proxy.insert(FLAGS.blocks+"test"+str(idx/10), self.db_proxy.get(FLAGS.blocks+str(idx), None, multi=True))
                print "elapsed %f" % (time.time() - time_start)

            cnt = self.db_proxy.get_table_count(FLAGS.uncles)
            for idx in range(cnt):
                if idx % 10 == 0:
                    uncles_indexs = [
                        [("mainNumber", pymongo.ASCENDING),("hash", pymongo.ASCENDING)],
                        [("hash", pymongo.ASCENDING)],
                        [("miner", pymongo.ASCENDING)],
                    ]   
                    for index in uncles_indexs:
                        self.db_proxy.add_index(FLAGS.uncles, index, block_height = block_height)

                time_start = time.time()
                self.db_proxy.insert(FLAGS.uncles+"test"+str(idx/10), self.db_proxy.get(FLAGS.uncles+str(idx), None, multi=True))
                print "elapsed %f" % (time.time() - time_start)
                        
