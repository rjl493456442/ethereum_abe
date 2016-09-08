from abe import dbproxy
from abe import flags
import time
import pymongo
FLAGS = flags.FLAGS
class Tool(object):
    def __init__(self):
        self.db_proxy = dbproxy.MongoDBProxy()

    def service(self, service_name, shardId = None):
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
                self.db_proxy.insert(FLAGS.blocks+"test"+str(idx/10), self.db_proxy.get(FLAGS.blocks+str(idx), None, multi=True), multi = True)
                print "[INFO]merge block tables finish, elapsed %f" % (time.time() - time_start)

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
                self.db_proxy.insert(FLAGS.uncles+"test"+str(idx/10), self.db_proxy.get(FLAGS.uncles+str(idx), None, multi=True), multi = True)
                print "[INFO]merge uncle tables finsh, elapsed %f" % (time.time() - time_start)

        elif service_name == "statistic":
            res = self.db_proxy.get(FLAGS.meta, {"sync_record" : "ethereum"}, multi = False)
            if res and res.has_key("statistic_flag"):
                statistic_flag = res["statistic_flag"]
            else:
                statistic_flag = -1
            cnt = self.db_proxy.get_table_count(FLAGS.blocks)

            if isinstance(shardId, int):
                if shardId <= statistic_flag:
                    print "[INFO]shardId %d has statistic already!" % (shardId); return

                if shardId == statistic_flag + 1 and shardId <= cnt - 2:
                    time_start = time.time()
                    self._statistic(shardId)
                    self.db_proxy.update(FLAGS.meta, {"sync_record" : "ethereum"}, {"$set" : {"statistic_flag":shardId}}, multi = False)
                    print "[INFO]statistic finish, elapsed %f" % (time.time() - time_start); return

                if shardId == statistic_flag + 1 and shardId > cnt - 2:
                    print "[INFO] %d is in synchronzation, wait for the synchronzation end" % (shardId); return

                if shardId > statistic_flag + 1:
                    print "[INFO] %d - %d shard hasn't been statistic, statistic those first" % (statistic_flag+1, cnt-2); return

            else:
                print "[INFO]invalid param shardId %s, statistic failed!" % (shardId); return

        elif service_name == 'erase_statistic':
            time_start = time.time()
            self._erase_statistic()
            self.db_proxy.update(FLAGS.meta, {"sync_record" : "ethereum"}, {"$set" : {"statistic_flag":-1}}, multi = False)

            print "[INFO]erase statistic finish, elapsed %f" % (time.time() - time_start); return


    def _statistic(self, shardId):
        accounts = self.db_proxy.get(FLAGS.accounts, None, multi = True)
        if accounts:
            addresses = [account["address"] for account in accounts]
            for addr in addresses:
                tx_num = self.db_proxy.count(FLAGS.txs, {"from":addr}, block_height = shardId * FLAGS.table_capacity)
                tx_num = tx_num + self.db_proxy.count(FLAGS.txs, {"to":addr}, block_height = shardId * FLAGS.table_capacity)
                tx_num = tx_num + self.db_proxy.count(FLAGS.txs, {"contractAddress":addr}, block_height = shardId * FLAGS.table_capacity)
                tx_num = tx_num - self.db_proxy.count(FLAGS.txs, {"$and":[{"from":addr}, {"to":addr}]}, block_height = shardId * FLAGS.table_capacity)
                
                mine_num = self.db_proxy.count(FLAGS.blocks, {"miner":addr}, block_height = shardId * FLAGS.table_capacity)
                uncle_num = self.db_proxy.count(FLAGS.uncles, {"miner":addr}, block_height = shardId * FLAGS.table_capacity)

                operation = {
                    "$inc" : {
                        "tx_num" : tx_num,
                        "mine_num" : mine_num,
                        "uncle_num" : uncle_num,
                     }
                }
                self.db_proxy.update(FLAGS.accounts, {"address":addr}, operation, multi = False)

    def _erase_statistic(self):
        accounts = self.db_proxy.get(FLAGS.accounts, None, multi = True)
        if accounts:
            addresses = [account["address"] for account in accounts]
            for addr in addresses:
                operation = {
                    "$set" : {
                        "tx_num" : 0,
                        "mine_num" : 0,
                        "uncle_num" : 0,
                     }
                }
                self.db_proxy.update(FLAGS.accounts, {"address":addr}, operation, multi = False)
                        
