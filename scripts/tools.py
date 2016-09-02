from abe import dbproxy
from abe import flags
import time
import pymongo
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

        elif service_name == 'add_miner_table':
            cnt = self.db_proxy.get_table_count(FLAGS.accounts)
            for idx in range(cnt):
                time_start = time.time()
                res = self.db_proxy.get(FLAGS.accounts+str(idx), None, multi = True)
                if res:
                    for acct in res:
                        if acct and (acct.has_key("mine") or acct.has_key('uncles')):
                            self.db_proxy.update(FLAGS.miner, {"address": acct['address']}, {"$set":{"address": acct['address']}}, upsert = True)
                print "process %d accounts finish, elapsed %f" % (len(res), time.time() - time_start)

        elif service_name == 'add_contract_table':
            cnt = self.db_proxy.get_table_count(FLAGS.accounts)
            for idx in range(cnt):
                time_start = time.time()
                res = self.db_proxy.get(FLAGS.accounts+str(idx), None, multi = True)
                if res:
                    for acct in res:
                        if acct and acct.has_key("is_contract"):
                            self.db_proxy.update(FLAGS.contract, {"address": acct['address']}, {"$set":{"address": acct['address']}}, upsert = True)
                print "process %d accounts finish, elapsed %f" % (len(res), time.time() - time_start)

        elif service_name == 'merge_account_table':
            self.db_proxy.add_index(FLAGS.accounts, [("address", pymongo.ASCENDING)])
            cnt = self.db_proxy.get_table_count(FLAGS.accounts)
            for idx in range(cnt):
                time_start = time.time()
                res = self.db_proxy.get(FLAGS.accounts+str(idx), None, multi = True)
                if res:
                    for acct in res:
                        if acct.has_key('balance'):
                            self.db_proxy.update(FLAGS.accounts, {"address": acct['address']}, {"$set": {"balance": acct['balance']}}, upsert = True)
                        else:
                            self.db_proxy.update(FLAGS.accounts, {"address": acct['address']}, {"$set": {"address": acct['address']}}, upsert = True)

                print "process %d accounts finish, elapsed %f" % (len(res), time.time() - time_start)