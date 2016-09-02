from abe import dbproxy
from abe import flags
import time
import pymongo
FLAGS = flags.FLAGS
class Tool(object):
    def __init__(self):
        self.db_proxy = dbproxy.MongoDBProxy()

    def service(self, service_name):
        if service_name == 'add_account_info':        
            res = self.db_proxy.get(FLAGS.miner, None, multi = True)
            if res:
                for acct in res:
                    self.db_proxy.update(FLAGS.accounts, {"address": acct['address']}, {"$set": {"miner": 1}})

            res = self.db_proxy.get(FLAGS.contract, None, multi = True)
            if res:
                for acct in res:
                    self.db_proxy.update(FLAGS.accounts, {"address": acct['address']}, {"$set": {"contract": 1}})
                        
