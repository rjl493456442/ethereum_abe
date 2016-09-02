from abe import dbproxy
from abe import flags
import time
import pymongo
FLAGS = flags.FLAGS
class Tool(object):
    def __init__(self):
        self.db_proxy = dbproxy.MongoDBProxy()

    def service(self, service_name):
        pass
                        
