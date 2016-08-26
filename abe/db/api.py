from pymongo import MongoClient
import pymongo
from abe import flags
FLAGS = flags.FLAGS


class MongodbClient(object):
    def __init__(self):
        if FLAGS.mongodb_remote_db:
            # connect to remote db with username and pwd
            url = "mongodb://%s:%s@%s:%s/%s" % (FLAGS.mongodb_user, FLAGS.mongodb_pwd, 
                    FLAGS.mongodb_remote_host, FLAGS.mongodb_remote_port, FLAGS.mongodb_default_db)
            self.conn = MongoClient(url, connect = False)
        else:
            self.conn = MongoClient(FLAGS.mongodb_host, FLAGS.mongodb_port, maxPoolSize=300, connect = False)        


    def use_db(self, db_name, mongo_user=None, mongodb_password=None):
        db = self.conn[db_name]
        self._auth(db, mongo_user, mongodb_password)
        self.mc = db
    
    def _auth(self, db, mongo_user, mongodb_password):
        if not mongo_user and not mongodb_password:
            res = db.authenticate(FLAGS.mongodb_user, FLAGS.mongodb_password)
        else:
            res = db.authenticate(mongo_user, mongodb_password)
        if not res:
            raise Exception("Mongodb Authentication Fail")


    def get_one(self, table, cond):
        res = self.mc[table].find_one(cond)
        return res if res else None

    def get_many(self, table, cond={}, items=None, n=0, sort_key=None, ascend=True, skip=0):
        collection = self.mc[table].find(cond, items) if items else self.mc[table].find(cond)
        if sort_key:            
            n = FLAGS.table_capacity if not n else n
            res = collection.limit(n).skip(skip).sort([(sort_key,1 if ascend else -1)])
            return list(res) if res else None
        else:
            ''' slice without sort make no sense '''
            res = collection.limit(n).skip(skip)
            return list(res) if res else None

    def update_one(self, table, cond, operation, upsert):
        self.mc[table].update_one(cond, operation, upsert)

    def update_many(self, table, cond, operation, upsert):
        self.mc[table].update_many(cond, operation, upsert)

    def insert(self, table, value):
        self.mc[table].insert(value)

    def delete_one(self, table, cond):
        self.mc[table].delete_one(cond)

    def delete_many(self, table, cond):
        self.mc[table].delete_many(cond)

    def count(self, table):
        res = self.mc[table].find().count()
        return int(res) if res else 0

    def add_index(self, table, indexs):
        self.mc[table].create_index(indexs)





        
    