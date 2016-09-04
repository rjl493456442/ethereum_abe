from abe import flags
FLAGS = flags.FLAGS
from abe.db.api import MongodbClient
from abe.decorator import mongo_res_handler


class MongoDBProxy(object):

    def __init__(self, mongo_cli = None):
        if mongo_cli is None:
            self.mongo_cli = MongodbClient()
        else:
            self.mongo_cli = mongo_cli

    @mongo_res_handler
    def get(self, table, cond, multi = False, block_height = None, projection = None, sort_key = None, ascend = True, skip = 0, limit = 0):
        '''
        find document from specific table with `table_id`
        search from collection with name `table` if `table_id` is None
        Args:
            table : table prefix
            cond : query condition
            table_id: table id
            multi : multi documents meet cond
            projection : projection
            sort_key : sort_key
            ascend : order of rank
            skip : skip
            limit : limit
        Returns:
            document(s) satisfies the cond
        '''
        self.mongo_cli.use_db(FLAGS.mongodb_default_db)
        table_name = table
        if isinstance(block_height, int):
            table_id = block_height / FLAGS.table_capacity
            table_name = table_name + str(table_id)

        if multi is False:
            return self.mongo_cli.get_one(table_name, cond)
        else:
            return self.mongo_cli.get_many(table_name, cond, projection, limit, sort_key, ascend, skip)

    @mongo_res_handler
    def search(self, table, cond, multi = False, skip = 0, limit = 0, projection = None, ascend = True, sort_key = None):
        '''
        find document from specific table with `table_id`
        search from collection with name `table` if `table_id` is None
        Args:
            table : table prefix
            cond : query condition
            multi : whether return single document
            projection : projection
            sort_key : sort_key
            ascend : order of rank
            skip : skip
            limit : limit
        Returns:
            document(s) satisfies the cond
        '''
        self.mongo_cli.use_db(FLAGS.mongodb_default_db)
        tables = self.mongo_cli.mc.collection_names()
        table_prefix = table
        nums = [int(t[len(table_prefix):]) for t in tables if t.startswith(table_prefix) and t != table_prefix]
        if multi is False:
            nums.sort(reverse = not ascend)
            ''' only single document is requred '''
            for id in nums:
                res = self.mongo_cli.get_one(table_prefix + str(id), cond)
                if res:
                    return res
            return None

        else:
            ''' return a slice of documents meet cond in db '''
            results = []
            nums.sort(reverse = not ascend)
            if limit:
                start = skip; to = skip + limit; _skip = skip; _to = to
                id1 = id2 = index = cnt = 0
                for index in range(len(nums)):
                    cnt = self.mongo_cli.count(table_prefix + str(nums[index]))
                    if _skip - cnt > 0:
                        _skip = _skip - cnt; id1 = id1 + 1
                    if _to - cnt > 0:
                        _to = _to - cnt; id2 = id2 + 1
                    else:
                        break
                if id1 >= len(nums) or id2 >= len(nums):
                    return 
                if id1 == id2:
                    return self.mongo_cli.get_many(table_prefix + str(nums[id1]), skip = _skip, n = limit, sort_key = sort_key, ascend = ascend)
                else:
                    res = self.mongo_cli.get_many(table_prefix + str(nums[id1]), skip = _skip, sort_key = sort_key, ascend = ascend)
                    results.extend(res)
                    index = id1 + 1
                    while index < id2:
                        res = self.mongo_cli.get_many(table_prefix + str(nums[index]), sort_key = sort_key, ascend = ascend)
                        index = index + 1
                        results.extend(res)
                    res = self.mongo_cli.get_many(table_prefix + str(nums[id2]), sort_key = sort_key, ascend = ascend, n = _to)
                    if res:
                        results.extend(res)
                    return results
            else:
                ''' search for whole db if no limit specified, it can cost a lot of time '''
                for id in nums:
                    res = self.mongo_cli.get_many(table_prefix + str(id), cond, items = projection)
                    if res:
                        results.extend(res)
                
                if sort_key:
                    results = sorted(results, key=lambda item : item[sort_key], reverse = not ascend)

                if skip > 0:
                    results = results[skip:]
                return results

    def update(self, table, cond, operation, upsert = False, multi = False, block_height = None):
        '''
        update document from specific table with table_id
        search from collection with default name table if table_id is None
        Args:
            table: table prefix
            cond: query condition
            table_id: table id
            multi : update multi document together
            upsert: whether insert if not exist
        Returns:
        '''
        self.mongo_cli.use_db(FLAGS.mongodb_default_db)
        table_name = table
        if isinstance(block_height, int):
            table_id = block_height / FLAGS.table_capacity
            table_name = table_name + str(table_id)

        if multi is False:
            return self.mongo_cli.update_one(table_name, cond, operation, upsert)
        else:
            return self.mongo_cli.update_many(table_name, cond, operation, upsert)

    def delete(self, table, cond, multi = False, block_height = None):
        '''
        delete document from specific table with table_id
        search from collection with default name table if table_id is None
        Args:
            table: table prefix
            cond: query condition
            table_id: table id
            multi: update multi document together
        Returns:
        '''
        self.mongo_cli.use_db(FLAGS.mongodb_default_db)
        table_name = table
        if isinstance(block_height, int):
            table_id = block_height / FLAGS.table_capacity
            table_name = table_name + str(table_id)

        if multi is False:
            return self.mongo_cli.delete_one(table_name, cond)
        else:
            return self.mongo_cli.delete_many(table_name, cond)

    def insert(self, table, object, block_height = None, multi = False):
        '''
        insert document from specific table with table_id
        search from collection with default name `table` if table_id is None
        Args:
            table: table prefix
            object: object wait to be inserted
            table_id: table id
        Returns:
            objectId
        '''
        self.mongo_cli.use_db(FLAGS.mongodb_default_db)
        table_name = table
        if isinstance(block_height, int):
            table_id = block_height / FLAGS.table_capacity
            table_name = table_name + str(table_id)
        if multi is False:
            return self.mongo_cli.insert_one(table_name, object)
        else:
            return self.mongo_cli.insert_many(table_name, object)

    def add_index(self, table, indexes, block_height = None):
        '''
        insert index to specific table
        Args:
            table: table prefix
            indexes: index list, each elem is a two-tuples contains the key and sort order
            block_height: use to determine the table slice
        Returns:
        '''
        self.mongo_cli.use_db(FLAGS.mongodb_default_db)
        table_name = table
        if isinstance(block_height, int):
            table_id = block_height / FLAGS.table_capacity
            table_name = table_name + str(table_id)
        self.mongo_cli.add_index(table_name, indexes)

    def drop_db(self, db_name = FLAGS.mongodb_default_db):
        '''
        drop db
        Args:
            db_name: db name
        Returns:
        '''
        self.mongo_cli.conn.drop_database(db_name)

    def get_table_count(self, table_prefix):
        self.mongo_cli.use_db(FLAGS.mongodb_default_db)
        tables = self.mongo_cli.mc.collection_names()
        nums = [int(t[len(table_prefix):]) for t in tables if t.startswith(table_prefix) and t != table_prefix]
        return len(nums)






