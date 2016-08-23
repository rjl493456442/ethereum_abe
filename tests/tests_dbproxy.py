#! /usr/bin/env python
# -*- coding: utf-8 -*-
from os import  sys, path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import unittest
import random
from abe import flags
from abe import dbproxy
FLAGS = flags.FLAGS
import time

class DbproxyTest(unittest.TestCase):
    """
    etheruem tests case
    """
    def setUp(self):
        self.dbproxy = dbproxy.MongoDBProxy()
        self.origin = FLAGS.table_capacity
        FLAGS.table_capacity = 100
        for i in range(250):
            self.dbproxy.insert("test", {"id":i}, block_height = i)

    def test_get(self):
        # multi is False
        test_cases = [0, 99, 101, 199, 249]
        for id in test_cases:
            res = self.dbproxy.get("test", {"id":id}, block_height = id)
            self.assertEqual(res['id'], id, msg = u'值不相等')
            self.assertNotIn("_id", res)

        # multi is True
        for id in test_cases:
            results = self.dbproxy.get("test", {"id":id}, block_height = id, multi = True)
            self.assertEqual(results[0]['id'], id, msg = u'值不相等')
            self.assertNotIn("_id", res)

    def test_search(self):
        test_cases = [
            # ascend start to firstelem lastelem
            (True, 15, 99, 15, 99),
            (True, 15, 105, 15, 105),
            (True, 0, 250, 0, 250),
            (True, 105, 205, 105, 205),
            (False, 0, 100, 249, 151),
            (False, 0, 10, 249, 241),
            (False, 105, 150, 144, 101)
        ]

        # multi is True
        for case in test_cases:
            ascend = case[0]
            start = case[1]
            to = case[2]
            length = to - start
            res = self.dbproxy.search("test", None, multi = True, skip = start, limit = length , sort_key = "id", ascend = ascend)
            self.assertEqual(len(res), length)
            self.assertEqual(res[0]['id'], case[3])
            self.assertEqual(res[-1]['id'], case[4]-1)

    def test_update(self):
        # multi is False
        self.dbproxy.update("test", {"id":1}, {"$set":{"msg" : "world"}}, block_height = 1, multi = False)
        res = self.dbproxy.get("test", {"id":1}, block_height = 1, multi = False)
        self.assertEqual(res["msg"], "world")

        #multi is True
        self.dbproxy.insert("test", {"id":205, "msg":"hello", "info":"a"},  block_height = 205)
        self.dbproxy.insert("test", {"id":206, "msg":"hello", "info":"b"}, block_height = 206)
        self.dbproxy.update("test", {"msg":"hello"}, {"$set":{"info" : "c"}}, block_height = 205, multi = True)
        res = self.dbproxy.get("test", {"msg":"hello"}, block_height = 205, multi = True)
        for item in res:
            self.assertEqual(item['info'], 'c')

    def test_delete(self):
        self.dbproxy.delete("test", {"id":5}, block_height = 5)
        res = self.dbproxy.get("test", {"id":5}, block_height = 5, multi = False)
        self.assertEqual(res, None)

    def tearDown(self):
        for id in range(3):
            self.dbproxy.mongo_cli.mc.drop_collection("test"+str(id))

