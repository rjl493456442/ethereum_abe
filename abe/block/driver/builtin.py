from abe.db import base
from abe import logger
from abe import constant
from pyethapp.rpc_client import JSONRPCClient
from abe import flags
from abe.decorator import signal_watcher
from handler import BlockHandler
from multiprocessing import Process, Pool, Pipe, Event, JoinableQueue, Queue
import multiprocessing
import time
import sys
import gevent
from gevent import monkey
monkey.patch_all()
from gevent import pool, queue, event
FLAGS = flags.FLAGS
from abe import utils
import json


class BuiltinDriver(base.Base):

    @property
    def type(self):
        return 'block_driver'

    def __init__(self):
        self.logger = logger
        self.rpc_cli = JSONRPCClient(host = FLAGS.rpc_host, port = FLAGS.rpc_port, print_communication = False)
        super(BuiltinDriver, self).__init__()


    ''' api '''    
    def synchronize(self):
        self.initialize()
        # get block parallelly
        self.run(self.db_block_number+1, self.net_block_number, sync_balance = True)
        # get block in sequence
        self.start_loop()

    def synchronize(self, begin, end, sync_balance):
        self.run(begin, end-1, sync_balance)

    def check(self, begin, end, sync_balance):
        try:
            blocks = self.db_proxy.search(FLAGS.blocks, None, projection = {"number":1},
                    multi = True, skip = begin ,limit = end-begin, ascend = True, sort_key = "number")
            
            if blocks is None:
                self.logger.info("no blocks in specific range")
            else:
                numbers = [item['number'] for item in blocks]
                miss = []
                for i in range(begin, end):
                    if i not in numbers:
                        miss.append(i)
                        
                handler = BlockHandler(self.rpc_cli, self.logger, self.db_proxy, sync_balance)

                self.logger.info("totaly %d blocks missing" % len(miss))

                while len(miss) > 0:
                    if not handler.execute(miss[0], fork_check = False):
                        miss.append(miss[0])
                    miss.pop(0)
                return True
                        
        except Exception, e:
            self.logger.info(e)


    ''' internal method '''   

    @property
    def db_block_number(self):
        return self._db_block_number

    @property
    def net_block_number(self):
        return self._net_block_number

    def get_status(self):
        try:
            self._net_block_number = utils.convert_to_int(self.rpc_cli.call(constant.METHOD_BLOCK_NUMBER))
            res = self.db_proxy.search(FLAGS.blocks, None, multi = True, limit = 1, ascend = False, sort_key = "number") 
            if not res:
                self._db_block_number = 0
            else:
                self._db_block_number = res[0]['number']
            return True
        except:
            return False

    def initialize(self):
        self.get_status()
        self.get_miss()
        self.fork_check_last_block()

    def get_miss(self):
        try:
            uncheck_blocks = self.db_proxy.search(FLAGS.blocks, None, projection = {"number":1},
                multi = True, limit = 10 * FLAGS.greenlet_num, ascend = False, sort_key = "number")
            if uncheck_blocks is None:
                return
        except AttributeError, e:
            return 

        numbers = [item['number'] for item in uncheck_blocks]
        min = uncheck_blocks[-1]['number']
        max = uncheck_blocks[0]['number']
        miss = []
        for i in range(min, max):
            if i not in numbers:
                miss.append(i)
                
        handler = BlockHandler(self.rpc_cli, self.logger, self.db_proxy)

        self.logger.info("totaly %d blocks missing" % len(miss))

        while len(miss) > 0:
            if not handler.execute(miss[0], fork_check = False):
                miss.append(miss[0])
            miss.pop(0)
        return True

    def fork_check_last_block(self):
        if self.db_block_number == 0:return 
        block_handler = BlockHandler(self.rpc_cli, self.logger, self.db_proxy)
        block_handler.execute(self.db_block_number, fork_check = True)

    

    def pending_blocks(self):
        if self.net_block_number is not None and self.db_block_number is not None:
            if isinstance(self.net_block_number, int) and isinstance(self.db_block_number, int):
                return self.net_block_number - self.db_block_number
        return None

        
    def start_loop(self):
        while True:
            self.get_status()
            # sequence
            block_handler = BlockHandler(self.rpc_cli, self.logger, self.db_proxy, sync_balance = True)
            for block_num in range(self.db_block_number+1, self.net_block_number+1):
                block_handler.execute(block_num, fork_check = True)


    def run(self, begin, end, sync_balance):
        time_start = time.time()
        self.logger.info("synchronize start, from %d to %d" % (begin, end))
        synchronizor = Synchronizor(begin, end, self.rpc_cli, self.logger)
        synchronizor.run()
        self.logger.info("synchronize start, from %d to %d finished, totally elapsed %f" % (begin, end, time.time() - time_start))
        
        if sync_balance:
            time_start = time.time()
            self.logger.info("synchronize balance begin")
            block_handler = BlockHandler(self.rpc_cli, self.logger, self.db_proxy)
            block_handler._sync_balance(end)
            self.logger.info("synchronize balance finished, totally elapsed %f" % (time.time() - time_start))


class Synchronizor(base.Base):
    def __init__(self, begin, end, rpc_cli, logger):
        self.pool = pool.Pool(FLAGS.greenlet_num)
        self.jobq = queue.Queue()
        self.worker_finished = event.Event()
        self.rpc_cli = rpc_cli
        self.logger = logger
        self.begin = begin
        self.end = end
        self.count = 0
        self.start = time.time()
        super(Synchronizor, self).__init__()

    def run(self):
        self.initialize_job(self.begin, self.end)
        self.scheduler_greenlet = gevent.spawn(self.scheduler)
        self.scheduler_greenlet.join()

    def initialize_job(self, down_limit, up_limit):
        for i in range(down_limit, up_limit + 1):
            self.jobq.put(i)

    def scheduler(self):
        while True:
            # join dead greenlets
            for greenlet in list(self.pool):
                if greenlet.dead:
                    self.pool.discard(greenlet)
            try:
                id = self.jobq.get_nowait()
            except queue.Empty:
                logger.info("No jobs remaining.")
                if self.pool.free_count() != self.pool.size:
                    logger.info("%d workers remaining, waiting..." % (self.pool.size - self.pool.free_count()))
                    self.worker_finished.wait()
                    self.worker_finished.clear()
                    continue
                else:
                    logger.info("No workers left, shutting down.")
                    return self.shutdown()
            self.pool.spawn(self.worker, id)

    def worker(self, number):
        if (number % FLAGS.table_capacity) == 0:
            self.add_indexes(number)

        handler = BlockHandler(self.rpc_cli, self.logger, self.db_proxy)

        while True:
            # failed, process this block again
            if handler.execute(number, fork_check = False):
                break
            else:
                time.sleep(FLAGS.poll_interval)
            
        self.count = self.count + 1
        if (self.count %  1000 == 0):
            self.logger.info("1000 blocks elapsed : %f" % (time.time() - self.start))
            self.start = time.time()

        self.worker_finished.set()


    def shutdown(self):
        """Shutdown the crawler after the pool has finished."""
        self.pool.join()
        return True






