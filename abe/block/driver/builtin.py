from abe.db import base
from abe import logger
from abe import constant
from pyethapp.rpc_client import JSONRPCClient
from abe import flags
from abe.decorator import signal_watcher
from handler import BlockHandler
from log_handler import LogHandler
from multiprocessing import Process, Pool, Pipe, Event, JoinableQueue, Queue
import multiprocessing
import time
import sys
import os
import glob
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
    def synchronize_forever(self):
        '''
        synchronize block forever: 
            step1, set listener to gather all pending block hashes;
            step2, sync blocks parallelly in range[last sync block+1, last mined block in network]
            step3, sync account balance in range[last sync block+1, last mined block in network]
            step4, start loop to handle the pending block gathered by listener(set balance together)
        '''
        self.initialize()

        # set listener
        self.logger.info("set Listener")
        self.listen()

        # get block parallelly
        self.run(self.db_block_number+1, self.net_block_number, sync_balance = True)
        
        # get block in sequence
        self.start_loop()

    def synchronize(self, begin, end, sync_balance):
        '''
        synchronize block in range[begin, end):
        '''
        self.run(begin, end-1, sync_balance = sync_balance)

    def check(self, shardId):
        '''
        check miss block in shardId slice, get it back if miss
        Args:
            shardId: int, slice id
            sync_balance: bool, flag specify whether sync balance for the missing block
        '''
        try:
            blocks = self.db_proxy.get(FLAGS.blocks, None, projection = {"number":1},
                    multi = True, block_height = shardId * FLAGS.table_capacity)
            
            if blocks is None:
                self.logger.info("no blocks in specific range")
            else:
                numbers = [item['number'] for item in blocks]
                min = shardId * FLAGS.table_capacity
                max = (shardId+1) * FLAGS.table_capacity
                miss = []
                for i in range(min, max):
                    if i not in numbers:
                        miss.append(i)
                        
                handler = BlockHandler(self.rpc_cli, self.logger, self.db_proxy)

                self.logger.info("totally %d blocks missing" % len(miss))

                time_start = time.time()
                while len(miss) > 0:
                    if not handler.execute(miss[0], fork_check = False):
                        miss.append(miss[0])
                    miss.pop(0)
                self.logger.info("retrieve blocks finish, totally elapsed:%f" % (time.time()-time_start))
                return True
                        
        except Exception, e:
            self.logger.info(e)

    def sync_it(self, log_path, shardId):
        '''
        sync internal transactions from log filter_id
        Args:
            log_path: string, specify the log_directory log_path
            shardId:  slice id, if shardId equal -1, means process all logs in directory; else, process specific log only
        '''
        if shardId == -1:
            # process all log in directory
            if os.path.isdir(log_path):
                time_start = time.time()
                log_files = glob.glob(log_path + "/tx.log*")
                handler = LogHandler(self.db_proxy)
                for log_file in log_files: handler.run(log_file)
                self.logger.info("process log %s finish, totally elapsed:%f" % (log_files ,time.time()-time_start))
                return
        else:
            # process specified log
            filename = log_path+"/tx.log"+str(shardId)
            if os.path.isfile(filename):
                time_start = time.time()
                handler = LogHandler(self.db_proxy)
                handler.run(filename)
                self.logger.info("process log %s finish, totally elapsed:%f" % (filename, time.time()-time_start))
                return

        self.logger.info("params not valid")

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
        '''
        (1) init share_queue
        (2) get last sync block number and last mined block number in network
        (3) get miss blocks since last synchronize
        (4) check last sync block whether is been reorg
        '''
        self.share_queue = Queue()
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

        self.logger.info("totally %d blocks missing" % len(miss))

        while len(miss) > 0:
            if not handler.execute(miss[0], fork_check = False):
                miss.append(miss[0])
            miss.pop(0)
        return True

    def fork_check_last_block(self):
        if self.db_block_number == 0:return 
        block_handler = BlockHandler(self.rpc_cli, self.logger, self.db_proxy)
        block_handler.execute(self.db_block_number, fork_check = True)

    def listen(self):
        self.listener = Process(target = self.listener_proc, args = (self.share_queue,))
        self.listener.daemon = True
        self.listener.start()

    @signal_watcher
    def listener_proc(self, queue):
        filter_id = self.rpc_cli.call(constant.METHOD_NEW_BLOCK_FILTER)
        while True:
            try:
                res = self.rpc_cli.call(constant.METHOD_GET_FILTER_CHANGES, filter_id)
                if res:
                    # new blocks array
                    self.logger.info("new blocks arrive: %s" % res)
                    for block in res:
                        queue.put(block)
                else:
                    time.sleep(FLAGS.poll_interval)
            except:
                # connect establish may be failed, just ignore it
                time.sleep(FLAGS.poll_interval)
        
    def start_loop(self):
        logger.info("begin loop handle")
        block_handler = BlockHandler(self.rpc_cli, self.logger, self.db_proxy, sync_balance = True)
        while True:
            block_hash = self.share_queue.get()
            # need fork-check
            while True:
                if block_handler.execute(block_hash, fork_check = True): break

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






