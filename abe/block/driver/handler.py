from abe.db import base
from abe import logger
from abe import constant
from pyethapp.rpc_client import JSONRPCClient
from abe import flags
import time
from abe import utils
import json
import pymongo
FLAGS = flags.FLAGS

class BlockHandler(object):
    def __init__(self, rpc_cli, logger, db_proxy, sync_balance = False, sync_it = False):
        self.rpc_cli = rpc_cli
        self.logger = logger
        self.db_proxy = db_proxy
        self.sync_balance = sync_balance
        super(BlockHandler, self).__init__()

    def execute(self, blockinfo, fork_check):
        if isinstance(blockinfo, int):
            if blockinfo == 0:
                self.process_genesis()
                return True

            method_name = constant.METHOD_GET_BLOCK_BY_NUMBER

        elif isinstance(blockinfo, str) or isinstance(blockinfo, unicode):
            method_name = constant.METHOD_GET_BLOCK_BY_HASH

        try:
            blk = self.rpc_cli.call(method_name, blockinfo, True)

            if blk:
                self.blk_number = utils.convert_to_int(blk['number'])
                self.add_indexes(self.blk_number)

                if fork_check:
                    self.maintain_chain(blk)
                    res = self.db_proxy.get(FLAGS.blocks, {"number":self.blk_number}, block_height = self.blk_number)
                    if res and res["hash"] != blk["hash"]:
                        '''
                        chain head change
                        (1) different txs
                        (2) undo txs in old block but not in new one
                        (3) apply txs in new block but not in old one
                        (4) replace the origin 
                        '''
                        success = self.process_fork(res, blk)
                        return success
                    else:
                        success = self.process_block(blk)
                        return success
                else:
                    # no necessary to check whether is a fork block
                    success = self.process_block(blk)
                    return success
            else:
                self.logger.info("block %s not exist in blockchain, discard" % blk["number"])
                return False
        except Exception, e:
            self.logger.info("request block failed, errormsg %s" % e)
            return False
    
    def maintain_chain(self, block):
        res = self.db_proxy.search(FLAGS.blocks, {"number": utils.convert_to_int(block['number']) - 1}, multi = False) 
        if res:
            chain_head = res
            current_block = block

            while True:
                if current_block['parentHash'] != chain_head['hash']:
                    try:
                        new_block = self.rpc_cli.call(constant.METHOD_GET_BLOCK_BY_HASH, current_block['parentHash'], True)
                        old_block = chain_head
                        self.logger.info("reorg chain, old height %s, new height %s, old hash:%s, new hash:%s" % (old_block['number'], new_block['number'], old_block['hash'], new_block['hash']))
                        if new_block and old_block:
                            success = self.process_fork(old_block, new_block)
                            if not success: continue
                        current_block = new_block
                        res = self.db_proxy.search(FLAGS.blocks, {"number":current_block["number"]-1}, multi = False) 
                        if res: chain_head = res
                        else:
                            self.logger.error("maintain_chain failed, not found %s height block in db" % chain_head['number'])
                    except Exception, e: 
                        self.logger.error("maintain_chain failed , errormsg %s" % e)
                        continue
                else: break

    def process_genesis(self):
        ''' add genesis block from file to db '''
        filename = FLAGS.genesis_data
        with open(filename, "r") as f:
            data = json.load(f)
            data['transactions'] = []
            for to in data['alloc'].keys():
                value = data['alloc'][to]['balance']
                value = hex(int(value))
                if value.endswith('L'): value = value[:-1]
                data['transactions'].append({
                    "hash" : "GENESIS_" + to,
                    "from" : "GENESIS",
                    "to" : '0x'+ to,
                    "value" : value,
                })

            del data['alloc']
        
        block = data
        txs = block['transactions']
        timestamp = block['timestamp']
        
        for tx in txs:
            tx['blockNumber'] = "0x0"
            tx['timestamp'] = timestamp
            operation = {
                "$set" : {"address" : tx["to"]},
            }
            self.db_proxy.update(FLAGS.accounts, {"address":tx["to"]}, operation, upsert = True, multi = False)
            self.db_proxy.update(FLAGS.txs, {"hash":tx["hash"]}, {"$set":tx}, upsert = True, multi = False, block_height = 0)
        block['txs_num'] = len(block['transactions'])
            
        del block['transactions']
        self.db_proxy.update(FLAGS.blocks, {"number":0}, {"$set":block}, block_height = 0, upsert = True)
        return True

    def process_block(self, block):
        # get tx related accounts
        try:
            accounts = []

            txs = block['transactions']
            timestamp = block['timestamp']      
            # process tx 
            total_fee = 0
            for tx in txs:
                res, accts = self.process_tx(tx, timestamp)
                if res:
                    total_fee = total_fee + res
                accounts.extend(accts)

            block['number'] = utils.convert_to_int(block['number'])

            block['txs_num'] = len(block['transactions'])
            del block['transactions']

            # update miner account
            basic_reward = FLAGS.default_reward + len(block['uncles']) * FLAGS.default_reward * 1.0 / 32
            basic_reward = utils.unit_convert_from_ether(basic_reward)
            block['reward'] = total_fee + basic_reward
        
            operation = {
                "$set" : {"address" : block["miner"], "miner":1},
            }

            self.db_proxy.update(FLAGS.accounts, {"address":block["miner"]}, operation, upsert = True)
            # process uncle
            uncle_miners = self.process_uncle(block)
            accounts.extend(uncle_miners)
            accounts.append(block['miner'])

            if self.sync_balance: self.set_balance(accounts, self.blk_number)
            # insert block
            self.db_proxy.update(FLAGS.blocks, {"number":block['number']}, {"$set":block}, block_height = self.blk_number, upsert = True)
            return True

        except Exception, e:
            self.logger.info("process block failed, errormsg: %s" % e)
            return False

    def process_fork(self, old_block, new_block):
        '''
        chain head change
        (1) different txs
        (2) undo txs in old block but not in new one
        (3) apply txs in new block but not in old one
        (4) mark the origin
        '''
        try:
            assert utils.convert_to_int(new_block['number']) == old_block['number']
            self.logger.info("chain head change at height %d, origin hash :%s, new block hash :%s" % (old_block["number"], old_block['hash'], new_block['hash']))
            accounts = old_hashes = new_hashes = []
            old_txs = self.db_proxy.get(FLAGS.txs, {"blockNumber" : hex(old_block['number'])}, block_height = self.blk_number, multi = True)

            old_hashes = [tx['hash'] for tx in old_txs]
            new_hashes = [tx['hash'] for tx in new_block['transactions']]    

            # undo_txs
            undo_txs = []
            undo_fee = 0
            for _,thash in enumerate(old_hashes):
                if thash not in new_hashes:
                    undo_txs.append(old_txs[_])
                    undo_fee = undo_fee + old_txs[_]['fee']

            # apply_txs
            apply_txs = []
            for _,tx in enumerate(new_hashes):
                if tx not in old_hashes:
                    apply_txs.append(new_block['transactions'][_])

            undo_txs_hash = [tx['hash'] for tx in undo_txs]
            apply_txs_hash = [tx['hash'] for tx in apply_txs]
            self.logger.info("process fork, undo txs: %s, apply txs:%s" % (undo_txs_hash, apply_txs_hash))
            # revert
            accts = self.revert(old_block, undo_txs)
            accounts.extend(accts)

            revert_uncle_miners = self.revert_uncle(old_block)
            accounts.extend(revert_uncle_miners)

            # process new block
            apply_fee = 0
            for tx in apply_txs:
                fee, accts = self.process_tx(tx, new_block['timestamp'])
                apply_fee = apply_fee + fee
                accounts.extend(accts)

            # save new block info
            new_block["number"] = utils.convert_to_int(new_block['number'])
            diff_uncle_reward = (len(new_block['uncles']) - len(old_block['uncles'])) * FLAGS.default_reward * 1.0 / 32
            diff_uncle_reward = utils.unit_convert_from_ether(diff_uncle_reward)
            new_block['reward'] = old_block['reward'] - undo_fee + apply_fee + diff_uncle_reward
            new_block['txs_num'] = len(new_block['transactions'])
            del new_block['transactions']

            # update miner account
            operation = {
                "$set" : {"address" : new_block["miner"], "miner":1},
            }
            self.db_proxy.update(FLAGS.accounts, {"address":new_block["miner"]}, operation, upsert = True)
            
            uncle_miners =  self.process_uncle(new_block)
            accounts.extend(uncle_miners)

            accounts.append(new_block["miner"])
            accounts.append(old_block["miner"])
            # set balance
            if self.sync_balance : self.set_balance(accounts, self.blk_number)
            
            self.db_proxy.update(FLAGS.blocks, {"hash": new_block['hash']}, {"$set":new_block}, block_height = self.blk_number, upsert = True)
            return True
        except Exception, e:
            self.logger.info("process fork block failed, errormsg: %s" % e)
            return False

    def process_uncle(self, block):
        miners = []
        if isinstance(block['number'], str) or isinstance(block['number'], unicode):
            current_height = utils.convert_to_int(block['number'])
            block['number'] = current_height
        else:
            current_height = block['number']
        
        for _, uncle in enumerate(block['uncles']):
            buncle = self.rpc_cli.call(constant.METHOD_GET_UNCLE_BY_BLOCK_HASH_AND_INDEX, block["hash"], _)
            if buncle:
                miners.append(buncle["miner"])
                uncle_reward = (utils.convert_to_int(buncle['number']) - current_height + 8) * 1.0 * FLAGS.default_reward / 8
                uncle_reward = utils.unit_convert_from_ether(uncle_reward)
                
                operation = {
                    "$set" : {"address" : buncle["miner"], "miner":1},
                }
                self.db_proxy.update(FLAGS.accounts, {"address":buncle["miner"]}, operation, upsert = True)

                buncle['mainNumber'] = current_height
                buncle['reward'] = uncle_reward
                self.db_proxy.update(FLAGS.uncles, {"hash":buncle["hash"]}, {"$set":buncle}, block_height = current_height, upsert = True)
        return miners

    def process_tx(self, tx, timestamp):
        # merge tx receipt data
        accounts = []
        while True:
            receipt = self.rpc_cli.call(constant.METHOD_GET_TX_RECEIPT, tx['hash'])
            if receipt: break
        receipt_useful_field = ['cumulativeGasUsed', 'contractAddress', 'gasUsed', 'logs']
        for k, v in receipt.items():
            if k in receipt_useful_field:
                tx[k] = v
        tx['timestamp'] = timestamp
        
        # apply tx
        try:
            operation = {
                "$set" : {"address" : tx["from"]},
            }
            self.db_proxy.update(FLAGS.accounts, {"address":tx["from"]}, operation, upsert = True)
            accounts.append(tx['from'])
            
            if tx['contractAddress']:
                # contract creation transaction
                code = self.rpc_cli.call(constant.METHOD_GET_CODE, tx['contractAddress'], "latest")
                operation = {
                    "$set" : {"address" : tx["contractAddress"], "contract" : 1, "code" : code},
                }
                # update contract table
                self.db_proxy.update(FLAGS.accounts, {"address":tx["contractAddress"]}, operation, upsert = True)
                accounts.append(tx['contractAddress'])
            else:
                operation = {
                    "$set" : {"address" : tx["to"]},
                }
                self.db_proxy.update(FLAGS.accounts, {"address":tx["to"]}, operation, upsert = True)
                accounts.append(tx['to'])
            # always insert the tx after this tx has been process
            
            gas_cost = utils.convert_to_int(tx['gasUsed']) * utils.convert_to_int(tx["gasPrice"])
            tx['fee'] = utils.unit_convert(gas_cost)
            self.db_proxy.update(FLAGS.txs, {"hash" : tx["hash"]}, {"$set":tx}, block_height = self.blk_number, upsert = True)
            return tx['fee'], accounts

        except Exception, e:
            self.logger.error(e)
            return 0, []

    def revert(self, block, tx_list):
        # delete origin block
        self.db_proxy.delete(FLAGS.blocks, {"hash":block["hash"]}, block_height = self.blk_number, multi = False)
        # revert txs
        accounts = []
        for tx in tx_list:
            res, accts = self.revert_tx(tx, self.blk_number)
            accounts.extend(accts)
        return accounts

    def revert_uncle(self, block):
        miners = []
        current_height = block['number']
        for _, uncle in enumerate(block['uncles']):
            buncle = self.db_proxy.get(FLAGS.uncles, {"mainNumber":current_height, "hash": uncle}, block_height = current_height, multi = False)
            if buncle:
                miners.append(buncle["miner"])
                self.db_proxy.delete(FLAGS.uncles, {"mainNumber":current_height, "hash":uncle}, block_height = current_height)
        return miners
    
    def revert_tx(self, tx, block_height):
        try:  
            accounts = []      
            if tx['contractAddress']:
                # contract creation transaction
                self.db_proxy.delete(FLAGS.accounts, {"address": tx["contractAddress"]}, block_height = block_height)
                accounts = [tx['from']]
            else:
                accounts = [tx['from'], tx['to']]
            self.db_proxy.delete(FLAGS.txs, {"hash" : tx["hash"]}, block_height = block_height)
            return True, accounts
        except Exception, e:
            self.logger.error(e)
            return False, []

    def _sync_balance(self, net_last_block):
        accounts = self.db_proxy.get(FLAGS.accounts, None, multi = True, projection = {"address":1})
        accounts = [acct["address"] for acct in accounts]
        self.set_balance(accounts, net_last_block)

    def set_balance(self, accounts, block_number):
        if type(accounts) is list: accounts = list(set(accounts))
        else: accounts = list(accounts)       
        while len(accounts) > 0:
            try:
                balance = self.rpc_cli.call(constant.METHOD_GET_BALANCE, accounts[0], block_number)
                if balance:    
                    operation = {
                        "$set" : {"balance" : balance, "address":accounts[0]}
                    }
                    self.db_proxy.update(FLAGS.accounts, {"address": accounts[0]}, operation, upsert = True)
                else:
                    self.logger.info("request %s balance failed!" % accounts[0])
                accounts.pop(0)
            except Exception, e:
                self.logger.info(e)
                accounts.append(accounts[0])
        
        self.db_proxy.update(FLAGS.meta, {"sync_record":"ethereum"}, {"$set": {"account_status_flag":block_number} }, multi = False, upsert = True)
         
    def add_indexes(self, block_height):
        if block_height % FLAGS.table_capacity == 0:

            blocks_indexs = [
            [("number", pymongo.ASCENDING)],
            [("hash", pymongo.ASCENDING)],
            [("miner", pymongo.ASCENDING)],
            ]
            for index in blocks_indexs:
                self.db_proxy.add_index(FLAGS.blocks, index, block_height = block_height)

            tx_indexes = [
                [("blockNumber", pymongo.ASCENDING)],
                [("hash", pymongo.ASCENDING)],
                [("from", pymongo.ASCENDING)],
                [("to", pymongo.ASCENDING)],

            ]
            for index in tx_indexes:
                self.db_proxy.add_index(FLAGS.txs, index, block_height = block_height)

            accounts_indexs = [
                [("address", pymongo.ASCENDING)],
            ]
            for index in accounts_indexs:
                self.db_proxy.add_index(FLAGS.accounts, index)

            uncles_indexs = [
                [("mainNumber", pymongo.ASCENDING),("hash", pymongo.ASCENDING)],
                [("hash", pymongo.ASCENDING)],
                [("miner", pymongo.ASCENDING)],
            ]
            for index in uncles_indexs:
            self.db_proxy.add_index(FLAGS.uncles, index, block_height = block_height)