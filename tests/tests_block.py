from abe.block.driver import handler
import unittest
from abe import flags
from pyethapp.rpc_client import JSONRPCClient
from abe import logger
from abe.db import api
from abe import utils
from abe import dbproxy
from abe.block.driver import builtin
FLAGS = flags.FLAGS

class BlockTest(unittest.TestCase):
    
    def setUp(self):
        self.db_proxy = dbproxy.MongoDBProxy()
        self.origin_db = FLAGS.mongodb_default_db
        FLAGS.mongodb_default_db = "test_db"
        self.rpc_cli = JSONRPCClient(host = FLAGS.rpc_host, port = FLAGS.rpc_port, print_communication = False)
        self.handler = handler.BlockHandler(self.rpc_cli, logger, self.db_proxy)
        
    
    def test_fork(self):
        # old
        self.handler.blk_number = 2074903
        old_block = self.rpc_cli.call("eth_getBlockByNumber", 2074903, True)
        self.handler.process_block(old_block, False)

        # new
        new_block = self.rpc_cli.call("eth_getBlockByNumber", 2074903, True)
        new_block['uncles'] = []
        tx = self.rpc_cli.call("eth_getTransactionByHash", "0xa2d7bdf90e507979d7005399f2af77918a538d5288076b0e2a1308e7a419f1bc")
        new_block['hash'] = "0x123"
        new_block['transactions'].pop(0)
        new_block['transactions'].append(tx)
        self.handler.process_fork(old_block, new_block)
        
        acc = self.db_proxy.get(FLAGS.accounts, {"address":"0x2a65aca4d5fc5b5c859090a6c34d164135398226"}, block_height = self.handler.blk_number)
        self.assertEqual(acc['balance'], utils.unit_convert_from_ether(5.001039206))
        
        block = self.db_proxy.get(FLAGS.blocks, {"number":2074903}, block_height = self.handler.blk_number)
        self.assertEqual(block['hash'], "0x123")

        buncle1 = self.db_proxy.get(FLAGS.uncles, {"mainNumber":2074903, "hash":"0xe83ede60f9ee37d506101d542578d7a26236829364a36652c0bd0d9e6652a0db"}, 
            block_height = self.handler.blk_number)
        self.assertEqual(buncle1, None)


    def test_process_block(self):
        self.handler.blk_number = 2074903
        block = self.rpc_cli.call("eth_getBlockByNumber", 2074903, True)
        self.handler.process_block(block, False)
        # block
        blk = self.db_proxy.get(FLAGS.blocks, {"number":self.handler.blk_number}, block_height = self.handler.blk_number)
        self.assertEqual(blk['hash'], '0xcb3d7de2ed7817fb5c5763c7cf8429ad0efb12ad4f14420c9ab56b71664f77d4')
        # account
        res = self.db_proxy.get(FLAGS.accounts, {"address":"0x2a65aca4d5fc5b5c859090a6c34d164135398226"}, block_height = self.handler.blk_number)
        # 1 balance
        balance = res['balance']
        self.assertEqual(balance, utils.unit_convert_from_ether(5.15782563))
        # 2 mine
        mine = res['mine']
        self.assertIn(blk['hash'], mine)
        # 3 uncle
        buncle = self.db_proxy.get(FLAGS.uncles, {"mainNumber":2074903}, block_height = self.handler.blk_number)
        self.assertEqual(buncle['mainNumber'], 2074903)
        # 4 tx
        txs = self.db_proxy.search(FLAGS.txs, {"blockNumber": hex(2074903)}, multi = True)
        self.assertEqual(len(txs), 2)

    def test_process_by_hash(self):
        self.handler.blk_number = 2074903
        block = "0xcb3d7de2ed7817fb5c5763c7cf8429ad0efb12ad4f14420c9ab56b71664f77d4"
        self.handler.execute(block, False, True)
        # block
        blk = self.db_proxy.get(FLAGS.blocks, {"number":self.handler.blk_number}, block_height = self.handler.blk_number)
        self.assertEqual(blk['hash'], '0xcb3d7de2ed7817fb5c5763c7cf8429ad0efb12ad4f14420c9ab56b71664f77d4')
        # account
        res = self.db_proxy.get(FLAGS.accounts, {"address":"0x2a65aca4d5fc5b5c859090a6c34d164135398226"}, block_height = self.handler.blk_number)
        # 1 balance
        balance = res['balance']
        self.assertEqual(balance, utils.unit_convert_from_ether(5.15782563))
        # 2 mine
        mine = res['mine']
        self.assertIn(blk['hash'], mine)
        # 3 uncle
        buncle = self.db_proxy.get(FLAGS.uncles, {"mainNumber":2074903}, block_height = self.handler.blk_number)
        self.assertEqual(buncle['mainNumber'], 2074903)
        # 4 tx
        txs = self.db_proxy.search(FLAGS.txs, {"blockNumber": hex(2074903)}, multi = True)
        self.assertEqual(len(txs), 2)

    def test_revert(self):
        self.handler.blk_number = 2074903
        block = self.rpc_cli.call("eth_getBlockByNumber", 2074903, True)
        txs = block['transactions']
        self.handler.process_block(block, False)

        blk = self.db_proxy.get(FLAGS.blocks, {"number":self.handler.blk_number}, block_height = self.handler.blk_number)
        self.handler.revert(blk,txs)

        # block
        blk = self.db_proxy.get(FLAGS.blocks, {"number":self.handler.blk_number}, block_height = self.handler.blk_number)
        self.assertEqual(blk, None)
        # account
        res = self.db_proxy.get(FLAGS.accounts, {"address":"0x2a65aca4d5fc5b5c859090a6c34d164135398226"}, block_height = self.handler.blk_number)
        # 1 balance
        balance = res['balance']
        self.assertEqual(balance, 0)
        # 2 mine
        mine = res['mine']
        self.assertEqual(mine, [])
        # 3 tx
        txs = self.db_proxy.search(FLAGS.txs, {"blockNumber": hex(2074903)}, multi = True)
        self.assertEqual(len(txs), 0)

    
    def test_add_genesis(self):
        FLAGS.genesis_data = "../genesisdata/genesis_frontier.json"
        driver = builtin.BuiltinDriver()
        driver.add_genesis_data()
        
        # block
        blk = self.db_proxy.get(FLAGS.blocks, {"number":0}, block_height = 0)
        self.assertEqual(blk['hash'], '0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3')
        # account
        res = self.db_proxy.get(FLAGS.accounts, {"address":"0x3282791d6fd713f1e94f4bfd565eaa78b3a0599d"}, block_height = 0)
        # 1 balance
        balance = res['balance']
        self.assertEqual(balance, utils.unit_convert(1337000000000000000000))
        # 2 tx
        res = self.db_proxy.get(FLAGS.txs, None, block_height=0, multi = True)
        self.assertEqual(len(res), 8893)
    
    def test_process_tx(self):
        # block 1700002
        tx_hash = "0xa2d7bdf90e507979d7005399f2af77918a538d5288076b0e2a1308e7a419f1bc"
        tx_obj = self.rpc_cli.call("eth_getTransactionByHash", tx_hash)
        self.handler.blk_number = 1700002
        self.handler.process_tx(tx_obj, None, False)
        # tx
        tx = self.db_proxy.get(FLAGS.txs, {"hash":tx_hash}, block_height = 1700002, multi = False)
        self.assertEqual(tx["hash"], tx_hash)
        self.assertEqual(utils.convert_to_int(tx['gasUsed']), 21000)
        # 1 account
        acc1 = self.db_proxy.get(FLAGS.accounts, {"address":"0x4bb96091ee9d802ed039c4d1a5f6216f90f81b01"}, block_height = 1700002, multi = False)
        acc2 = self.db_proxy.get(FLAGS.accounts, {"address":"0xae8f3c8d1134e50a7c63c39d78406ab7334149ac"}, block_height = 1700002, multi = False)
        self.assertIn(tx['hash'], acc1['tx_out'])
        self.assertIn(tx['hash'], acc2['tx_in'])
        total = tx['fee'] + utils.unit_convert(utils.convert_to_int(tx['value']))
        self.assertEqual(acc1['balance'], -1 * total)
        self.assertEqual(acc2['balance'], utils.unit_convert(utils.convert_to_int(tx['value'])))

        # create contract tx
        # insert
        tx_hash = "0xfeae1ff3cf9b6927d607744e3883ea105fb16042d4639857d9cfce3eba644286"
        tx_obj = self.rpc_cli.call("eth_getTransactionByHash", tx_hash)
        self.handler.blk_number = 1883496
        self.handler.process_tx(tx_obj, None, False)

        # tx
        tx = self.db_proxy.get(FLAGS.txs, {"hash":tx_hash}, block_height = 1883496, multi = False)
        self.assertEqual(tx["hash"], tx_hash)
        self.assertEqual(utils.convert_to_int(tx['gasUsed']), 368040)
        # account
        # 1 account
        acc1 = self.db_proxy.get(FLAGS.accounts, {"address":"0x2ef1f605af5d03874ee88773f41c1382ac71c239"}, block_height = 1883496, multi = False)
        acc2 = self.db_proxy.get(FLAGS.accounts, {"address":"0xbf4ed7b27f1d666546e30d74d50d173d20bca754"}, block_height = 1883496, multi = False)
        
        self.assertIn(tx['hash'], acc1['tx_out'])
        self.assertIn(tx['hash'], acc2['tx_in'])
        total = tx['fee'] + utils.unit_convert(utils.convert_to_int(tx['value']))
        self.assertEqual(acc1['balance'], -1 * total)
        self.assertEqual(acc2['balance'], utils.unit_convert(utils.convert_to_int(tx['value'])))
        self.assertEqual(acc2['is_contract'], 1)
        
    def test_revert_tx(self):
        # insert
        tx_hash = "0xa2d7bdf90e507979d7005399f2af77918a538d5288076b0e2a1308e7a419f1bc"
        tx_obj = self.rpc_cli.call("eth_getTransactionByHash", tx_hash)
        self.handler.blk_number = 1700002
        self.handler.process_tx(tx_obj, None, False)

        # revert
        tx = self.db_proxy.get(FLAGS.txs, {"hash":tx_hash}, block_height = 1700002, multi = False)
        self.handler.revert_tx(tx, 1700002)
        # tx
        res = self.db_proxy.get(FLAGS.txs, {"hash":tx_hash}, block_height = 1700002, multi = False)
        self.assertEqual(res, None)
        # account
        # 1 account
        acc1 = self.db_proxy.get(FLAGS.accounts, {"address":"0x4bb96091ee9d802ed039c4d1a5f6216f90f81b01"}, block_height = 1700002, multi = False)
        acc2 = self.db_proxy.get(FLAGS.accounts, {"address":"0xae8f3c8d1134e50a7c63c39d78406ab7334149ac"}, block_height = 1700002, multi = False)
        self.assertNotIn(tx['hash'], acc1['tx_out'])
        self.assertNotIn(tx['hash'], acc2['tx_in'])
        self.assertEqual(acc1['balance'], 0)
        self.assertEqual(acc2['balance'], 0)

        # create contract tx
        # insert
        tx_hash = "0xfeae1ff3cf9b6927d607744e3883ea105fb16042d4639857d9cfce3eba644286"
        tx_obj = self.rpc_cli.call("eth_getTransactionByHash", tx_hash)
        self.handler.blk_number = 1883496
        self.handler.process_tx(tx_obj, None, False)

        # revert
        tx = self.db_proxy.get(FLAGS.txs, {"hash":tx_hash}, block_height = 1883496, multi = False)
        self.handler.revert_tx(tx, 1883496)
        # tx
        res = self.db_proxy.get(FLAGS.txs, {"hash":tx_hash}, block_height = 1883496, multi = False)
        self.assertEqual(res, None)
        # account
        # 1 account
        acc1 = self.db_proxy.get(FLAGS.accounts, {"address":"0x2ef1f605af5d03874ee88773f41c1382ac71c239"}, block_height = 1883496, multi = False)
        acc2 = self.db_proxy.get(FLAGS.accounts, {"address":"0xbf4ed7b27f1d666546e30d74d50d173d20bca754"}, block_height = 1883496, multi = False)
        self.assertEqual(acc2, None)
        self.assertNotIn(tx['hash'], acc1['tx_out'])
        self.assertEqual(acc1['balance'], 0)

    def test_process_uncle(self):
        # block 2122962
        blk_hash = "0xa9389966cec0062be52f16440e9ee9447e849698934b62aac93138fdfdb751b1"
        blk_obj = self.rpc_cli.call("eth_getBlockByHash", blk_hash, False)
        self.handler.blk_number = 2122962
        self.handler.process_uncle(blk_obj, False)
        # uncle
        uncle = self.db_proxy.get(FLAGS.uncles, {"hash":"0xa05ba9c6f686d92ef62a1adf18c3c97ed9041b3341de74c20d3cb421216a7f48"}, block_height = 2122962, multi = False)
        self.assertEqual(uncle['mainNumber'], 2122962)
        self.assertEqual(uncle['hash'], "0xa05ba9c6f686d92ef62a1adf18c3c97ed9041b3341de74c20d3cb421216a7f48")
        self.assertEqual(uncle['reward'], utils.unit_convert_from_ether(4.375))
        # account
        acc = self.db_proxy.get(FLAGS.accounts, {"address":"0x2a65aca4d5fc5b5c859090a6c34d164135398226"}, block_height = 2122962, multi = False)
        # 1 balance
        balance = acc['balance']
        self.assertEqual(balance, utils.unit_convert_from_ether(4.375))
        # 2 mine
        mine_uncles = acc['uncles']
        self.assertIn(uncle['hash'], mine_uncles)

    def test_revert_uncle(self):
        # block 2122962
        blk_hash = "0xa9389966cec0062be52f16440e9ee9447e849698934b62aac93138fdfdb751b1"
        blk_obj = self.rpc_cli.call("eth_getBlockByHash", blk_hash, False)
        self.handler.blk_number = 2122962
        self.handler.process_uncle(blk_obj, False)
        self.handler.revert_uncle(blk_obj)
        # uncle
        uncle = self.db_proxy.get(FLAGS.uncles, {"hash":"0xa05ba9c6f686d92ef62a1adf18c3c97ed9041b3341de74c20d3cb421216a7f48"}, block_height = 2122962, multi = False)
        self.assertEqual(uncle, None)
        # account
        acc = self.db_proxy.get(FLAGS.accounts, {"address":"0x2a65aca4d5fc5b5c859090a6c34d164135398226"}, block_height = 2122962, multi = False)
        # 1 balance
        balance = acc['balance']
        self.assertEqual(balance, 0)
        # 2 mine
        mine_uncles = acc['uncles']
        self.assertEqual(mine_uncles, [])

    def tearDown(self):
        self.db_proxy.drop_db("test_db")
        FLAGS.mongodb_default_db = self.origin_db

        
        