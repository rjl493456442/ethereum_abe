import datetime
import os
import gflags
FLAGS = gflags.FLAGS

#driver
gflags.DEFINE_string('db_driver', 'abe.db.api.MongodbClient', '')
gflags.DEFINE_string('block_driver', 'abe.block.driver.builtin.BuiltinDriver', '')
gflags.DEFINE_string('tx_driver', 'abe.tx.driver.builtin.BuiltinDriver', '')
gflags.DEFINE_list('token_driver', ['abe.token.driver.DGD.builtin.BuiltinDriver', 'abe.token.driver.DCS.builtin.BuiltinDriver',
    'abe.token.driver.DIGIX.builtin.BuiltinDriver'], '')

#mongodb
gflags.DEFINE_bool('MONGODB_ENABLE', True, 'switch')
gflags.DEFINE_bool('mongodb_remote_db', False, '')
gflags.DEFINE_string('mongodb_remote_host', '192.168.188.2', 'mongodb remote host')
gflags.DEFINE_string('mongodb_remote_port',  27017, 'mongodb remote port')
gflags.DEFINE_string('mongodb_user', 'abe', 'mongodb user')
gflags.DEFINE_string('mongodb_password', '14cZMQk89mRYQkDEj8Rn25AnGoBi5H6uer', 'mongodb host')
gflags.DEFINE_string('mongodb_host', 'localhost', 'mongodb host')
gflags.DEFINE_string('mongodb_port',  27017, 'mongodb port')
gflags.DEFINE_string('mongodb_default_db', 'ethereum', 'mongodb db')

#mongodb table
gflags.DEFINE_string('blocks', 'blocks', 'mongodb table')
gflags.DEFINE_string('uncles', 'uncles', 'mongodb table')
gflags.DEFINE_string('txs',  'txs', 'mongodb table')
gflags.DEFINE_string('accounts', 'accounts', 'mongodb table')
gflags.DEFINE_string('token_basic', 'token_basic', 'mongodb table')
gflags.DEFINE_string('token_prefix', 'token_', 'mongodb table')
gflags.DEFINE_string('balance_prefix', 'balance_', 'mongodb table')
gflags.DEFINE_string('meta', 'meta', 'mongodb table')
gflags.DEFINE_integer('table_capacity', 100000, 'table capacity')


#mongodb init index
gflags.DEFINE_list('blocks_index', ['number', 'hash'], 'mongodb index')
gflags.DEFINE_string('txs_index', 'hash', 'mongodb index')
gflags.DEFINE_string('accounts_index', 'address', 'mongodb index')
gflags.DEFINE_string('balance_index', 'account', 'mongodb index')

#rpc
gflags.DEFINE_string('rpc_host', '121.201.29.105', 'mongodb host')
gflags.DEFINE_string('rpc_port',  8545, 'mongodb port')
gflags.DEFINE_integer('poll_interval', 5, 'poll interval')

#multiprocess
gflags.DEFINE_integer('process_num', 1, 'process num')


#gevent
gflags.DEFINE_integer('greenlet_num', 20, 'greenlet num')
gflags.DEFINE_integer('threshold', 200, 'greenlet num')

#log
gflags.DEFINE_string('DEBUG_LOG', 'logs/debug.log', 'location')
gflags.DEFINE_string('ERROR_LOG', 'logs/error.log', 'location')
gflags.DEFINE_string('INFO_LOG', 'logs/info.log', 'location')
gflags.DEFINE_string("log_level", 'info', 'level')

#genesis
gflags.DEFINE_string('genesis_data', 'genesisdata/genesis_frontier.json', 'location')

#ethereum params
gflags.DEFINE_integer('default_reward', 5, 'default miner reward')

#ethereum coin precision
gflags.DEFINE_string('precision', 'wei', 'precision')


#cmd params
gflags.DEFINE_bool("sync_block", False, 'start sync block service')
gflags.DEFINE_bool("check_block", False, 'start check block service')
gflags.DEFINE_bool("sync_balance", False, 'whether sync balance after sync block finish')
gflags.DEFINE_integer("begin", -1, 'start sync block from')
gflags.DEFINE_integer("end", -1, 'start sync block to')

gflags.DEFINE_bool("sync_block_forever", False, 'start sync block service')

gflags.DEFINE_bool("sync_token", False, 'start sync token service')
gflags.DEFINE_string("token", "", "name of token to sync")

#log need to monitor
gflags.DEFINE_string("log_path", "", "location")
gflags.DEFINE_string("internaltxlog", "tx.log", "log name")
gflags.DEFINE_string("blocklog", "block.log", "log name")


#test cmd
gflags.DEFINE_bool('test_all', False, 'switch')
gflags.DEFINE_list('test_case', [], 'test case')



