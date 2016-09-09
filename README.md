## ethereum abe

**ethereum** 是一个用来爬取以太坊数据的工具

### 使用方法

	python execute.py [options]
	options:
      --sync_block_forever:  同步区块数据
      --sync_token:  同步token数据
	  --token:       token名字
	  --tool:        小工具
	  --service:     小工具服务名
	  --shardId:     数据库的分片ID
      --mongodb_default_db:  数据库
	  --table_capacity: shard大小
	  --rpc_host:    数据源ip
      --rpc_port:    数据源端口

    example：
       python execution.py --sync_block_forever=True
       python execution.py --sync_block=True --begin=0 --end=2000000
       python execution.py --sync_token=True --token=DCS
       python execution.py --tool=True --service=statistic --shard=0 (对第一组表blocks0, txs0, uncles0进行统计， 统计tx_num, mine_num, uncle_num)

> 注意， 在使用`statistic`进行统计的时候，只能按顺序进行统计， 例如已经统计完了shard0，才能统计shard1。 且被统计的shard必须是已经完全同步完了，如进行统计shard0的前提是当前程序已经同步完shard0所有数据且已经开始进行shard1的同步


### mongo表
 - meta
 - blocks
 - uncles
 - txs
 - accounts
 - token_{$token-name}
 - balance_{$token_name}
 - token_basic

### meta

字段名  | 类型 |  描述
------------- | ------------- | ----------
_id  | bson.ObjectId | id
sync_record | string | 同步记录，如以太坊
account\_status\_flag | int | 当前程序同步的最新块
statistic_flag | int | 当前统计完的shardId

### blocks

字段名  | 类型
------------- | -------------
_id  | bson.ObjectId
number  | int
nonce  | hexstr
transactionRoot  | hexstr
hash  | hexstr
uncles  | list
receiptRoot  | hexstr
miner  | hexstr
parentHash  | hexstr
extraData  | hexstr
gasLimit  | hexstr
stateRoot  | hexstr
difficulty  | hexstr
size  | hexstr
timestamp  | hexstr
totalDifficulty  | hexstr
gasUsed  | hexstr
reward  | hexstr
sha3Uncles  | hexstr
logsBloom  | hexstr
reward  | int

### uncles

字段名  | 类型
------------- | -------------
_id  | bson.ObjectId
mainNumber | int
number  | hexstr
nonce  | hexstr
transactionRoot  | hexstr
hash  | hexstr
uncles  | list
receiptRoot  | hexstr
miner  | hexstr
parentHash  | hexstr
extraData  | hexstr
gasLimit  | hexstr
stateRoot  | hexstr
difficulty  | hexstr
size  | hexstr
timestamp  | hexstr
totalDifficulty  | hexstr
gasUsed  | hexstr
reward  | hexstr
sha3Uncles  | hexstr
logsBloom  | hexstr
reward  | int

### txs
字段名  | 类型
------------- | -------------
_id  | bson.ObjectId
hash  | hexstr
nonce  | hexstr
contractAddress  | hexstr
cumulativeGasUsed  | hexstr
fee  | int
logs  | list
blockHash  | hexstr
timestamp  | hexstr
gas  | hexstr
value  | hexstr
blockNumber  | hexstr
to  | hexstr
input  | hexstr
from  | hexstr
transactionIndex  | hexstr
gasPrice  | hexstr
gasUsed  | hexstr

### accounts
字段名  | 类型
------------- | -------------
_id  | bson.ObjectId
address  | hexstr
balance  | hexstr

### token_basic
字段名  | 类型
------------- | -------------
_id  | bson.ObjectId
address  | hexstr
abi  | str
source_code  | str
token  | str

### balance_${token-name}
字段名  | 类型
------------- | -------------
_id  | bson.ObjectId
account  | hexstr
balance  | int

### token_${token-name}
字段名  | 类型
------------- | -------------
_id  | bson.ObjectId
from  | hexstr
to  | hexstr
value  | hexstr
type  | str
hash  | hexstr
transactionHash  | hexstr
logIndex  | hexstr
block  | int
blockHash  | hexstr







