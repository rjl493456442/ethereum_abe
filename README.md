## ethereum abe

**ethereum** 是一个用来爬取以太坊数据的项目

### 使用方法

	python execute.py [options]
	options:
      --sync_block_forever:  同步区块数据
      --sync_token:  同步token数据
	  --token:       token名字
      --mongodb_default_db:  数据库
	  --table_capacity: shard大小
	  --rpc_host:    数据源ip
      --rpc_port:    数据源端口

    example：
       python execute.py --sync_block_forever=True
       python execute.py --sync_block=True --begin=0 --end=2000000
       python execute.py --sync_token=True --token=DCS



### mongo表

 - blocks
 - uncles
 - txs
 - accounts
 - token_{$token-name}
 - balance_{$token_name}
 - token_basic

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







