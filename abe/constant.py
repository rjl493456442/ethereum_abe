# RPC METHOD
METHOD_GET_BLOCK_BY_NUMBER = "eth_getBlockByNumber"
METHOD_GET_BLOCK_BY_HASH = "eth_getBlockByHash"
METHOD_BLOCK_NUMBER = "eth_blockNumber"
METHOD_GET_TX_RECEIPT = "eth_getTransactionReceipt"
METHOD_GET_CODE = "eth_getCode"
METHOD_GET_TX_BY_HASH = "eth_getTransactionByHash"
METHOD_GET_BALANCE = "eth_getBalance"

METHOD_NEW_FILTER = "eth_newFilter"
METHOD_NEW_BLOCK_FILTER = "eth_newBlockFilter"
METHOD_NEW_PENDING_TX_FILTER = "eth_newPendingTransactionFilter"


METHOD_UNINSTALL_FILTER = "eth_uninstallFilter"
METHOD_GET_FILTER_CHANGES = "eth_getFilterChanges"
METHOD_GET_FILTER_LOGS = "eth_getFilterLogs"
METHOD_GET_UNCLE_BY_BLOCK_HASH_AND_INDEX = "eth_getUncleByBlockHashAndIndex"



#DCS
DCS_ADDR = "0xafe6851c1d9ee2e759acdee8cfc827e22a9ec5d7"
DCS_INIT_TX_HASH = "0x02de590b6dffa94c177182cbb960082a3cc741a97728038b814744c6df413566"
DCS_CONTRACT_NAME = "MyToken"
DCS_EVENT = 'Transfer'
DCS_CONSTRUCTOR = "MyToken"

#DGD
DGD_ADDR = "0xe0b7927c4af23765cb51314a0e0521a9645f0e2a"
DGD_INIT_TX_HASH = "0xe6a50122e15dd149eabf18e9de8044264d32360b7fea156cb0e63ec3f721120e"
DGD_CONTRACT_NAME = "Token"
DGD_EVENT = 'Transfer'
DGD_CROWDSALE = "DIGIX"

#DIGIX
DIGIX_ADDR = "0xf0160428a8552ac9bb7e050d90eeade4ddd52843"
DIGIX_CONTRACT_NAME = "TokenSales"
DIGIX_EVENT = 'Claim'
DIGIX_TOKEN_INFO = "DGD"