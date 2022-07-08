from dotenv import load_dotenv
import os
from web3 import Web3
import eth_keys
from eth_account import account
from web3.middleware import geth_poa_middleware
import json
 

load_dotenv()

WALLET = os.getenv('WALLET') #public key 
WALLET_PRIVATE_KEY = os.getenv('WALLET_PRIVATE_KEY')
INFURA_PROJECT_ID = os.getenv('INFURA_PROJECT_ID') #set up an account with infura, it's free, toggle polychain to be on. 
INFURA_URL = os.getenv('INFURA_URL') #'https://polygon-mainnet.infura.io/v3'
POLYGON_CHAIN_ID = os.getenv('POLYGON_CHAIN_ID') #137
COVEY_LEDGER_POLYGON_ADDRESS = os.getenv('COVEY_LEDGER_POLYGON_ADDRESS') #"0x587Ec5a7a3F2DE881B15776BC7aaD97AA44862Be"

COVEY_LEDGER_SKALE_ADDRESS = os.getenv('COVEY_LEDGER_SKALE_ADDRESS')
SKALE_URL = os.getenv('SKALE_URL')
SKALE_CHAIN_ID = os.getenv('SKALE_CHAIN_ID')

# Opening JSON file
f = open('CoveyLedger.json')
 
# returns JSON object as
# a dictionary
ledger_info = json.load(f)
# Geth web3 for account stuff
gethWeb3 = Web3(Web3.IPCProvider())
# You can switch this to polygon "mainnet" by using mainnet.infura instead of mumbai.infura

def post_trades_polygon(positionString):
  w3 = Web3(Web3.HTTPProvider(f'{INFURA_URL}/{INFURA_PROJECT_ID}'))
  w3.middleware_onion.inject(geth_poa_middleware, layer=0)
  covey_ledger = w3.eth.contract(address = COVEY_LEDGER_POLYGON_ADDRESS, abi = ledger_info['abi'])
  my_address = w3.toChecksumAddress(WALLET)
  nonce = w3.eth.get_transaction_count(my_address)

  gas = covey_ledger.functions.createContent(positionString).estimateGas({'from': my_address, 'nonce': nonce})

  txn = covey_ledger.functions.createContent(positionString).buildTransaction({
    'chainId': int(POLYGON_CHAIN_ID),
    'gas': gas,
    'nonce': nonce,
    'from': my_address
  })
  signed_txn = w3.eth.account.sign_transaction(txn, private_key=WALLET_PRIVATE_KEY)
  w3.eth.send_raw_transaction(signed_txn.rawTransaction)  

def post_trades_skale(positionString):
  '''TESTING STILL - DO NOT USE YET'''
  w3 = Web3(Web3.HTTPProvider(SKALE_URL))
  w3.middleware_onion.inject(geth_poa_middleware, layer=0)
  covey_ledger = w3.eth.contract(address = COVEY_LEDGER_SKALE_ADDRESS, abi = ledger_info['abi'])
  my_address = w3.toChecksumAddress(WALLET)
  nonce = w3.eth.get_transaction_count(my_address)

  #gas = covey_ledger.functions.createContent(positionString).estimateGas({'from': my_address, 'nonce': nonce})

  txn = covey_ledger.functions.createContent(positionString).buildTransaction({
    'chainId': int(SKALE_CHAIN_ID),
    'gas': 21000,
    'nonce': nonce,
    'from': my_address
  })
  signed_txn = w3.eth.account.sign_transaction(txn, private_key=WALLET_PRIVATE_KEY)
  w3.eth.send_raw_transaction(signed_txn.rawTransaction)  

# The password here MUST match the password you used to generate your accounts, otherwise it will fail
def get_private_keys(password):
  wallets_list = gethWeb3.geth.personal.list_wallets()
  for i in wallets_list:
    address = i['accounts'][0].address
    keyfile_path = i['accounts'][0].url.replace("keystore://", "").replace("\\", "/")
    keyfile = open(keyfile_path)
    keyfile_contents = keyfile.read()
    keyfile.close()
    private_key = eth_keys.keys.PrivateKey(account.Account.decrypt(keyfile_contents, password))
    public_key = private_key.public_key
    private_key_str = str(private_key)
    public_key_str = str(public_key)
    print(f'Address: {address} Private Key: {private_key_str}')


# The password used here must match the password used to generate the address you are feeding into here
def get_private_key(address, password):
  wallets_list = gethWeb3.geth.personal.list_wallets()
  keyfile_path = (wallets_list[list(i['accounts'][0]['address'].lower() for i in wallets_list).index(address)]['url']).replace("keystore://", "").replace("\\", "/")
  keyfile = open(keyfile_path)
  keyfile_contents = keyfile.read()
  keyfile.close()
  private_key = eth_keys.keys.PrivateKey(account.Account.decrypt(keyfile_contents, password))
  public_key = private_key.public_key

  private_key_str = str(private_key)
  public_key_str = str(public_key)
  print(private_key_str)



#post_trades_polygon('FB:0.1,FNF:0.2,BTCUSDT:0.2,FNV:0.2,PLTR:0.2,GPS:0.2')
#get_private_keys("password")
#get_private_key("0xd3170f3405782d38fbf9ccb291e143b9702c0659", "password")
#get_private_key("0x1aba07fe746e690d917117315cd42c6dad6cb4c6", "password")



