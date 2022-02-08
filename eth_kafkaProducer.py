import json
from datetime import datetime
from kafka import KafkaProducer
from etherscan import Etherscan
from local.token import EthScan
from ratelimit import rate_limited, sleep_and_retry


def main():
    producer.send('ethTest', get_price())


@sleep_and_retry
@rate_limited(1, 60)
def get_price():
    tmp = eth.get_eth_last_price()

    tmp['ethbtc'] = float(tmp['ethbtc'])
    tmp['ethbtc_timestamp'] = datetime.utcfromtimestamp(int(tmp["ethbtc_timestamp"]))
    tmp['ethusd'] = float(tmp['ethusd'])
    tmp['ethusd_timestamp'] = datetime.utcfromtimestamp(int(tmp["ethusd_timestamp"]))

    return tmp


producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'))
eth = Etherscan(EthScan.main_token)

while True:
    main()
