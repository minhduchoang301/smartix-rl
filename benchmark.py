from dqn import *
from pg_database import PG_Database
from math import sqrt
def get_indexes_size(db):
    return db.get_indexes_size()

def create_all_indexes(db):
    return db.create_all_index()

def get_qphh(db):
    return sqrt(get_power_at_size(db) * get_throughput_at_size(db))

# def smartix_get_qhh(all_execution_time):
#     product_all_execution_time = 1
#     for time in all_execution_time:
#         product_all_execution_time *= time
#     power_at_size = 3600 / (product_all_execution_time ** (1/19))
#     throughput_at_size = 19 * 3600/sum(all_execution_time)
#     return sqrt(power_at_size * throughput_at_size)

def get_power_at_size(db):
    return 3600 / (get_product_all_query_execution_time(db) ** (1/19))

def get_throughput_at_size(db):
    stream = 1
    return 19 * 3600/sum(db.get_all_query_execution_time())

def get_product_all_query_execution_time(db):
    times = db.get_all_query_execution_time()
    product = 1
    for time in times:
        product *= time
    return product

def create_index_for_smartix(db, pairs):
    for pair in pairs:
        db.create_index(pair[0], pair[1])

if __name__ == '__main__':
    import os
    print("Restarting PostgreSQL...")
    os.system('sudo service postgresql restart')
    db1 = PG_Database(hypo = False)
    db1.reset_indexes()
    # no indices
    print(get_indexes_size(db1))
    print(get_qphh(db1))

    # all indices
    print("Restarting PostgreSQL...")
    os.system('sudo service postgresql restart')
    db2 = PG_Database(hypo=False)
    db2.reset_indexes()
    create_all_indexes(db2)
    print(get_indexes_size(db2))
    print(get_qphh(db2))
    db2.reset_indexes()

    # smartix
    print("Restarting PostgreSQL...")
    os.system('sudo service postgresql restart')
    agent_test = Agent(env=Environment(window_size=40, shift=False), tag='winsize40_model40_test_10gb')
    result = agent_test.test(model_path='/home/hmduc9c2/smartix/smartix-rl/output/1637785785.0371192_0.0001_0.9_100000_10000_128_1024_0.01_0.01_winsize40_noshift')
    db3 = PG_Database(hypo=False)
    db3.reset_indexes()
    create_index_for_smartix(db3, result)
    print(get_indexes_size(db3))
    print(get_qphh(db3))