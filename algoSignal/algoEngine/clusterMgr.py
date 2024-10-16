# -*- coding: utf-8 -*-
"""
@Create on  2024/9/29 12:12
@file: clusterMgr.py
@author: Jerry
"""
from algoUtils.redisUtil import RedisClient
from ..algoConfig.threadPoolConfig import pool


class RedisCluster:
    def __init__(self, _host, _port, _type='data'):
        self.cluster_type = _type
        self.client = RedisClient(_host, _port)
        self.cluster = {}

    def init_cluster(self, _is_localhost=False):
        if self.cluster_type == 'data':
            shard_ids = self.client.get_hash_all(0, 'data_shard')
        elif self.cluster_type == 'signal':
            shard_ids = self.client.get_hash_all(0, 'signal_shard')
        else:
            shard_ids = self.client.get_hash_all(0, 'order_shard')

        if _is_localhost:
            self.cluster.update(
                {'localhost:{}'.format(7001 + v): RedisClient('localhost', 7001 + v)
                 for v in range(len(shard_ids))}
            )
        else:
            self.cluster.update({v: RedisClient(*v.decode().split(':')) for v in shard_ids.keys()})

    def get_batch_by_labels(self, _db, _start_ts, _end_ts, _labels: dict, _limit=None) -> {} or None:
        tasks = [
            pool.submit(v.get_ts_batch_by_labels, *(_db, _start_ts, _end_ts, _labels, _limit))
            for v in self.cluster.values()
        ]
        rsp = [v.result() for v in tasks]
        if None in rsp:
            return

        return {k: v for k, v in zip(self.cluster.keys(), rsp)}
