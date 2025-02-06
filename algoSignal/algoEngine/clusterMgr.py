# -*- coding: utf-8 -*-
"""
@Create on  2024/9/29 12:12
@file: clusterMgr.py
@author: Jerry
"""
import asyncio
from algoUtils.asyncRedisUtil import AsyncRedisClient


class RedisCluster:
    def __init__(self, _config_redis: AsyncRedisClient):
        self.config_redis = _config_redis
        self.cluster = {}

    async def init_cluster(self, _is_localhost=False):
        shard_ids = await self.config_redis.get_hash_all(0, 'data_shard')
        if _is_localhost:
            self.cluster.update(
                {'localhost:{}'.format(7001 + v): AsyncRedisClient('localhost', 7001 + v)
                 for v in range(len(shard_ids))}
            )
        else:
            self.cluster.update({v: AsyncRedisClient(*v.decode().split(':')) for v in shard_ids.keys()})

    async def get_batch_by_labels(self, _db, _start_ts, _end_ts, _labels: dict, _limit=None) -> dict | None:
        tasks = [v.get_ts_batch_by_labels(_db, _start_ts, _end_ts, _labels, _limit) for v in self.cluster.values()]
        rsp = await asyncio.gather(*tasks)
        if None in rsp:
            return

        return {k: v for k, v in zip(self.cluster.keys(), rsp)}
