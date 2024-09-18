# -*- coding: utf-8 -*-
"""
@Create: 2024/9/10 10:46
@File: redisClient.py
@Author: Jingyuan
"""
import redis
from loggerUtil import generate_logger

logger = generate_logger(level='INFO')


class RedisClient:
    def __init__(self, _host, _port):
        self.client = redis.Redis(_host, _port)

    def get_db_keys(self, _db) -> list or None:
        try:
            self.client.select(_db)
            keys = self.client.keys('*') or []
            return keys

        except Exception as e:
            logger.error(e)
            return

    def remove(self, _db, _key) -> bool:
        try:
            key = [_key] if isinstance(_key, str) else _key
            self.client.select(_db)
            self.client.delete(*key)
            return True

        except Exception as e:
            logger.error(e)
            return False

    def get_hash(self, _db, _key, _field) -> str or None:
        try:
            self.client.select(_db)
            value = self.client.hget(_key, _field) or ''
            return value

        except Exception as e:
            logger.error(e)
            return

    def get_hash_all(self, _db, _key) -> dict or None:
        try:
            self.client.select(_db)
            keys = self.client.hgetall(_key) or {}
            return keys

        except Exception as e:
            logger.error(e)
            return

    def add_hash(self, _db, _key, _field_dict: dict) -> bool:
        try:
            self.client.select(_db)
            self.client.hset(_key, mapping=_field_dict)
            return True

        except Exception as e:
            logger.error(e)
            return False

    def get_ts_batch_by_key(self, _db, _key, _start_ts, _end_ts, _limit=None) -> list or None:
        try:
            self.client.select(_db)
            ts = self.client.ts()
            batch = ts.range(_key, _start_ts, _end_ts, count=_limit) or {}
            return batch

        except Exception as e:
            logger.error(e)
            return

    def get_ts_batch_by_labels(self, _db, _labels, _start_ts, _end_ts, _limit=None) -> list or None:
        try:
            self.client.select(_db)
            ts = self.client.ts()
            batch = ts.mrange(_start_ts, _end_ts, filters=_labels, count=_limit) or []
            return batch

        except Exception as e:
            logger.error(e)
            return

    def add_ts_batch(self, _db, _key, _batch: list):
        try:
            self.client.select(_db)
            ts = self.client.ts()
            with ts.pipeline() as pipe:
                pipe.multi()
                for data in _batch:
                    pipe.ts().add(_key, data[0], data[1])
                pipe.execute()

            return True

        except Exception as e:
            logger.error(e)
            return False
