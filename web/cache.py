import os
import redis
import json


class Cache:
    def __init__(self):
        self.redis = redis.Redis(host=os.getenv('redisHost', 'adapter-redis-master.default.svc.cluster.local'), port=os.getenv('redisPort', 6379), db=os.getenv('redisDB', 0), password=os.getenv('redisPassword', 'wdias123'))

    def set_timeseries(self, key: str, timeseriesId, moduleId, valueType, parameterId, locationId, timeseriesType, timeStepId, *args, **kargs):
        return self.set(key, {
            'timeseriesId': timeseriesId,
            'moduleId': moduleId,
            'valueType': valueType,
            'parameterId': parameterId,
            'locationId': locationId,
            'timeseriesType': timeseriesType,
            'timeStepI': timeStepId
        })

    def set(self, key: str, data: dict):
        return self.redis.set(key, json.dumps(data))

    def get(self, key: str):
        data = self.redis.get(key)
        if data is None:
            return data
        return json.loads(data)

    def delete(self, key: str):
        return self.redis.delete(key)
