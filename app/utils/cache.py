"""缓存管理"""
import time


class DataCache:
    """内存缓存类"""
    def __init__(self, ttl=300):
        """
        初始化缓存
        Args:
            ttl: 缓存过期时间（秒），默认5分钟
        """
        self.store = {}
        self.ttl = ttl

    def get(self, key):
        """
        获取缓存数据
        Args:
            key: 缓存键
        Returns:
            缓存的数据，如果不存在或已过期返回None
        """
        if key in self.store:
            data, timestamp = self.store[key]
            if time.time() - timestamp < self.ttl:
                return data
        return None

    def set(self, key, data):
        """
        设置缓存数据
        Args:
            key: 缓存键
            data: 要缓存的数据
        """
        self.store[key] = (data, time.time())
    
    def clear(self):
        """清空所有缓存"""
        self.store.clear()
    
    def delete(self, key):
        """删除指定缓存"""
        if key in self.store:
            del self.store[key]

