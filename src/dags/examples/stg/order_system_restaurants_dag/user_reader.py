from datetime import datetime
from typing import Dict, List

from lib import MongoConnect


class UserReader:
    def __init__(self, mc: MongoConnect) -> None:
        self.dbs = mc.client()

    def get_users(self, load_threshold: datetime, limit) -> List[Dict]:
        filter = {'update_ts': {'$gt': load_threshold}}
        sort = [('update_ts', 1)]

        docs = list(self.dbs.get_collection("users").find(filter=filter, sort=sort, limit=limit))
        return docs