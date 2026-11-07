from src.config.redis import get_redis
import os
from dotenv import load_dotenv
import redis
load_dotenv()
redis_client= get_redis()

def group_exits(stream:str,group:str):
    try:
        groups = redis_client.xinfo_groups(stream)
        return any(g["name"] == group for g in groups)
    except redis_client.exceptions.ResponseError as e:
        if "no such key" in str(e).lower():
            return False
        raise

async def create_group(stream:str,group:str):
    try:
        await redis_client.xgroup_create(
            name=stream,
            groupname=group,
            id="0",
            mkstream=True
        )
    except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise