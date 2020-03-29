from celery import Celery
from celery import Celery, chord, chain
from celery.utils.log import get_task_logger


import random
import os
import threading

app = Celery('tasks', backend='mongodb://localhost', broker='mongodb://localhost')

logger = get_task_logger(__name__)

@app.task(acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 5})
def reduce(mapped):
    """ Reduce worker """
    data = 0
    count = 0
    for chunk in mapped:
        for d in chunk:
            data += d["data"]
            count += d["count"]
    return {"count": count, "data": data}

@app.task(acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 5})
def map(data):
    """ Map worker """
    results = []
    logger.debug(f"{os.getpid()} - {threading.get_ident()}")
    for chunk in data:
        results.append({"chunk": chunk, "count": 1, "data": random.randrange(10000)})
    return results