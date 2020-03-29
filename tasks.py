from celery import Celery
from celery import Celery, chord, chain
from celery.utils.log import get_task_logger

import os
import threading

app = Celery('tasks', backend='redis://localhost', broker='redis://localhost')

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
def partial_reduce(mapped):
    """ Reduce worker """
    data = 0
    count = 0
    for d in mapped:
        data += d["data"]
        count += d["count"]
    return {"count": count, "data": data}

DB =  {"count": 0, "data": 0}
DB_LOCK = threading.Lock()

@app.task(acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 5})
def db_reduce(mapped):
    """ Reduce worker """
    for d in mapped:
        with DB_LOCK:
            DB["data"] += d["data"]
            DB["count"] += d["count"]

@app.task(acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 5})
def get_db_result(mapped):
    """ Reduce worker """
    global DB
    data = DB
    DB = {"count": 0, "data": 0}
    return data

@app.task(acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 5})
def map(data):
    """ Map worker """
    results = []
    logger.debug(f"{os.getpid()} - {threading.get_ident()}")
    for chunk, data in data:
        results.append({"chunk": chunk, "count": 1, "data": data})
    return results
