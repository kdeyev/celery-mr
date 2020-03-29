from celery import Celery
from celery import Celery, chord, chain

from toolz.itertoolz import partition_all, concat
import random
import os

# https://www.distributedpython.com/2018/08/21/celery-4-windows/
os.environ["FORKED_BY_MULTIPROCESSING"] = "1"

app = Celery('tasks', backend='redis://localhost', broker='redis://localhost')

@app.task(acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 5})
def add(x, y):
    return x + y

@app.task(acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 5})
def process(chunk):
    return {"chunks" : 1}

@app.task(acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 5})
def reduce(mapped):
    """ Reduce worker """
    return list(concat(mapped))

@app.task(acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 5})
def map(data):
    """ Map worker """
    results = []
    for chunk in data:
        results.append(sum(chunk))
    return results


@app.task(acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 5})
def mapreduce(chunk_size):
    """ A long running task which splits up the input data to many workers """
    # create some sample data for our summation function
    data = []
    for i in range(10000):
        x = []
        for j in range(random.randrange(10) + 5):
            x.append(random.randrange(10000))
        data.append(x)

    # break up our data into chunks and create a dynamic list of workers
    maps = (map.s(x) for x in partition_all(chunk_size, data))
    mapreducer = chord(maps)(reduce.s())
    return {'chord_id': mapreducer.id}