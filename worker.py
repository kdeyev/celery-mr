
from celery import Celery
import celery.bin.worker
import tasks

app = tasks.app

def start_worker():
    worker = celery.bin.worker.worker(app=app)

    options = {
        'loglevel': 'INFO',
        'traceback': True,
        # "pool_cls": "solo", # single thread
        "pool_cls": "threads", # multithread
        "concurrency": 2
    }

    worker.run(**options)
    
    

if __name__ == "__main__":    
    start_worker()