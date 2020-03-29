
from celery import Celery
import celery.bin.worker
import tasks

app = tasks.app

def start_worker():
    worker = celery.bin.worker.worker(app=app)

    # options = {
    #     'broker': 'amqp://guest:guest@localhost:5672//',
    #     'loglevel': 'INFO',
    #     'traceback': True,
    # }

    worker.run()

if __name__ == "__main__":    
    start_worker()