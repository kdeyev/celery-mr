import threading

from flask import Flask
app = Flask(__name__)

def start_worker():
    import celery.bin.worker
    import tasks

    worker = celery.bin.worker.worker(app=tasks.app)

    options = {
        'loglevel': 'WARNING',
        'traceback': True,
        # "pool_cls": "solo", # single thread
        "pool_cls": "threads", # multithread
        "concurrency": 2
    }

    worker.run(**options)    

@app.route('/')
def login():
    return 'Worker'

if __name__ == "__main__": 
    worker_thread = threading.Thread(target=start_worker)
    worker_thread.start()
    
    app.run()