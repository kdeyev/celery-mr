import time
from celery import Celery, chord, chain
import tasks

app = tasks.app

def create_work(chunk_size):
    """ A fast task for initiating our map function """
    return tasks.mapreduce.delay(chunk_size).id


def get_work(chord_id):
    """ A fast task for checking our map result """

    if app.AsyncResult(chord_id).ready():
        result_id = app.AsyncResult(chord_id).get()['chord_id']
    else:
        return {'status': 'pending', 'stage': 1}

    if app.AsyncResult(result_id).ready():
        return {
            'status': 'success',
            'results': app.AsyncResult(result_id).get()}
    else:
        return {'status': 'pending', 'stage': 2}

if __name__ == '__main__':
    my_id = create_work(chunk_size=4)
    
    # start_worker()

    for i in range(100):
        time.sleep(1)
        results = get_work(my_id)
        if results['status'] == 'success':
            print(results)
            break