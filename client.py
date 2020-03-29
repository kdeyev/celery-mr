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

def wait_for_task(my_id):
    print(f"Waiting for task {my_id}")
    
    for i in range(100):
        time.sleep(1)
        results = get_work(my_id)
        print(f"Task {my_id} status: {results['status']}")
        if results['status'] == 'success':
            return results
        
    return None 
        
if __name__ == '__main__':
    my_id = create_work(chunk_size=4)
    print(f"Task started {my_id}")
    
    results = wait_for_task(my_id)
    
    print(f"Task execution result {results}")
