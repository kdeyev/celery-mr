import time
import celery
import tasks
from toolz.itertoolz import partition_all, concat

app = tasks.app

def create_work(elements_count, chunk_size):
    """ A fast task for initiating our map function """
    data = partition_all(chunk_size, range(elements_count))

    # break up our data into chunks and create a dynamic list of workers
    maps = (tasks.map.s(x) for x in data)
    mapreducer = celery.chord(maps)(tasks.reduce.s())

    mapper = mapreducer.parent
    reducer = mapreducer
    
    # required for celery.result.GroupResult.restore
    mapper.save()
    
    return (mapper.id, reducer.id)


def get_work(mapper_id, reducer_id):
    """ A fast task for checking our map result """

    reducer = app.AsyncResult(reducer_id)
    completed = 0
    
    # GroupResult doesn't work properly with MongoDB
    # mapper = celery.result.GroupResult.restore(mapper_id)
    # completed = mapper.completed_count()
    
    if reducer.ready():
        return {
            'status': 'success',
            'completed': completed,
            'results': reducer.get()}
    else:
        return {'status': 'pending', 'completed': completed}

def wait_for_task(mapper_id, reducer_id):
    print(f"Waiting for task {reducer_id}")
    
    for i in range(100):
        time.sleep(1)
        results = get_work(mapper_id, reducer_id)
        print(f"Task {reducer_id} status: {results}")
        if results['status'] == 'success':
            return results['results']
        
    return None 
        
if __name__ == '__main__':
    mapper_id, reducer_id = create_work(elements_count=100000,chunk_size=100)
    print(f"Task started {reducer_id}")
    
    results = wait_for_task(mapper_id, reducer_id)
    
    print(f"Task execution result {results}")
