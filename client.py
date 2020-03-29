import time
import celery
import tasks
from toolz.itertoolz import partition_all, concat

app = tasks.app

def create_mr(elements_count, chunk_size):
    # Generate input data data
    data = partition_all(chunk_size, range(elements_count))

    # Cretate map tasks
    maps = (tasks.map.s(x) for x in data)
    
    # Cretate reduce tasks
    mapreducer = celery.chord(maps)(tasks.reduce.s())

    mapper = mapreducer.parent
    reducer = mapreducer
    
    # Required for celery.result.GroupResult.restore
    mapper.save()
    
    return (mapper.id, reducer.id)

def create_part_mr(elements_count, chunk_size):
    # Generate input data data
    data = partition_all(chunk_size, range(elements_count))

    # Cretate map tasks chaned with part reduce tasks
    maps = (celery.chain(tasks.map.s(x), tasks.part_reduce.s()) for x in data)
    # Create global reduce task
    mapreducer = celery.chord(maps)(tasks.part_reduce.s())

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
    mapper = celery.result.GroupResult.restore(mapper_id)
    # mapper = celery.result.result_from_tuple([[mapper_id, None], [[task, None] for task in mapper]])
    completed = mapper.completed_count()
    
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
    mapper_id, reducer_id = create_mr(elements_count=10000,chunk_size=100)
    print(f"MR task started {reducer_id}")
    results = wait_for_task(mapper_id, reducer_id)
    print(f"MR task execution result {results}")
    
    mapper_id, reducer_id = create_part_mr(elements_count=1000,chunk_size=100)
    print(f"Part MR task started {reducer_id}")
    results = wait_for_task(mapper_id, reducer_id)
    print(f"Part MR task execution result {results}")