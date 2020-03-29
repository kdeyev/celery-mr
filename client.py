import time
import celery

import tasks

from toolz.itertoolz import partition_all, concat
import random

app = tasks.app

def generate_data(elements_count, chunk_size):
    # Generate input data data
    data = [(chunk, random.randrange(10000)) for chunk in range(elements_count)]
    data = partition_all(chunk_size, data)
    return data
    
def run_mr(data):
    """ Map-reduce task where:
    1. Map tasks are running in parallel
    2. Celery aggregate the mappers results
    3. Single reduce task receives whole set of mappers results
    4. Client receives reduced result from the reducer
    """
    # Cretate map tasks
    maps = (tasks.map.s(x) for x in data)
    
    # Cretate reduce tasks
    mapreducer = celery.chord(maps)(tasks.reduce.s())

    mapper = mapreducer.parent
    reducer = mapreducer
    
    # Required for celery.result.GroupResult.restore
    mapper.save()
    
    return (mapper.id, reducer.id)

def run_partial_mr(data):
    """ Map-reduce task with local reduce step where:
    1. Map tasks are running in parallel
    2. Local reducers are running in parallel 
    3. Local reducers are receving part of mappers results
    4. Celery aggregate the local reducers results
    5. Single reduce task receives partly reduced (by local reducers) data
    6. Client receives reduced result from the global reducer
    """
    # Cretate map tasks chaned with partial reduce tasks
    maps = (celery.chain(tasks.map.s(x), tasks.partial_reduce.s()) for x in data)
    # Create global reduce task
    mapreducer = celery.chord(maps)(tasks.partial_reduce.s())

    mapper = mapreducer.parent
    reducer = mapreducer
    
    # required for celery.result.GroupResult.restore
    mapper.save()
    
    return (mapper.id, reducer.id)

def run_db_mr(data):
    """ Map-reduce task with db-backed reduce step where:
    1. Map tasks are running in parallel
    2. Db-backed reduce steps are running in parallel 
    3. Db-backed reduce are aggregating mappers results in DB
    4. Celery aggregates nothing
    5. Client receives the DB-stored reduced result
    """
    
    # Cretate map tasks chaned with DB-backed reduce tasks
    maps = (celery.chain(tasks.map.s(x), tasks.db_reduce.s()) for x in data)
    # Create global reduce task
    mapreducer = celery.chord(maps)(tasks.get_db_result.s())

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
    data = list(generate_data(100000, 100))
    
    mapper_id, reducer_id = run_mr(data)
    print(f"MR task started {reducer_id}")
    results = wait_for_task(mapper_id, reducer_id)
    print(f"MR task execution result {results}")
    
    mapper_id, reducer_id = run_partial_mr(data)
    print(f"Partial MR task started {reducer_id}")
    results = wait_for_task(mapper_id, reducer_id)
    print(f"Partial MR task execution result {results}")
    
    mapper_id, reducer_id = run_db_mr(data)
    print(f"DB-backed MR task started {reducer_id}")
    results = wait_for_task(mapper_id, reducer_id)
    print(f"DB-backed MR task execution result {results}")