1. Run broker/backend
Redis
```sh
docker run --name some-redis -d redis
```
Mongo
```sh
docker run --name some-mongo -d mongo
```
2. Install/run Flower (optional)
Install: flower
```sh
pip install flower
```
and run with your brocker: \<brocker\> = redis or mongodb 
```sh
flower --port=5555 --broker=<brocker>://localhost
```
open http://localhost:5555 in browser

3. Install celery
```sh
pip install celery
```
4. Run worker
```sh
python worker.py
```
Pay attention: backend is herdcoded in tasks.py
5. Run client:
```sh
python client.py
```
6. Have fun