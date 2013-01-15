MiniMum Queue for Redis Zset
============================

## environ

```
export MMQRZ_REDIS_HOST='localhost'
export MMQRZ_REDIS_PORT=6379
export MMQRZ_REDIS_DB=0
```

## image

```
        +-------+
        | Mmqrz |
        +---+---+
            |
    +-------+-------+
    |               |
+---+---+       +---+---+
| Queue |       | Queue |
+---+---+       +---+---+
    |               |
    |          +----+----+
    |          |         |
+---+---+  +---+---+ +---+---+
| Task  |  | Task  | | Task  |
+-------+  +-------+ +-------+
```


## mmqrz-cli

CLI interface for mmqrz

### commands

```
mmqrz-cli qinit
```

initialize queue table and remove all exists queues.


```
mmqrz-cli qadd {queue} [--score=10]
```

create queue


```
mmqrz-cli qrem {queue}
```

remove queue


```
mmqrz-cli enqueue {queue} {task}
```

register task to queue

task stores with UNIXTIME


```
mmqrz-cli dequeue {queue}
```

dequeue task from queue


```
mmqrz-cli select
```

dequeue task from any queue


```
mmqrz-cli info
```

show all queues and tasks


## example

```
$ mmqrz-cli qinit
$ mmqrz-cli qadd fruits
$ mmqrz-cli enqueue apple
$ mmqrz-cli enqueue banana
$ mmqrz-cli enqueue orange
$ mmqrz-cli info
fruits : 10.0 (3 tasks)
 - apple : 1358233522.0
 - banana : 1358233524.0
 - orange : 1358233526.0
```
