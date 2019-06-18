a implement of redlock

usage:

	from redlock import redlock

	#init
    lock = relock.Redlock({"host": "localhost", "port": 6379, "db": 0},)

    #lock
    ttl, key, value = lock.lock("testkey",30000)

    #unlock
    lock.unlock(key, value)

