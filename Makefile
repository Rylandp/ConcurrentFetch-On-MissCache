CC = gcc
CFLAGS = -std=c11 -Wall -Wextra -Werror -g -O2 -pthread
LDFLAGS = -pthread

OBJS = main.o trace.o backing.o pool.o index.o cache.o

all: cachelab

cachelab: $(OBJS)
	$(CC) $(CFLAGS) -o cachelab $(OBJS) $(LDFLAGS)

main.o: main.c cachelab.h
	$(CC) $(CFLAGS) -c main.c

trace.o: trace.c cachelab.h
	$(CC) $(CFLAGS) -c trace.c

backing.o: backing.c cachelab.h
	$(CC) $(CFLAGS) -c backing.c

pool.o: pool.c cachelab.h
	$(CC) $(CFLAGS) -c pool.c

index.o: index.c cachelab.h
	$(CC) $(CFLAGS) -c index.c

cache.o: cache.c cachelab.h
	$(CC) $(CFLAGS) -c cache.c

test: cachelab
	python3 tests/check_p3.py

test-stress: cachelab
	python3 tests/check_p3.py --stress

test-helgrind: cachelab
	valgrind --tool=helgrind --error-exitcode=1 ./cachelab 1024 test_tr/stampede.trace 4 -f 2 -a first -e clock --barrier-start --jitter
	valgrind --tool=helgrind --error-exitcode=1 ./cachelab 1024 test_tr/stress.trace 4 -f 2 -a best -e clock --jitter

test-valgrind: cachelab
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./cachelab 1024 test_tr/stress.trace 4 -f 2 -a best -e clock --jitter
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./cachelab 1024 test_tr/shutdown.trace 4 -f 2 -a first -e fifo --barrier-start

clean:
	rm -f $(OBJS) cachelab

.PHONY: all test test-stress test-helgrind test-valgrind clean
