#!/bin/sh

# load 1M items with 100K value size with a1 having 100 distinct values, using 10 threads in parallel 
/usr/local/bin/python3.5 LoadGenCouchbase.py -hn=couchbase://localhost/default -op=load -kp=A -ks=0 -ke=2000000 -vs=100 -sl=100 -tc=10
