#!/bin/sh

#load 1M items with 100K value size with a1 having 100 distinct values, using 10 threads in parallel 
/usr/local/bin/python3.5 LoadGenCouchbase.py -hn=couchbase://10.0.0.52/default -op=query -qs="select * from default where a1='$1'" -kp=A -ks=0 -ke=100 -qi=1000 -tc=2