#!/usr/bin/python3.5

import sys
import time
import threading
from couchbase.bucket import Bucket

# class cb_loader(threading.Thread):
def cb_loader(_tid, _total_threads, _key_prefix, _key_start, _key_end, _a1_selectivity, _value_size, _connection_string):
    print ("Starting Thread %s" %  _tid)

    #establish connection
    print ("Connecting: ",connection_string)
    b = Bucket(_connection_string)

    for i in range( _key_start, _key_end):
        if (i % _total_threads == _tid):
            t0 = time.clock()
            b.upsert( _key_prefix + str(i),
                {'a1': (i) % _a1_selectivity, 'a2': "Zero".zfill(_value_size)},
                replicate_to=0,
                persist_to=0)
            t1 = time.clock()
            print ("Last execution time in milliseond: %3.3f" % ((t1 - t0) * 1000))


def printhelp():
    print("""
Python load generator for Couchbase Server 4.0 or later. Command line arguments:

Connection parameter
    -hs=host address couchbase://ADDR/BUCKET (ex: -hs=couchbase://localhost/default)

Operation parameter
    -op=operation to perform. Can be set to query, load

Key generation parameters. Applies to load and query operations. All keys get the key prefix (-kp), if 
one is specified. Keys are generated from starting key number (ks) to ending key number (ke). 
    -kp=document key prefix (string)
    -ks=starting key postfix value (int)
    -ke=ending key postfix value (int)

Value generation parameters. Applies to load operation
    -vs=value size in bytes (int)
    -sl=selectivity of a1 attribute in valuet (int) - distinct values for a1 within total items (ke-kb).
        for unique values, set this to the value of ke-kb
        for 2 distinct a1 values, set this to the value of (ke-kb)/2
        and so on.

Query parameters. Applies to query operation.
    -qs=query string. N1QL statement used for query. You can specify one generated value for the query: $1.
        $1 is replaced with the key generation parameters (kp,ks and ke) explained above
    -qi=number of iterations for query execution. specify 0 for looping and any integer for specify the times
        to execute the query.

Samples:
Loading data: The following generates 100 keys from A0 to A100 with a value that has a total of 1024 bytes 
in value with an attribute "a1" that is values (100-0) % 10
    LoadGenCouchbase.py -hs=couchbase://localhost/default -op=load -kp=A -ks=0 -ke=100 -vs=1024 -sl=10

Querying data: The following run the query specified 1000 times with the $1 replaced with values from A0 to 
A100 for a1.
    LoadGenCouchbase.py -hs=couchbase://localhost/default -op=query -qs=select * from default where a1='$1' 
    -kp=A -ks=0 -ke=100 -qi=1000
""")
    return

key_prefix=""
key_start=0
key_end=0
operation=""
total_threads=1

#process commandline arguments
if (len(sys.argv) == 0):
    #no command line option specified - display help
    printhelp()
    raise("No arguments specified.")
elif (len(sys.argv) > 0):
    for arg in sys.argv:
        #splitter based on platform
        argsplit = arg.split("=")

        #read commandline args
        if (argsplit[0] == "-op"):
            #connection string
            operation = str(argsplit[1])
            continue
        elif (argsplit[0] == "-qs"):
            #query string
            if (len(argsplit)>2):
                query_string = str("=".join(argsplit[1:]))
            else:
                query_string = str(argsplit[1])
            continue
        elif (argsplit[0] == "-qi"):
            #query string
            query_iterations = int(argsplit[1])
            continue
        elif (argsplit[0] == "-hn"):
            #connection string
            connection_string = str(argsplit[1])
            continue
        elif (argsplit[0] == "-kp"):
            #key prefix
            key_prefix = str(argsplit[1])
            continue
        elif (argsplit[0] == "-ks"):
            #key starting value 
            key_start = int(argsplit[1])
            continue
        elif (argsplit[0] == "-ke"):
            #key ending value 
            key_end = int(argsplit[1])
            continue
        elif (argsplit[0] == "-vs"):
            #value size 
            value_size = int(argsplit[1])
            continue
        elif (argsplit[0] == "-sl"):
            #selectivity
            a1_selectivity = int(argsplit[1])
            continue
        elif (argsplit[0] == "-tc"):
            #selectivity
            total_threads = int(argsplit[1])
            continue
        elif ((argsplit[0] == "-h") or (argsplit[0] == "--help")):
            printhelp()
            sys.exit(0)
        # else:
        #     print("Invalid argument: {}", arg)
        #     printhelp()

if (operation == "load"):
    print ("STARTING: inserting total items: " + str(key_end-key_start))
    if (total_threads > 1):
        cb_loader_threads = []
        for i in range(total_threads):
            cb_loader_threads.append(
                threading.Thread(target = cb_loader, 
                    args = (i, total_threads, key_prefix, key_start, key_end, a1_selectivity, value_size, connection_string, )
                    )
                )
        for j in cb_loader_threads:
            j.start()

        for k in cb_loader_threads:
            k.join()
    else:
        #establish connection
        print ("Connecting: ",connection_string)
        b = Bucket(connection_string)

        for i in range(key_start,key_end):
            t0 = time.clock()
            b.upsert(key_prefix + str(i),{'a1': i % a1_selectivity, 'a2': "Zero".zfill(value_size)},
                replicate_to=0,
                persist_to=0)
            t1 = time.clock()
            print ("Last execution time in milliseond: %3.3f" % ((t1 - t0) * 1000))
    print ("DONE: inserted total items: " + str(key_end-key_start))

elif (operation == "query"):
    print ("STARTING: querying : " + str(query_string))
    for i in range(query_iterations):
        query_valued = query_string.replace("$1",key_prefix + str((key_start + i) % key_end))
        t0 = time.clock()
        for row in b.n1ql_query(query_valued):
            # print (row)
            pass
        # b.query(query_string.replace("$1",key_prefix + str((key_start + i) % key_end)))
        t1 = time.clock()
        print ("Last execution time in milliseond: %3.3f" % ((t1 - t0) * 1000))
    print ("DONE: queried total iterations: " + str(query_iterations))

elif (operation == ""):
    print ("No operation argument (-op) specified.")
    printhelp()
    sys.exit()

