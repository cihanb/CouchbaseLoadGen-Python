#!/usr/bin/python3.5

import sys
import time
from couchbase.bucket import Bucket

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


total_items=0
key_prefix=""
key_start=0
key_end=0
operation=""

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
        elif ((argsplit[0] == "-h") or (argsplit[0] == "--help")):
            printhelp()
            sys.exit(0)
        # else:
        #     print("Invalid argument: {}", arg)
        #     printhelp()
            
    total_items=key_end-key_start

#establish connection
print ("Connecting: ",connection_string)
b = Bucket(connection_string)

if (operation == "load"):
    print ("STARTING: inserting total items: " + str(total_items))
    for i in range(total_items):
        t0 = time.clock()
        b.upsert(key_prefix + str(key_start + i),{'a1': (key_start + i) % a1_selectivity, 'a2': "Zero".zfill(value_size)},
            replicate_to=0,
            persist_to=0)
        t1 = time.clock()
        print ("Last execution time in milliseond: %3.3f" % ((t1 - t0) * 1000))
    print ("DONE: inserted total items: " + str(total_items))

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
