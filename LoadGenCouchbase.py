#!/usr/bin/python

from couchbase.bucket import Bucket
import sys
from datetime import datetime
import timeit
import time

def printhelp():
    if (sys.platform=="win32"):
        print("""
        Python load generator. command line arguments
        Connection parameter
            -hs=host address couchbase://ADDR/BUCKET
        Key generation parameters
            -kp=document key prefix (string)
            -ks=starting key postfix value (int)
            -ke=ending key postfix value (int)
        Value generation parameters
            -vs=value size in bytes (int)
            -sl=selectivity of a1 attribute in valuet (int) - distinct values for a1 within total items (ke-kb).
                for unique values, set this to the value of ke-kb
                for 2 identical a1 values, set this to the value of (ke-kb)/2 
       Samples:
        The following generates 100 keys from A0 to A100 with a value that has a total of 1024 bytes 
        in value with an attribute "a1" that is values (100-0) % 10
            LoadGenCouchbase.py -hs=couchbase://localhost/default -kp=A -ks=0 -ke=100 -vs=1024 -sl=10
        """)
        return
    else:
        print("""
        Python load generator. command line arguments
        Connection parameter
            -hs host address couchbase://ADDR/BUCKET
        Key generation parameters
            -kp document key prefix (string)
            -ks starting key postfix value (int)
            -ke ending key postfix value (int)
        Value generation parameters
            -vs value size in bytes (int)
            -sl selectivity of a1 attribute in valuet (int) - distinct values for a1 within total items (ke-kb).
                for unique values, set this to the value of ke-kb
                for 2 identical a1 values, set this to the value of (ke-kb)/2 
        Samples:
        The following generates 100 keys from A0 to A100 with a value that has a total of 1024 bytes 
        in value with an attribute "a1" that is values (100-0) % 10
            LoadGenCouchbase.py -hs couchbase://localhost/default -kp A -ks 0 -ke 100 -vs 1024 -sl 10
        """)
        return


total_items=0

#process commandline arguments
if (len(sys.argv) == 0):
    #no command line option specified - display help
    printhelp()
    raise("No arguments specified.")
elif (len(sys.argv) > 0):
    for arg in sys.argv:
        if (sys.platform=="win32"):
            argsplit = arg.split("=")
        else:
            argsplit = arg.split(" ")

        if (argsplit[0] == "-hs"):
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
        elif ((sys.argv[0] == "-h") or (sys.argv[0] == "-help")):
            printhelp()
            raise()
        # else:
        #     print("Invalid argument: {}", arg)
        #     printhelp()
            
    total_items=key_end-key_start

#establish connection
print ("Connecting: ",connection_string)
b = Bucket(connection_string)

print ("STARTING: inserting total items: " + str(total_items))
for i in range(total_items):
    t0 = time.clock()
    b.upsert(key_prefix + str(key_start + i),{'a1': (key_start + i) % a1_selectivity, 'a2': "Zero".zfill(value_size)},
        replicate_to=0,
        persist_to=0)
    t1 = time.clock()
    print ("Last execution time in milliseond: %3.3f" % ((t1 - t0) * 1000))

print ("DONE: inserted total items: " + str(total_items))
