#!/usr/bin/python

from couchbase.bucket import Bucket
import sys
import time

def printhelp():
    if (sys.platform=="win32"):
        print("""
        python load generator. command line arguments
        connection
            -hs:host address couchbase://ADDR/BUCKET
        key generation
            -kp:document key prefix (string)
            -ks:starting key postfix value (int)
            -ke:ending key postfix value (int)
        value generation
            -vs:value size in bytes (int)
            -sl:selectivity of a1 attribute in valuet (int) - distinct values for a1 within total items (ke-kb).
                for unique values, set this to the value of ke-kb
                for 2 identical a1 values, set this to the value of (ke-kb)/2 
        """)
        return
    else:
        print("""
        python load generator. command line arguments
        connection
            -hs host address couchbase://ADDR/BUCKET
        key generation
            -kp document key prefix (string)
            -ks starting key postfix value (int)
            -ke ending key postfix value (int)
        value generation
            -vs value size in bytes (int)
            -sl selectivity of a1 attribute in valuet (int) - distinct values for a1 within total items (ke-kb).
                for unique values, set this to the value of ke-kb
                for 2 identical a1 values, set this to the value of (ke-kb)/2 
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

print ("inserting total items: " + str(total_items))
for i in range(total_items):
    t0 = time.time()
    b.upsert(key_prefix + str(key_start + i),{'a1': (key_start + i) % a1_selectivity, 'a2': "Zero".zfill(value_size)},
        replicate_to=0,
        persist_to=0)
    print ("total time in milliseconds: %2.3f" % ((time.time() - t0)*1000))

print ("done!")
