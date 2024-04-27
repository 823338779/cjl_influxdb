#!/bin/bash
timestamp=$(date +%s%N)


curl -XPOST "http://localhost:8086/write?db=mydb" \
-d 'cpu,host=server01,region=uswest load=42 '"${timestamp}"
#
#curl -XPOST "http://localhost:8086/write?db=mydb" \
#-d 'cpu,host=server02,region=uswest load=78 '"${timestamp}"
#
#curl -XPOST "http://localhost:8086/write?db=mydb" \
#-d 'cpu,host=server03,region=useast load=15.4 '"${timestamp}"

#curl -XPOST "http://localhost:8086/write?db=mydb" \
#-d 'cpu,host=server03,region=useast load=777.7,name="cjl" '"${timestamp}"
