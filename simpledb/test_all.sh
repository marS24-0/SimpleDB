#!/bin/bash

echo "-- tests --"
for i in `echo "PredicateTest, JoinPredicateTest, FilterTest, JoinTest, IntegerAggregatorTest, StringAggregatorTest, AggregateTest, HeapPageWriteTest, HeapFileWriteTest, BufferPoolWriteTest, InsertTest" | tr -d ','`
do
    echo "$i" 1>&2
    ant runtest -Dtest=$i
done | egrep '(FAILED|SUCCESSFUL)'

echo
echo "-- system tests --"
for i in `echo "FilterTest, JoinTest, AggregateTest, InsertTest, EvictionTest" | tr -d ','`
do
    echo "$i" 1>&2
    ant runsystest -Dtest=$i
done | egrep '(FAILED|SUCCESSFUL)'

echo
echo "-- jointestexample --"
ant
java -classpath dist/simpledb.jar simpledb.jointestexample

echo
echo "-- parser --"
ant
echo 'select d.f1, d.f2 from data d;' | java -jar dist/simpledb.jar parser catalog.txt
