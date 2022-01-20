#!/bin/bash


dir="MPC2_watdiv100M /data12/home/lcj/MPC2_100M_query/query/round0/r0"
for ((p = 1; p <= 20; p++))
do
    for ((q = 1; q <= 5;q++))
    do 
        query=$dir"_p"$p"_q"$q"_dir"
        # echo $query
        ./bin/connect $query
    done
done