#!/bin/bash

set -x

rm -rf receiver-hostname.txt

srun -n1 -N1 --cpus-per-task=32 python3 effis-server.py &

while [ ! -f receiver-hostname.txt ]
    do
        sleep 1
    done

srun -n1 -N1 --cpus-per-task=32 python3 app-client.py 1 &
srun -n1 -N1 --cpus-per-task=32 python3 app-client.py 2 &

wait

