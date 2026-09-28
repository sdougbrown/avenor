#!/bin/sh
cat > /dev/null
sleep 60 &
echo $! > child.pid
wait
