#!/bin/sh
cat > /dev/null
echo $$ > sh.pid
sleep 60 &
echo $! > child.pid
yes 'a'
