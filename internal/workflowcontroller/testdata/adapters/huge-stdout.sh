#!/bin/sh
cat > /dev/null
head -c 2097152 /dev/zero | tr '\0' 'a'
