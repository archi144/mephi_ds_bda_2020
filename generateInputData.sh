#!/bin/bash
if [ $# -ne 1 ]
then
echo "Please enter only one parameter - number of generated words"
else
word_count=$1
BINARY=2
echo "Number of generated words is $1"
echo "==============================="
for ((i=0;i<$word_count;i++)) {
char_count=$RANDOM
ASCII=$RANDOM
let "ASCII %= $BINARY"
word=""
if [ $ASCII -eq 1 ]
then
word=$(head /dev/urandom | tr -dc A-Za-z | head -c$char_count)
else
word=$(head /dev/urandom | head -c$char_count)
fi
echo "$word" >> input/log.txt
}
fi