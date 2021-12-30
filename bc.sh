#!/bin/sh

#bot parametrs:
#ref-key, fut-key

#run bot on 3 processor core
taskset -c 3 ./bot 86340 ref_3 futures_quote_3

