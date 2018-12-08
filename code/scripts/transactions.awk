#!/bin/bash

awk -F \| '
($5=="1" || $6=="1"){
    printf("%s|%s|%s\n",$2,$4,$3);}' $* | sort -t \|  >transactions_sorted.txt
