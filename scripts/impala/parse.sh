#!/bin/bash

cat $1 | cut -d " " -f 5 | cut -d "s" -f 1
