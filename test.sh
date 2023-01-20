#!/bin/bash
echo "init data ..."
./mongodb_init.sh

sed -i 's/REGRESS =.*/REGRESS = server_options connection_validation dml select pushdown column_remapping join_pushdown extra\/aggregates extra\/join extra\/json extra\/jsonb extra\/limit extra\/enhance/' Makefile

make clean
make
make check | tee make_check.out
