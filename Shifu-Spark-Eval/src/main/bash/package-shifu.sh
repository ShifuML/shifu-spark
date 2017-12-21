#!/usr/bin/env bash
if [ ! -d "./shifu" ]; then
    git clone -b develop git@github.com:ShifuML/shifu.git
    cd shifu
    mvn -DskipTests clean install
fi
