#!/bin/bash

if [ -f out/Blog6_image ]; then
    echo "Using native image 'out/Blog6_image'" 1>&2
    out/Blog6_image
else
    JAVA_OPTS="--enable-preview"
    echo "Native image not found, using JVM mode." 1>&2
    java $JAVA_OPTS -cp src Blog6
fi
