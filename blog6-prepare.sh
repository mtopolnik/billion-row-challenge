#!/bin/bash

source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk use java 21.0.2-graal 1>&2

if [ ! -f out/Blog6_image ]; then
    NATIVE_IMAGE_OPTS="--gc=epsilon -O3 -H:+UnlockExperimentalVMOptions -H:-GenLoopSafepoints -march=native --enable-preview -H:InlineAllBonus=10 -H:-ParseRuntimeOptions --initialize-at-build-time=Blog6"
    native-image $NATIVE_IMAGE_OPTS -o out/Blog6_image Blog6
fi
