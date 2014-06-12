#!/bin/bash

export CFLAGS="-Wall -Wextra -Wdeclaration-after-statement -Wmissing-field-initializers -Wshadow -Wno-unused-parameter -ggdb3"
phpize && ./configure --enable-kafka && make clean && make


# -j 5
#all
#&& make install