#!/bin/bash

tar zcvf bubing-deps.tar.gz --owner=0 --group=0 --transform='s|.*/||' $(find jars/runtime -iname \*.jar -not -iname \*javadoc\* -exec readlink {} \;)
