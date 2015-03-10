FROM wiredmindlabs/vertx-with-repos:2.1.15
MAINTAINER Chuck Swanberg <cswanberg@mad-swan.com>


WORKDIR /data
ADD https://github.com/facebook/rocksdb/archive/rocksdb-3.2.zip /data/rocksdb-3.2.zip
RUN unzip rocksdb-3.2.zip
WORKDIR /data/rocksdb-rocksdb-3.2/

RUN make rocksdbjava
RUN cp ./java/librocksdbjni.so /usr/lib/librocksdbjni.so
ADD echo_example.js /data/echo_example.js
EXPOSE 2500
CMD ["run", "/data/echo_example.js"]
