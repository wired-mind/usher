# Getting started on Mac

* Install [homebrew](http://brew.sh)

If your brew is old, then:

    brew update
    brew upgrade

Download vertx:

    http://vertx.io/downloads.html

Then:

    brew install maven vert.x groovy gradle

    export GROOVY_HOME=/usr/local/opt/groovy/libexec

## RocksDB

    git clone git@github.com:facebook/rocksdb.git
    cd rocksdb/
    git checkout rocksdb-3.2
    make rocksdbjava
    sudo cp librocksdb.a /usr/local/bin/
    

We'll need to build the library for the target platform and install in that platform's default system library path.


