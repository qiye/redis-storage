# Top level makefile, the real shit is at src/Makefile

default: all

.DEFAULT:
	cd deps/lua && $(MAKE) $@
	cd deps/hiredis && $(MAKE) $@
	cd deps/linenoise && $(MAKE)
	cd src && $(MAKE) $@

init:
	cd deps/gperftools-2.0 && ./configure --enable-minimal --enable-frame-pointers && make
	cd deps && tar xzvf snappy-1.1.0.tar.gz && cd snappy-1.1.0 && ./configure && make
	rm -rf /usr/local/lib/libsnappy.so.1
	cp ./deps/snappy-1.1.0/.libs/libsnappy.so.1 /usr/local/lib
	ldconfig
	chmod +x deps/leveldb-1.10.0/build_detect_platform
	cd deps/leveldb-1.10.0 && make
	rm -rf /usr/local/lib/libleveldb.so.1
	cp ./deps/leveldb-1.10.0/libleveldb.so.1 /usr/local/lib
	ldconfig
	
install:
	mkdir -p $(PREFIX)/db
	mkdir -p $(PREFIX)/bin
	mkdir -p $(PREFIX)/log
	mkdir -p $(PREFIX)/conf
	cp src/redis-server $(PREFIX)/bin
	cp src/redis-cli $(PREFIX)/bin
	cp src/redis-check-dump $(PREFIX)/bin
	cp src/redis-sentinel $(PREFIX)/bin
	cp src/redis-benchmark $(PREFIX)/bin
	cp src/redis-check-aof $(PREFIX)/bin
	cp redis.conf $(PREFIX)/conf
    
           
.PHONY: install
