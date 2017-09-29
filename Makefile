ifdef config
include $(config)
endif

include make/ps.mk

ifndef CXX
CXX = g++
endif

ifndef DEPS_PATH
DEPS_PATH = $(shell pwd)/deps
endif


ifndef PROTOC
PROTOC = ${DEPS_PATH}/bin/protoc
endif

ifndef FLATC
FLATC = ${DEPS_PATH}/bin/flatc
endif

INCPATH = -I$(DEPS_PATH)/include -I./src -I./include 
CFLAGS = $(INCPATH) -std=c++11 -msse2 -fPIC -O3 -ggdb -Wall -finline-functions $(ADD_CFLAGS)

all: ps test

include make/deps.mk

clean:
	rm -rf build $(TEST) tests/*.d
	find src -name "*.pb.[ch]*" -delete

lint:
	python tests/lint.py ps all include/ps src

ps: build/libps.a

OBJS = $(addprefix build/, customer.o postoffice.o van.o meta.pb.o)
build/libps.a: $(OBJS)
	ar crv $@ $(filter %.o, $?)

build/%.o: src/%.cc ${ZMQ} src/meta.pb.h src/test_generated.h
	@mkdir -p $(@D)
	$(CXX) $(INCPATH) -std=c++0x -MM -MT build/$*.o $< >build/$*.d
	$(CXX) $(CFLAGS) -c $< -o $@

src/%.pb.cc src/%.pb.h : src/%.proto ${PROTOBUF}
	$(PROTOC) --cpp_out=./src --proto_path=./src $<

src/%_generated.h : src/%.fbs ${FLATBUF}
	$(FLATC) -c -o ./src $<

-include build/*.d
-include build/*/*.d

include tests/test.mk
test: $(TEST)
