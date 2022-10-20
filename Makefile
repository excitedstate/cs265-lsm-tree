BOOST_DIR=/usr/local
BOOST_FLAGS=-I$(BOOST_DIR) -L$(BOOST_DIR)/lib
GSL_FLAGS=-L /opt/homebrew/lib -I/opt/homebrew/include -lgsl -lgslcblas

all: build

build:
	g++ src/*.cpp -o bin/lsm -std=c++11 -I./lib $(BOOST_FLAGS) -l boost_system -g

generator:
	gcc generator.c -o generator $(GSL_FLAGS) -g

clean:
	rm bin/lsm bin/generator
