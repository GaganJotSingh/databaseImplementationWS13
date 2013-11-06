CXX:=g++
CXXFLAGS:=-std=c++0x -g -O2 -Wall

all: parser1

Schema.o: Schema.cpp Schema.hpp Makefile
	$(CXX) $(CXXFLAGS) -c Schema.cpp -o Schema.o

Parser.o: Parser.cpp Parser.hpp Makefile
	$(CXX) $(CXXFLAGS) -c Parser.cpp -o Parser.o

parser: Schema.o Parser.o Schema.hpp Types.hpp parser1.cpp Makefile
	$(CXX) $(CXXFLAGS) Parser.o Schema.o parser1.cpp -o parser1

clean:
	rm -rf *.o parser1
