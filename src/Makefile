CC = g++
CFLAGS = -std=c++11 -c -Wall
LDFLAGS = 
SOURCES = newOrder.cpp
OBJECTS = $(SOURCES:.cpp=.o)
EXECUTABLE = newOrder

all: $(SOURCES) $(EXECUTABLE)
	
$(EXECUTABLE): $(OBJECTS) 
	$(CC) $(LDFLAGS) $(OBJECTS) -o $@

.cpp.o:
	$(CC) $(CFLAGS) $< -o $@
