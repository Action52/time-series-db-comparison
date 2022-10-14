%.o: %.C
	/usr/bin/gcc -g -c $<

tickgen: cal.o Time.o tickgen.o
	/usr/bin/gcc -lstdc++ -g -o tickgen cal.o Time.o tickgen.o

histgen: cal.o histgen.o 
	/usr/bin/gcc -lstdc++ -g -o histgen cal.o histgen.o

timetest: Time.o timetest.o
	/usr/bin/gcc -lstdc++ -g -o timetest Time.o timetest.o

all: histgen tickgen
