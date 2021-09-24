CC = g++
CFLAG = -std=c++11 -O2

objdir = .obj/

exedir = bin/

$(exedir)connect: $(objdir)connect.o $(objdir)client.o $(objdir)joiner.o
	$(CC) $(CFLAG) -lcurl -lpthread $(objdir)connect.o $(objdir)client.o $(objdir)joiner.o -o $(exedir)connect

$(objdir)connect.o:connect.cpp
	$(CC) $(CFLAG) -c connect.cpp -o $(objdir)connect.o

$(objdir)client.o:client.cpp
	$(CC) $(CFLAG) -c client.cpp -o $(objdir)client.o

$(objdir)joiner.o:joiner.cpp
	$(CC) $(CFLAG) -c joiner.cpp -o $(objdir)joiner.o

clean:
	rm -f $(objdir)*.o
	rm -f $(exedir)connect
