

all: followship_ops.o
	gcc followship_ops.o -bundle -flat_namespace -undefined suppress -I/users/mike/apps/include/postgresql/server -o followship_ops.so

followship_ops.o: followship_ops.c
	gcc followship_ops.c -c  -I/users/mike/apps/include/postgresql/server

clean:
	rm followship_ops.o followship_ops.so
