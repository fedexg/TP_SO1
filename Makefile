CC=gcc
C_SRC=agente.c utils.c types.c resources.c \
	  ds/hashmap.c ds/queue.c ds/list.c
C_OBJ=$(C_SRC:.c=.o)
C_OUT=agente
CFLAGS=-Wall -Werror -pthread

default: $(C_OBJ)
	$(CC) -o $(C_OUT) $(CFLAGS) $(C_OBJ)

agente.o: agente.c
	$(CC) -c $^ $(CFLAGS)

ds/hashmap.o: ds/hashmap.c
	$(CC) -c $^ $(CFLAGS) -o ds/hashmap.o

ds/queue.o: ds/queue.c
	$(CC) -c $^ $(CFLAGS) -o ds/queue.o

ds/list.o: ds/list.c
	$(CC) -c $^ $(CFLAGS) -o ds/list.o

utils.o: utils.c
	$(CC) -c $^ $(CFLAGS)

types.o: types.c
	$(CC) -c $^ $(CFLAGS)

resources.o: resources.c
	$(CC) -c $^ $(CFLAGS)

clean:
	-rm $(C_OUT)
	-rm $(C_OBJ)
