CC=gcc
C_SRC=agente.c ds/hashmap.c ds/queue.c
C_OBJ=$(C_SRC:.c=.o)
C_OUT=agente
CFLAGS=-Wall -Werror

default: $(C_OBJ)
	$(CC) -o $(C_OUT) $(CFLAGS) $(C_OBJ)

agente.o: agente.c
	$(CC) -c $^ $(CFLAGS)

ds/hashmap.o: ds/hashmap.c
	$(CC) -c $^ $(CFLAGS) -o ds/hashmap.o

ds/queue.o: ds/queue.c
	$(CC) -c $^ $(CFLAGS) -o ds/queue.o

clean:
	-rm $(C_OUT)
	-rm $(C_OBJ)
