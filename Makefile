CC=gcc
C_SRC=main.c utils.c types.c resources.c erl.c agents.c \
	  ds/hashmap.c ds/queue.c ds/list.c \
	  log/log.c
C_OBJ=$(C_SRC:.c=.o)
C_OUT=agente
CFLAGS=-Wall -Werror -pthread

default:
	@echo "Error: se debe elegir qué compilar; make agente o make scheduler"

scheduler:
	erlc scheduler.erl
	@erl -noshell -run scheduler start -s init
	
schedulerTEST:
	erlc schedulerTEST.erl
	@erl -noshell -run schedulerTEST start -s init

agente: $(C_OBJ)
	$(CC) -o $(C_OUT) $(CFLAGS) $(C_OBJ)

main.o: main.c
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

erl.o: erl.c
	$(CC) -c $^ $(CFLAGS)

agents.o: agents.c
	$(CC) -c $^ $(CFLAGS)

log.o: log.c
	$(CC) -c $^ $(CFLAGS)

clean:
# Limpear .o
	-rm $(C_OUT)
	-rm $(C_OBJ)
# Limpear .beam
	rm -f *.beam