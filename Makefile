CC=gcc
C_OBJ=agente.o
C_OUT=agente
CFLAGS=-Wall -Werror

agente: agente.o
	$(CC) -o $(C_OUT) $(C_OBJ) $(CFLAGS)

agente.o: agente.c
	$(CC) -c $^ $(CFLAGS)

clean:
	-rm $(C_OUT)
	-rm $(C_OBJ)
