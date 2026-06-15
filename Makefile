CC=gcc
C_OBJ=agente.o
C_OUT=agente

agente: agente.o
	$(CC) -o $(C_OUT) $(C_OBJ)

agente.o: agente.c
	$(CC) -c $^

clean:
	-rm $(C_OUT)
	-rm $(C_OBJ)
