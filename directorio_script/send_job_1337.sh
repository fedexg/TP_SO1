#!/bin/bash

# Esperar 2 segundos para dar tiempo a los agentes a iniciar
#sleep 2

MENSAJE="JOB_REQUEST 0 @192.168.100.89:8080:cpu:2 @192.168.100.89:8081:gpu:1"

# Ejecutamos echo y vemos la salida en la consola
echo "Enviando job a puerto 1337..."
echo "$MENSAJE" | nc localhost 1337
echo "Job enviado."