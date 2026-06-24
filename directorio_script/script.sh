#    ---- Ejemplo del Enunciado del TP: ---- 
#
# realiza dos .sh -> 1. habre el Agente C, que posee localmente al Nodo A.
#                 -> 2. habre el Agente C, que posee localmente al Nodo B.
# Con dos nodos: 
# -  Nodo A: 2 CPUs, 8 GB RAM, 0 GPU. 
# -  Nodo B: 2 CPUs, 4 GB RAM, 1 GPU. 
# Jobs: 
# -  Job1 (desde A): necesita 2 CPUs de A y 1 GPU de B. 
# -  Job2 (desde B): necesita 1 GPU de B y 2 CPUs de A
#
# durante la ejecucion de este programa, se conecta simultaneamente al puerto 1337 y 1338 (localmente) de los
# sockets de escucha especializados para el planificador de ambos Agentes C.
# Luego, enviamos una cadena "JOB_REQUEST <NodoA_IP>:<NodoA_PORT>:cpu:2:ram:0:gpu:0 <NodoB_IP>:<NodoB_PORT>:cpu:0:ram:0:gpu:1\n"




JOB_REQUEST @192.168.100.89:<NodoA_PORT>:cpu:2:ram:0:gpu:0