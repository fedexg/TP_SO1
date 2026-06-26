#    ---- Ejemplo del Enunciado del TP: ---- 
#
# realiza dos .sh -> 1. habre el Agente C, que posee localmente al Nodo A.
#                 -> 2. habre el Agente C, que posee localmente al Nodo B.
# Con dos nodos: 
# -  Nodo A: 2 CPUs, 0 GPU, 8 GB RAM. Con planificador conectado al puerto 1337 
# -  Nodo B: 2 CPUs, 1 GPU, 4 GB RAM. Con planificador conectado al puerto 1338
# Jobs: 
# -  Job1 (desde A): necesita 2 CPUs de A y 1 GPU de B. 
# -  Job2 (desde B): necesita 1 GPU de B y 2 CPUs de A
# 
# 
#
# durante la ejecucion de este programa, se conecta simultaneamente al puerto 1337 y 1338 (localmente) de los
# sockets de escucha especializados para el planificador de ambos Agentes C.
# Luego, enviamos una cadena "JOB_REQUEST <JOB_ID> <NodoA_IP>:<NodoA_PORT>:cpu:2 <NodoB_IP>:<NodoB_PORT>:gpu:1\n"




# Hacer un scheduler_for_node1
# Hacer un scheduler_for_node2
# lo que envian los schedulers a sus respectivos nodos: 
# "JOB_REQUEST 0 @192.168.100.89:8080:cpu:2 @192.168.100.89:8081:gpu:1\n"
# Lo enviado es enviado casi simultaneamente a ambos Agentes 

# make agente
# ./agente 8080 1337 2:0:8
# ./agente 8081 1338 2:1:4

# Compilamos todo
make -C .. agente

# Damos permisos de ejecución a los archivos (por seguridad)
chmod +x ./send_job_1337.sh ./send_job_1338.sh ./run_agent_A.sh ./run_agent_B.sh

# Preparar entorno
mkdir -p logs
chmod +x run_*.sh

# Lanzar procesos en segundo plano y guardamos su PID en el archivo
./run_agent_A.sh &
./run_agent_B.sh &
sleep 0.5
./send_job_1337.sh &
./send_job_1338.sh &
echo "Procesos iniciados."
echo "PIDs guardados en $PID_FILE"
echo "Monitoreando logs..."
echo "Ingrese "end" o "quit" para finalizar la prueba"
echo "-----------------------------------------------"

# Bucle que espera la cadena "end" o "quit" por pantalla
# para terminar los procesos.
while true; do
    read -p "> " comando
    case $comando in
        "end"|"quit")
            echo "Deteniendo agentes y listeners..."
            
            # Usamos pkill -f para asegurar que matamos todo el grupo de procesos
            # El -f busca por el nombre completo del comando ejecutado
            pkill -f "./agente"
            pkill -f "nc -l 1337"
            pkill -f "nc -l 1338"
            
            echo "Limpieza completada. Fin"
            exit 0
            ;;
        *)
            echo "Comando no reconocido. Usa 'end' o 'quit' para salir."
            ;;
    esac
done

