# Trabajo práctico Sistemas Operativos I

# Introduccion: Simulador de cluster de computadoras en c y planificador-cliente 
Simula un entorno de trabajo, gestionando recursos de su computadora donde se aloje, como
de las computadoras a las que esté conectado en la misma red. Con el objetivo de simular
un cluster de computadoras. 
 Ademas, tambien simula un entorno planificador-cliente, donde un proceso principal toma el rol
de planificador de pedidos al cluster, y otros procesos secundarios simulan ser clientes que constantemente 
necesitan recursos del cluster.  

# Cómo funciona: 
    1. Agente C: 
        Hace uso de un programa en lenguaje C, que funciona como un agente intermediario
        que pide recursos a su sistema ficticio o a otros agentes c que hayan publicado 
        en la red. (Estos otros agentes deben ser de la misma naturaleza de simulación).
    2. Scheduler/Client Erlang: 
        Hace uso de un programa en leguaje Erlang, para conformar una comunicacion local
        tanto con el Agente C, como con los clientes que el mismo programa simula.
          - El Scheduler se encarga de planificar pedidos entrantes de clientes, y enviar
          solicitudes de trabajos al Agente C. Luego, el resultado de esta comunicacion 
          se la informan al cliente respectivo de cada solicitud.
          - Los clientes se comunican con el Scheduler para pedir recursos del cluster.
          Al terminar de usar sus recursos, avisan al Scheduler que terminaron, para que 
          este pida liberar esos recursos de su uso. 

# Carasterísticas: 
    - Uso de conexiones Epoll para gestionar cientos de conexiones TCP de forma no bloqueante.
    - Muestra una debida implementacion de sistemas distribuidos, conectando multiples nodos
    (computadoras) y procesos independientes de manera coherente y funcional (sin deadlocks).
    - Estrategia de prevencion y manejo de interbloqueos (deadlocks), mediante uso de timeouts.
     
## Compilar el agente de C
```
$ make
```
## Compilar el scheduler/client de Erlang
```
$ make erl
```
## Limpiar archivos
```
$ make clean
```
