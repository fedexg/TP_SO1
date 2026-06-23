# Trabajo práctico Sistemas Operativos I

## Introducción: simulador de cluster de computadoras en C y planificador-cliente
Simula un entorno de trabajo, gestionando recursos de la computadora donde se aloje, como
de las computadoras a las que esté conectado en la misma red con el objetivo de simular
un cluster.

Además, simula un entorno planificador-cliente, donde un proceso principal toma el rol
de planificador de pedidos al cluster, y otros procesos secundarios simulan ser clientes
que constantemente necesitan recursos.

## Cómo funciona
### Agente C

Funciona como un agente intermediario que pide recursos a su
sistema ficticio o a otros agentes C que estén en la red. A su vez,
actúa de servidor esperando respuestas de los demás agentes.

### Scheduler/Cliente Erlang

Hace uso de un programa en lenguaje Erlang para conformar una comunicación local
tanto con el agente C, como con los clientes que el mismo programa simula.

- El scheduler se encarga de planificar pedidos entrantes de clientes, y enviar
solicitudes de trabajos al agente C. Luego, el resultado de esta comunicación
se la informa al cliente respectivo de cada solicitud.

- Los clientes se comunican con el scheduler para pedir recursos del cluster.
Al terminar de usar sus recursos, avisan al scheduler que terminaron, para que
este pueda liberar esos recursos.

## Características
- Uso de conexiones epoll para gestionar un número ilimitado de conexiones TCP de forma no bloqueante;
- Conexión de múltiples nodos y procesos independientes;
- Estrategia de prevención y manejo de deadlocks mediante uso de timeouts.
- Concurrencia en el manejo simulado de solicitudes de trabajo.
     
## Cómo usarlo
```sh
$ git clone https://github.com/fedexg/TP_SO1
$ cd TP_SO1/
$ make agente
```

En una terminal, ejecutar el agente
```sh
$ ./agente <PORT>
```
PORT es opcional, y el agente brindará el puerto 8080.

Y en otra terminal, ejecutar el scheduler
```sh
$ make scheduler
```

### Limpiar archivos generados
```
$ make clean
```