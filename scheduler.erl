-module(scheduler).
-export([start/0]).

-export([start_scheduler/0, job_handler/5]).
-define(PORT,1337).

% Inicia el iniciador del Scheduler y el simulador de cliente 
% %
start() ->
    Scheduler = spawn(?MODULE, start_scheduler, []),
    client_simulator(Scheduler).
% %

% Crea la comunicacion con el Agente C creando un socket tcp 
% local conectado al Agente C. Ademas procede a iniciar el scheluder
% %
start_scheduler() ->
    {ok, Socket} = gen_tcp:connect("localhost",?PORT),
    Message_Manager = spawn(?MODULE, message_manager, []),
    scheduler_loop(Socket, queue:new(), maps:new(), 1000, false, Message_Manager).
% %

% Funcion pricipal donde el proceso SCHEDULER hace el trabajo
% de administrador de pedidos de trabajo de clientes simulados
% %
scheduler_loop(Socket, Job_Queue, Client_Map, N) ->
    Nodes_Info = request_nodes_info(Socket),            % <- Cada nuevo loop, le pregunta al Agente C que opciones tiene para otorgar a los clientes. 
    case queue:is_empty(Job_Queue) of                   
        % La cola de JOBS tiene elementos: atiende el JOB que desencola. 
        false -> 
            {Queue_Head, New_Job_Queue} = queue:out(Job_Queue),             % <- La cola guarda tuplas de la forma {ID,INFO}, JOBs
            {value, {Job_Id, Job_Info}} = Queue_Head,
            spawn(?MODULE, job_handler, [Socket, Nodes_Info, Job_Id, Job_Info, Client_Map]),
            New_Client_Map = maps:remove(Job_Id, Client_Map),                       % <- Saca el JOB atendido
            scheduler_loop(Socket, New_Job_Queue, New_Client_Map, N);               
        _ -> receive
                    {new_job, Client_Id, Job_Info_Recv} -> 
                        New_Job_Queue = queue:in({N, Job_Info_Recv},Job_Queue),     % <- Encola el JOB con su id unico.
                        New_Client_Map = maps:add(N, Client_Id, Client_Map),        % <- Agrega en el diccionario el JOB asignado al cliente.
                        Client_Id ! {given_jobid, N},                               % <- Confirma al cliente el almacenamiento de su pedido a la cola.  
                        scheduler_loop(Socket, New_Job_Queue, New_Client_Map, N+1); 
                    {job_finished, Job_Id} ->
                        send_to_agent(Socket, release, integer_to_list(Job_Id)),                   
                        scheduler_loop(Socket, Job_Queue, Client_Map, N);
                    _ -> 
                        scheduler_loop(Socket, Job_Queue, Client_Map, N)
                end
    end.
% %

job_handler(Socket, Nodes_Info, Job_Id, Job_Info, Client_Map) ->
    Msg_to_client = check_job_valid(Socket, Nodes_Info, Job_Id, Job_Info),  % <- devuelve un mensaje para que el cliente sepa si su trabajo fue atendido con exito.
    maps:get(Job_Id, Client_Map) ! Msg_to_client.                           % ¡IMPORTANTE! Durante ésta funcion, el Agente C recibe el pedido y el handler queda en escucha.

message_manager() ->
    

% request_nodes_info/1: Funcion para consultarle al Agente C los nodos que posee a disposicion. 
% Utilizada para devuelve una 4-upla Nodes_Info utilizada por la funcion scheduler_loop.
% %
% parse_node_info/2: Funcion auxilear de request_nodes_info para parsear la información recibida de consultar al Agente C. 
% %
% manage_nodes_info/1: Funcion auxilear de parse_node_info/2 que calcula el maximo 
% habilitado para pedir cada determinado recurso.
% %
% fold_node_data/3: Funcion auxilear de manage_node_info/3 que calcula el maximo habilitado 
% de un determinado tipo de recurso
% %
request_nodes_info(Socket) ->
    
% %

parse_node_info(Data) ->
    case string:split(Data, " ") of
        ["NODES" | String_Nodes] -> List_Nodes = string:tokens(String_Nodes, ";"), % ["NODES"] | ["IP_N1:PORT:cpu:NUM1:mem:NUM2:gpu:NUM3", "IP_N2:PORT:cpu:NUM1:mem:NUM2:gpu:NUM3", ...] %"
                                    manage_nodes_info(List_Nodes); 
        Any -> io:fwrite("Error in info: ~p~n", [Any])
    end.
% %      

manage_nodes_info(List_Nodes) ->                                         
    Max_CPU = lists:foldl(fun(L,Acum) -> fold_node_data(L, Acum, 4) end, 0, List_Nodes),
    Max_MEM = lists:foldl(fun(L,Acum) -> fold_node_data(L, Acum, 6) end, 0, List_Nodes),
    Max_GPU = lists:foldl(fun(L,Acum) -> fold_node_data(L, Acum, 8) end, 0, List_Nodes),
    {Max_CPU, Max_MEM, Max_GPU, List_Nodes}. 
% %

fold_node_data(L, Acum, Data_Index) ->
    Data = string:split(L, ":"),
    NewAcum = Acum + list_to_integer(lists:nth(Data_Index, Data)),
    NewAcum.
% %


% Recibe y Analiza el resultado enviado por el Agente C, en respuesta de
% un JOB_REQUEST enviado por el Sch. Erlang previamente. 
%  Tiene dos funciones principales:
%    * Devolver un atomo para informar que un trabajo fue atendido. Tanto 
%      por si fue aceptado y se les otorgó los recursos al respectivo cliente,
%      como si fue eliminado de la cola de espera por sobrepasar el tiempo limite.
%    * Loopear junto a la funcion send_to_agent/3, con el objetivo de constantemente
%      consultarle al Agente C el estado del pedido hecho anteriormente.%
% %
job_request_inbox(Socket) ->
    Data = gen_tcp:recv(Socket, 0),  %<- Espera a que responda el Agente C
    write_inbox(Data),
    case string:split(Data, " ") of
        ["JOB_GRANTED" | _Job_Id] -> valid_job; 
        
        ["JOB_DENIED" | Job_Id] -> io:fwrite("Job is on the queue by the C agent~n"),
                                    timer:sleep(5000),
                                    send_to_agent(Socket, status, hd(Job_Id)); 

        ["JOB_TIMEOUT" | _Job_Id] -> io:fwrite("Job was timeouted by the C agent~n"),
                                    invalid_job;

        Any -> io:fwrite("Command error: ~p~n", [Any]),
               job_request_inbox(Socket)
    end.
% %

write_inbox(Data) ->
    File_Name = "scheduler_log.txt"
    Data_To_Write = "# El cliente C responde a Erlang:\n\t"++Data++"\n",
    case file:write_file(Filename, Data_To_Write) of
        ok -> 
            io:format("Written Inbox in the Log.~n");
        {error, Reason} -> 
            io:format("Failed to write file: ~p~n", [Reason])
    end.

% 
% Funcion auxiliar que realiza envios informativos al Agente C.
% %
send_to_agent(Socket, Message_Type, INFO) ->
    case Message_Type of
        request -> gen_tcp:send(Socket, "JOB_REQUEST "++INFO),
                   job_request_inbox(Socket);
        status ->  gen_tcp:send(Socket, "JOB_STATUS "++INFO),
                   job_request_inbox(Socket);
        release -> gen_tcp:send(Socket, "JOB_RELEASE "++INFO),
                   timer:sleep(5000)
    end.
% %

% check_job_valid/4: Comprueba que un JOB {Job_Id, Job_Info} es valido.
% Un JOB es valido si el pedido de recursos no excede los valores 
% maximos de cada uno respectivamente.
% %
% manage_job_info/4: Administra un JOB valido; organiza la informacion respectiva de 
% ese JOB y calcula las cantidades de recursos a pedir a cada nodo.
% Luego, envia al Agente C el respectivo REQUEST calculado.
% %
% map_node_data/2: Funcion auxiliar que devuelve una lista de IPs a los Nodos.
% %
% map_node_data_to_int/2: Funcion auxiliar que devuelve una lista de <RESOURSE_TYPE> enteros.
% %
% ammout_to_ask/2: Funcion auxiliar que dado un <RESOURSE_TYPE>_NUM devuelve una lista de 2-uplas 
% {<RESOURSE_TYPE>_NUM,INDEX}, que reprecenta la cantidad de <RESOURSE_TYPE> que 
% se va a pedir a un determinado Nodo de INDEX i.
% %
% string_of_ip_request/3: Funcion auxiliar que parsea el resultado final a enviar a Agente C.
% %
check_job_valid(Socket, Nodes_Info, Job_Id, Job_Info) ->
    {Max_CPU, Max_MEM, Max_GPU, List_Nodes} = Nodes_Info,
    {CPU, MEM, GPU} = Job_Info,
    if 
        Max_CPU - CPU >= 0, Max_MEM - MEM >= 0, Max_GPU - GPU >= 0 ->
            manage_job_info(Socket, List_Nodes, Job_Id, Job_Info);
        true ->
            invalid_job
    end.
% %

manage_job_info(Socket, List_Nodes, Job_Id, Job_Info) ->
    {CPU, MEM, GPU} = Job_Info,
    IP_LIST = lists:map(fun(L) -> map_node_data(L,1) end, List_Nodes),
    CPU_LIST = lists:map(fun(L) -> map_node_data_to_int(L,4) end, List_Nodes),
    MEM_LIST = lists:map(fun(L) -> map_node_data_to_int(L,6) end, List_Nodes),
    GPU_LIST = lists:map(fun(L) -> map_node_data_to_int(L,8) end, List_Nodes),
    CPU_TO_ASK = ammout_to_ask(CPU, CPU_LIST, 1),
    MEM_TO_ASK = ammout_to_ask(MEM, MEM_LIST, 1),
    GPU_TO_ASK = ammout_to_ask(GPU, GPU_LIST, 1),
    String_To_Send = string_of_ip_request(IP_LIST, CPU_TO_ASK,"cpu") ++
                     string_of_ip_request(IP_LIST, MEM_TO_ASK,"mem") ++ string_of_ip_request(IP_LIST, GPU_TO_ASK, "gpu"),
    send_to_agent(Socket, request, integer_to_list(Job_Id)++" "++String_To_Send).
% %  

map_node_data(L, Data_Index) ->
    Data = string:split(L, ":"),
    lists:nth(Data_Index, Data).
% %

map_node_data_to_int(L, Data_Index) ->
    Data = string:split(L, ":"),
    list_to_integer(lists:nth(Data_Index, Data)).
% %

ammout_to_ask(Amount, Resourse_List, Index) ->
    case Resourse_List of 
        [Num | Rest] ->
            if 
                Amount - Num > 0 -> % la cantidad buscada no es suficiente en este nodo
                    Rest_to_ask = Amount - Num,
                    lists:append([{Index, Num}],ammout_to_ask(Rest_to_ask, Rest, Index+1));
                true -> 
                    [{Index, Amount}]
            end;
        [] -> []
    end.
% %

string_of_ip_request(IP_LIST, DATA_TO_ASK, DATA_TYPE) ->
    case DATA_TO_ASK of
        [{Index_IP, AMMOUT} | XS] -> "@"++lists:nth(Index_IP, IP_LIST)++":"++DATA_TYPE++":"++integer_to_list(AMMOUT)++" "++string_of_ip_request(IP_LIST, XS, DATA_TYPE);
        [] -> ""
    end.
% %

% % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % 
%                                                               CLIENTE
% % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % %
%                                                               
% client_simulator/1: Simulador de un cliente que se comunica con el Scheduler para pedir recursos.
% do_job/2: Si recibe que su trabajo fue marcado valido por el Sch. entonces procede a simular trabajar
%         (espera 10 segundos) y responde al Sch. que terminó de trabajar.
% %
client_simulator(Scheduler) ->
    Job_Info = {rand:uniform(10),rand:uniform(10),rand:uniform(10)},
    Scheduler ! {new_job, self(), Job_Info},
    receive
        {given_jobid, Job_Id} -> do_job(Job_Id, Scheduler),
                                 client_simulator(Scheduler);
        _ -> client_simulator(Scheduler)
    end.
% %

do_job(Job_Id, Scheduler) ->
    receive
        valid_job -> timer:sleep(10000),
                     Scheduler ! {job_finished, Job_Id},
                     client_simulator(Scheduler);
        invalid_job -> client_simulator(Scheduler);
        _ -> client_simulator(Scheduler)
    end.
% %

% FIN %

%                                                           EXAMPLE: 
% Abrir conexion
% Pedir info de los nodos usando:
%   "GET_NODES"
% Recibir algo de la forma:                                 (El erlang decide a que nodo pedir sus recursos)
%   "NODES [NODE]"
%       con NODE :: "IP:PORT:cpu:NUM1:mem:NUM2:gpu:NUM3;" 
% Si hay suficientes recursos se lanza un job              
% El Job pide los recursos con "JOB_REQUEST" + Una ID
% Le puede llegar:
%   "JOB_GRANTED", se pone a laburar
%   "JOB_DENIED", se queda esperando
%   "JOB_TIMEOUT", se mata
% Erlang puede enviar "JOB_STATUS + ID"                     (¿Que hace?)
% El erlang libera los recursos con "JOB_RELEASE + ID"