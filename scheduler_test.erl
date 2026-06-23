-module(scheduler_test).
-export([start/0]).

-export([start_scheduler/0, scheduler_loop/5, job_manager/5, message_manager/3, start_not_agent/0]).
%-define(PORT,1337).

% Cosa que debe devolver el agente dado un get_nodes
% ["NODES"] | ["IP_N1:PORT:cpu:NUM1:mem:NUM2:gpu:NUM3", "IP_N2:PORT:cpu:NUM1:mem:NUM2:gpu:NUM3", ...] %

%Un JOB es: {Job_Id, Job_Info} 
%       Donde Job_Id es: Un numero unico para identificar un JOB del resto
%       Donde Job_Info es: {CPU_NUM, MEM_NUM, GPU_NUM}                             
%           Donde <RESOURSE_TYPE>_NUM es un entero que reprecenta la cantidad de un recurso que quiere el cliente.

not_agent_loop(Map_Of_JOBs, Message_Manager) ->
    io:fwrite("TESTER :: ~n"),
    receive 
        "GET_NODES\n" -> % Tengo que devolver un NODES 192.168.1.10:8100:cpu:4:mem:8192:gpu:1;192.168.1.11:8101:cpu:2:mem:4096
            io:fwrite("TESTER :: HOLA???? ~p~n",[self()]),
            List_Of_Rand_Nodes = make_rand_nodes(1 + rand:uniform(2)),
                                % Esto devuelve algo tipo ["192.168.1.10:8100:cpu:4:mem:8192:gpu:1","192.168.1.11:8101:cpu:2:mem:4096:gpu:0",N-veces]
            String_Response = "NODES " ++ string:join(List_Of_Rand_Nodes, ";") ++ "\n",
            Message_Manager ! {ok, String_Response},
            io:fwrite("TESTER :: GET_NODES~n"),
            not_agent_loop(Map_Of_JOBs, Message_Manager);

        "JOB_REQUEST " ++ STRING -> %JOB_REQUEST Job_Id @192.168.1.2:cpu:2 @192.168.1.3:gpu:1
            Data_Splited = string:tokens(STRING, " "),
            Job_Id = hd(Data_Splited),
            io:fwrite("TESTER :: JOB_REQUEST~n"),
            case io:get_line("TESTER :: 1: Enviar un 'JOB_GRANTED' al scheduler\nTESTER :: 2: Enviar un 'JOB_DENIED' al scheduler\nTESTER :: ") of 
                "1\n" -> 
                    Message_Manager ! {ok, "JOB_GRANTED " ++ Job_Id}, 
                    not_agent_loop(maps:put(list_to_integer(Job_Id), granted, Map_Of_JOBs), Message_Manager);
                "2\n" ->
                    Message_Manager ! {ok, "JOB_DENIED " ++ Job_Id},
                    not_agent_loop(maps:put(list_to_integer(Job_Id), denied, Map_Of_JOBs), Message_Manager)
            end;
        
        "JOB_STATUS " ++ STRING -> %JOB_STATUS Job_Id
            Data_Splited = string:tokens(STRING, " "),
            Job_Id = hd(Data_Splited),
            io:fwrite("TESTER :: JOB_STATUS~n"),
            case io:get_line("TESTER :: 1: Enviar un 'JOB_GRANTED' al scheduler\nTESTER :: 2: Enviar un 'JOB_DENIED' al scheduler\nTESTER :: 3: Enviar un 'JOB_TIMEOUT' al scheduler\nTESTER ::") of 
                "1\n" -> 
                    Message_Manager ! {ok, "JOB_GRANTED " ++ Job_Id}, %JOB_GRATED 12
                    not_agent_loop(maps:put(list_to_integer(Job_Id), granted, Map_Of_JOBs), Message_Manager);
                "2\n" ->
                    Message_Manager ! {ok, "JOB_DENIED " ++ Job_Id},
                    not_agent_loop(maps:put(list_to_integer(Job_Id), denied, Map_Of_JOBs), Message_Manager);
                "3\n" ->
                    Message_Manager ! {ok, "JOB_TIMEOUT " ++ Job_Id},
                    not_agent_loop(maps:remove(list_to_integer(Job_Id), Map_Of_JOBs), Message_Manager)
            end;
        
        "JOB_RELEASE " ++ STRING -> %JOB_RELEASE Job_Id
            Data_Splited = string:tokens(STRING, " "),
            Job_Id = hd(Data_Splited),
            List_Of_JOBs = maps:to_list(Map_Of_JOBs),
            io:fwrite("TESTER :: JOB_RELEASE~n"),
            lists:foreach(fun(A)-> io:fwrite("TESTER :: ~p~n", [A]) end, List_Of_JOBs),
            case maps:find(list_to_integer(Job_Id), Map_Of_JOBs) of
                {ok, granted} ->
                    not_agent_loop(maps:remove(list_to_integer(Job_Id), Map_Of_JOBs), Message_Manager);
                {ok, denied} ->
                    io:fwrite("TESTER :: ERROR, Quiere liberar un JOB denegado!~n"),
                    not_agent_loop(Map_Of_JOBs, Message_Manager);
                error ->
                    io:fwrite("TESTER :: ERROR, Quiere liberar un JOB no existente!~n"),
                    not_agent_loop(Map_Of_JOBs, Message_Manager)
            end
    end.

make_rand_nodes(0) -> [];
make_rand_nodes(N) -> 
    Rand_IP = "192.168.1." ++ integer_to_list(rand:uniform(50)),
    Rand_Port = integer_to_list(8000 + rand:uniform(999)),
    Rand_Cpu = integer_to_list(rand:uniform(10)),
    Rand_Mem = integer_to_list(2000 + rand:uniform(8)*1000),
    Rand_Gpu = integer_to_list(rand:uniform(3)*2),
    Node_Data = [Rand_IP] ++ [Rand_Port] ++ ["cpu"] ++ [Rand_Cpu] ++ ["mem"] ++ [Rand_Mem] ++ ["gpu"] ++ [Rand_Gpu],
    Node_String = string:join(Node_Data,":"),
    [Node_String] ++ make_rand_nodes(N-1).





% Inicia el iniciador del Scheduler y el simulador de cliente 
% %
start() ->
    Scheduler = spawn(?MODULE, start_scheduler, []),
    client_simulator(Scheduler).
% %

% Crea la comunicacion con el Agente C creando un socket tcp local. 
% Ademas procede a iniciar el scheluder, compuesto por:
% - El scheduler_loop/5: Que atiende las solicitudes de trabajo.
% - Y el message_manager/3: Que administra la comunicacion Agente C -> Scheduler.%
% %
start_scheduler() ->
    Scheduler_Pid = self(),
    Not_Socket = spawn(?MODULE, start_not_agent, []),
    Message_Manager = spawn(?MODULE, message_manager, [Not_Socket, Scheduler_Pid, maps:new()]),
    Not_Socket ! Message_Manager,
    scheduler_loop(Not_Socket, queue:new(), maps:new(), 1000, Message_Manager).
% %

start_not_agent() ->
    receive
        Message_Manager_Pid ->
            not_agent_loop(maps:new(),Message_Manager_Pid) end. 
% %

scheduler_loop(Not_Socket, Job_Queue, Client_Map, N, Message_Manager) ->
    Nodes_Info = request_nodes_info(Not_Socket),            % <- Cada nuevo loop, le pregunta al Agente C que opciones tiene para otorgar a los clientes. 
    case queue:is_empty(Job_Queue) of                   
        % La cola de JOBS tiene elementos: atiende el JOB que desencola. 
        false -> 
            {Queue_Head, New_Job_Queue} = queue:out(Job_Queue),             % <- La cola guarda tuplas de la forma {ID,INFO}, JOBs
            {value, {Job_Id, Job_Info}} = Queue_Head,
            Job_Manager = spawn(?MODULE, job_manager, [Not_Socket, Nodes_Info, Job_Id, Job_Info, maps:get(Job_Id, Client_Map)]),  %<- Se le asigna al JOB un manager para que lo gestione.
            Message_Manager ! {scheduler_loop, Job_Id, Job_Manager},
            New_Client_Map = maps:remove(Job_Id, Client_Map),                       % <- Saca el JOB de la atencion del Scheduler, ahora se encarga su manager.
            scheduler_loop(Not_Socket, New_Job_Queue, New_Client_Map, N, Message_Manager);               
        _ -> receive
                    {new_job, Client_Id, Job_Info_Recv} -> 
                        New_Job_Queue = queue:in({N, Job_Info_Recv},Job_Queue),     % <- Encola el JOB con su id unico.
                        New_Client_Map = maps:put(N, Client_Id, Client_Map),        % <- Agrega en el diccionario el JOB asignado al cliente.
                        io:fwrite("SCHE :: HOLA???? ~p~n",[New_Client_Map]),
                        Client_Id ! {given_jobid, N},                               % <- Confirma al cliente el almacenamiento de su pedido a la cola.  
                        scheduler_loop(Not_Socket, New_Job_Queue, New_Client_Map, N+1, Message_Manager); 
                    {job_finished, Job_Id} ->
                        send_to_agent(Not_Socket, release, integer_to_list(Job_Id)),                   
                        scheduler_loop(Not_Socket, Job_Queue, Client_Map, N, Message_Manager);
                    Any -> 

                        scheduler_loop(Not_Socket, Job_Queue, Client_Map, N, Message_Manager)
                end
    end.
% %

% Funcion que gestiona la solicitud de trabajo de un determinado cliente de pid Client_Pid.
% %
job_manager(Not_Socket, Nodes_Info, Job_Id, Job_Info, Client_Pid) ->
    Job_Request_Results = check_job_valid(Not_Socket, Nodes_Info, Job_Id, Job_Info),  % <- devuelve un mensaje para que el cliente sepa si su trabajo fue atendido con exito.
    case Job_Request_Results of
        invalid_job ->          %<- El pedido está fuera de los limites admitidos
            Client_Pid ! invalid_job;
        waiting_on_manager ->   %<- Comienza la espera 
            receive
                Msg_to_client -> Client_Pid ! Msg_to_client % ¡IMPORTANTE! Durante ésta funcion, el Agente C recibe el pedido y el handler queda en escucha.
            end
    end.
% %

% Guarda, administra y reenvia los datos entrantes del Agente C, hacia el Sch. Erlang. Como la 
% transmisión de datos es UDP, el manager está preparado para recibir la informacion en datagramas.
% %
message_manager(Not_Socket, Scheduler_Pid, Manager_Map) ->
    receive
        {scheduler_loop, Job_Id, Job_Manager} -> 
            New_Manager_Map = maps:put(Job_Id, Job_Manager, Manager_Map),
            message_manager(Not_Socket, Scheduler_Pid, New_Manager_Map)
    after 0 ->                                                                       %<- Siempre que el buzon no este vacio, pasa inmediatamente a este bloque.
            receive                                                                     %<- Espera a que responda el Agente C.
                {ok, Data} ->                                              
                    io:fwrite("MANAGER :: HOLA???? ~p~n",[Data]),
                    Data_List = string:tokens(Data, "\n"),                              %<- Previene errores de datapacks al separarlos por '\n'.
                    lists:foreach(fun(Elem) -> job_request_inbox(Not_Socket, Elem, Scheduler_Pid, Manager_Map) end, Data_List), 
                    message_manager(Not_Socket, Scheduler_Pid, Manager_Map)
            end
    end.
% %    

% request_nodes_info/1: Funcion auxilear de scheduler_loop/5 para consultarle al 
% Agente C los nodos que posee a disposicion. Devuelve Nodes_Info en scheduler_loop.
% %
% parse_node_info/2: Funcion auxilear de request_nodes_info para parsear la 
% información recibida de consultar al Agente C. 
% %
% manage_nodes_info/1: Funcion auxilear de parse_node_info/2 que calcula el maximo 
% habilitado para pedir cada determinado recurso.
% %
% fold_node_data/3: Funcion auxilear de manage_node_info/3 que calcula el maximo habilitado 
% de un determinado tipo de recurso
% %
request_nodes_info(Not_Socket) ->
    Not_Socket ! "GET_NODES\n",
    receive
        {nodes, Data} -> parse_node_info(Data)
    end.    
% %

parse_node_info(String_Nodes) ->
    List_Nodes = string:tokens(String_Nodes, ";"), % ["NODES"] | ["IP_N1:PORT:cpu:NUM1:mem:NUM2:gpu:NUM3", "IP_N2:PORT:cpu:NUM1:mem:NUM2:gpu:NUM3", ...] %"
    manage_nodes_info(List_Nodes).
% %      

manage_nodes_info(List_Nodes) ->                                         
    Max_CPU = lists:foldl(fun(L,Acum) -> fold_node_data(L, Acum, 4) end, 0, List_Nodes),
    Max_MEM = lists:foldl(fun(L,Acum) -> fold_node_data(L, Acum, 6) end, 0, List_Nodes),
    Max_GPU = lists:foldl(fun(L,Acum) -> fold_node_data(L, Acum, 8) end, 0, List_Nodes),
    {Max_CPU, Max_MEM, Max_GPU, List_Nodes}.
% %

fold_node_data(L, Acum, Data_Index) ->
    Data = string:tokens(L, ":"),
    NewAcum = Acum + list_to_integer(lists:nth(Data_Index, Data)),
    NewAcum.
% %


% job_request_inbox: Analiza un unico envio hecho por el Agente C a la vez.
% Ademas, consulta en periodos de 5 segundos al Agente C del estado del pedido asignado
% %
% write_inbox: Funcion auxilear que crea/escribe en un archivo que lleva registro
% de la comunicacion entre el Scheduler Erlang y el Agente C.
% %
job_request_inbox(Not_Socket, Data, Scheduler_Pid, Manager_Map) ->
    write_inbox(Data),
    case string:split(Data, " ") of
        ["NODES" | String_Nodes] -> Scheduler_Pid ! {nodes, hd(String_Nodes)};

        ["JOB_GRANTED" | Job_Id] -> maps:get(list_to_integer(hd(Job_Id)),Manager_Map) ! valid_job;  %maps:get(list_to_integer(hd(Job_Id)),Manager_Map) == Job_Manager Pid

        ["JOB_DENIED" | Job_Id] -> io:fwrite("Job is on the queue by the C agent~n"),
                                    timer:sleep(5000),                                  %<- Espera inactiva.
                                    send_to_agent(Not_Socket, status, hd(Job_Id)); 

        ["JOB_TIMEOUT" | Job_Id] -> io:fwrite("Job was timeouted by the C agent~n"),
                                    maps:get(list_to_integer(hd(Job_Id)),Manager_Map) ! invalid_job;

        Any -> io:fwrite("Command error: ~p~n", [Any])
    end.
% %

write_inbox(Data) ->
    File_Name = "scheduler_log.txt",
    Data_To_Write = "# Agent C responce to Erlang Scheduler:\n\t"++Data++"\n",
    case file:write_file(File_Name, Data_To_Write) of
        ok -> 
            io:format("Written Inbox in the Log.~n");
        {error, Reason} -> 
            io:format("Failed to write file: ~p~n", [Reason])
    end.
% %

% 
% Funcion auxiliar que realiza envios informativos al Agente C.
% %
send_to_agent(Not_Socket, Message_Type, INFO) ->
    case Message_Type of
        request -> Not_Socket ! "JOB_REQUEST " ++ INFO;
        status ->  Not_Socket ! "JOB_STATUS " ++ INFO;
        release -> Not_Socket ! "JOB_RELEASE " ++ INFO,
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
check_job_valid(Not_Socket, Nodes_Info, Job_Id, Job_Info) ->
    {Max_CPU, Max_MEM, Max_GPU, List_Nodes} = Nodes_Info,
    {CPU, MEM, GPU} = Job_Info,
    if 
        Max_CPU - CPU >= 0, Max_MEM - MEM >= 0, Max_GPU - GPU >= 0 ->
            manage_job_info(Not_Socket, List_Nodes, Job_Id, Job_Info),
            waiting_on_manager;
        true ->
            invalid_job
    end.
% %

manage_job_info(Not_Socket, List_Nodes, Job_Id, Job_Info) ->
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
    send_to_agent(Not_Socket, request, integer_to_list(Job_Id)++" "++String_To_Send).
% %  

map_node_data(L, Data_Index) ->
    Data = string:tokens(L, ":"),
    lists:nth(Data_Index, Data).
% %

map_node_data_to_int(L, Data_Index) ->
    Data = string:tokens(L, ":"),
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