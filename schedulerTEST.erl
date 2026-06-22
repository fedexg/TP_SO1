-module(schedulerTEST).
-export([start/0]).

-export([start_scheduler/0]).

-export([not_agent_loop/1]).

% Cosa que debe devolver el agente dado un get_nodes
% ["NODES"] | ["IP_N1:PORT:cpu:NUM1:mem:NUM2:gpu:NUM3", "IP_N2:PORT:cpu:NUM1:mem:NUM2:gpu:NUM3", ...] %

%Un JOB es: {Job_Id, Job_Info} 
%       Donde Job_Id es: Un numero unico para identificar un JOB del resto
%       Donde Job_Info es: {CPU_NUM, MEM_NUM, GPU_NUM}                             
%           Donde <RESOURSE_TYPE>_NUM es un entero que reprecenta la cantidad de un recurso que quiere el cliente.


not_agent_loop(Map_Of_JOBs) ->
    io:fwrite("TESTER :: A la espera de una solicitud~n"),
    receive 
        "GET_NODES " ++ Pid_String -> % Tengo que devolver un NODES 192.168.1.10:8100:cpu:4:mem:8192:gpu:1;192.168.1.11:8101:cpu:2:mem:4096
            Pid_To_Answer = list_to_pid(Pid_String),
            List_Of_Rand_Nodes = make_rand_nodes(rand:uniform(10)),
                                % Esto devuelve algo tipo ["192.168.1.10:8100:cpu:4:mem:8192:gpu:1","192.168.1.11:8101:cpu:2:mem:4096:gpu:0",N-veces]
            String_responce = "NODES " ++ string:join(List_Of_Rand_Nodes,";"),
            io:fwrite("TESTER :: Generé: ~s ~n", [String_responce]),
            Pid_To_Answer ! {get_nodes, String_responce},
            not_agent_loop(Map_Of_JOBs);

        "JOB_REQUEST " ++ STRING -> %JOB_REQUEST <PID> Job_Id @192.168.1.2:cpu:2 @192.168.1.3:gpu:1
            Data_Splited = string:tokens(STRING, " "),
            Pid_To_Answer = list_to_pid(lists:nth(1, Data_Splited)),
            Job_Id = lists:nth(2, Data_Splited),
            case io:get_line("TESTER :: 1: Enviar un 'JOB_GRANTED' al scheduler\nTESTER :: 2: Enviar un 'JOB_DENIED' al scheduler\nTESTER ::") of 
                "1\n" -> 
                    Pid_To_Answer ! {ok, "JOB_GRANTED " ++ Job_Id}, %JOB_GRATED 12
                    not_agent_loop(maps:puts(Job_Id, granted, Map_Of_JOBs));
                "2\n" ->
                    Pid_To_Answer ! {ok, "JOB_DENIED " ++ Job_Id},
                    not_agent_loop(maps:puts(Job_Id, denied, Map_Of_JOBs))
            end;
        
        "JOB_STATUS " ++ STRING -> %JOB_STATUS <PID> Job_Id
            Data_Splited = string:tokens(STRING, " "),
            Pid_To_Answer = list_to_pid(lists:nth(1, Data_Splited)),
            Job_Id = lists:nth(2, Data_Splited),
            case io:get_line("TESTER :: 1: Enviar un 'JOB_GRANTED' al scheduler\nTESTER :: 2: Enviar un 'JOB_DENIED' al scheduler\nTESTER :: 3: Enviar un 'JOB_TIMEOUT' al scheduler\nTESTER ::") of 
                "1\n" -> 
                    Pid_To_Answer ! {ok, "JOB_GRANTED " ++ Job_Id}, %JOB_GRATED 12
                    not_agent_loop(maps:puts(Job_Id, granted, Map_Of_JOBs));
                "2\n" ->
                    Pid_To_Answer ! {ok, "JOB_DENIED " ++ Job_Id},
                    not_agent_loop(maps:puts(Job_Id, denied, Map_Of_JOBs));
                "3\n" ->
                    Pid_To_Answer ! {ok, "JOB_TIMEOUT " ++ Job_Id},
                    not_agent_loop(maps:remove(Job_Id, Map_Of_JOBs))
            end;
        
        "JOB_RELEASE " ++ STRING -> %JOB_RELEASE <PID> Job_Id
            Data_Splited = string:tokens(STRING, " "),
            Job_Id = lists:nth(2, Data_Splited),
            case maps:find(Job_Id, Map_Of_JOBs) of
                {ok, granted} ->
                    not_agent_loop(maps:remove(Job_Id, Map_Of_JOBs));
                {ok, denied} ->
                    io:fwrite("TESTER :: ERROR, Quiere liberar un JOB denegado!~n"),
                    not_agent_loop(Map_Of_JOBs);
                error ->
                    io:fwrite("TESTER :: ERROR, Quiere liberar un JOB no existente!~n"),
                    not_agent_loop(Map_Of_JOBs)
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

% Crea la comunicacion con el Agente C creando un Not_Socket tcp 
% local conectado al Agente C. Ademas procede a iniciar el scheluder
% %
start_scheduler() ->
    Not_Socket = spawn(?MODULE, not_agent_loop, [maps:new()]),
    scheduler_loop(Not_Socket, queue:new(), maps:new(), 1000).

% Funcion pricipal donde el proceso SCHEDULER hace el trabajo
% de administrador de pedidos de trabajo de clientes simulados
% %
% Tengo un problema con esta implementacion... que sucede si en la cola hay muchos request de trabajo de
% clientes y como el scheduler tarda mucho en responder el scheduler pierde en la linea 'receive' señales
% de nuevos clientes. DE TODAS FORMAS, nunca pasa en esta implementacion porque la cola es inmediatamente vaciada
% por el scheduler, devido a que hay un unico cliente pidiendo trabajos.
% %
scheduler_loop(Not_Socket, Job_Queue, Client_Map, N) ->
    Nodes_Info = request_nodes_info(Not_Socket),            % <- Cada nuevo loop, le pregunta al Agente C que opciones tiene para otorgar a los clientes. 
    io:fwrite("~p~n",[Nodes_Info]),
    case queue:is_empty(Job_Queue) of                   % (En mi opinion) capaz un poco excesivo, podría hacerlo una vez cada N tiempo
        % La cola de JOBS tiene elementos: atiende el JOB que desencola. 
        false -> 
            {{Job_Id, Job_Info}, New_Job_Queue} = queue:out(Job_Queue),                 % <- La cola guarda tuplas de la forma {ID,INFO}, JOBs
            Msg_to_client = check_job_valid(Not_Socket, Nodes_Info, Job_Id, Job_Info),  % <- devuelve un mensaje para que el cliente sepa si su trabajo fue atendido con exito.
            maps:get(Job_Id, Client_Map) ! Msg_to_client,                               % ¡IMPORTANTE! Durante ésta funcion, el Agente C recibe el pedido y el scheduler queda en escucha.
            New_Client_Map = maps:remove(Job_Id, Client_Map),                           % <- Saca el JOB atendido
            scheduler_loop(Not_Socket, New_Job_Queue, New_Client_Map, N);               
              
        true -> ok
    end,
    receive
        {new_job, Client_Id, Job_Info_Recv} -> 
            Neew_Job_Queue = queue:in({N, Job_Info_Recv},Job_Queue),     % <- Encola el JOB con su id unico.
            Neew_Client_Map = maps:put(N, Client_Id, Client_Map),        % <- Agrega en el diccionario el JOB asignado al cliente.
            Client_Id ! {given_jobid, N},                               % <- Confirma al cliente el almacenamiento de su pedido a la cola.  
            scheduler_loop(Not_Socket, Neew_Job_Queue, Neew_Client_Map, N+1); 

        {job_finished, Old_Job_Id} ->
            send_to_agent(Not_Socket, release, Old_Job_Id),                   
            scheduler_loop(Not_Socket, Job_Queue, Client_Map, N);

        _ -> 
            scheduler_loop(Not_Socket, Job_Queue, Client_Map, N)
    end.

% request_nodes_info/1: Funcion para consultarle al Agente C los nodos que posee a disposicion. 
%                       Utilizada para devuelve una 4-upla Nodes_Info utilizada por la funcion scheduler_loop.
%
% parse_node_info/2: Funcion auxilear de request_nodes_info para parsear la información recibida de consultar al Agente C. 
% 
% manage_nodes_info/1: Funcion auxilear de parse_node_info/2 que calcula el maximo 
%                      habilitado para pedir cada determinado recurso.
% 
% fold_node_data/3: Funcion auxilear de manage_node_info/3 que calcula el maximo habilitado 
%                   de un determinado tipo de recurso
% %
request_nodes_info(Not_Socket) ->
    Not_Socket ! "GET_NODES " ++ pid_to_list(self()),
    receive
        {get_nodes, Data} -> parse_node_info(Data); % NODES 192.168.1.10:8100:cpu:4:mem:8192:gpu:1;192.168.1.11:8101:cpu:2:mem:4096
        {error, closed} -> io:fwrite("Not_Socket closed ~n");
        {error, Reason} -> io:fwrite("Error, reason: ~p~n", Reason)
    end.
% %
parse_node_info(Data) ->
    io:fwrite("Tengo que parcear esto: ~s~n",[Data]),
    case string:split(Data," ") of
        ["NODES", String_Nodes] -> List_Nodes = string:tokens(String_Nodes, ";"), 
                                    manage_nodes_info(List_Nodes); 
        Any -> io:fwrite("Error in info: ~p~n", [Any])
    end.
% %                                                                
manage_nodes_info(List_Nodes) ->                                         
    Max_CPU = lists:foldl(fun(L,Acum) -> fold_node_data(L, Acum, 4) end, 0, List_Nodes),
    Max_MEM = lists:foldl(fun(L,Acum) -> fold_node_data(L, Acum, 6) end, 0, List_Nodes),
    Max_GPU = lists:foldl(fun(L,Acum) -> fold_node_data(L, Acum, 8) end, 0, List_Nodes),
    io:format("DEBUG: Valores calculados -> CPU:~p, MEM:~p, GPU:~p~n", [Max_CPU, Max_MEM, Max_GPU]),
    {Max_CPU, Max_MEM, Max_GPU, List_Nodes}. 
% %
fold_node_data(L, Acum, Data_Index) ->
    Data = string:tokens(L, ":"),
    if 
        length(Data) >= Data_Index ->
            NewAcum = Acum + list_to_integer(lists:nth(Data_Index, Data)),
            NewAcum;
        true ->
            % Si la línea está mal formada, simplemente retornamos el Acumulador sin sumar nada
            Acum
    end.


% Recibe y Analiza el resultado enviado por el Agente C, en respuesta de
% un JOB_REQUEST enviado por el Sch. Erlang previamente. 
%  Tiene dos funciones principales:
%    * Devolver un atomo para informar que un trabajo fue atendido. Tanto 
%      por si fue aceptado y se les otorgó los recursos al respectivo cliente,
%      como si fue eliminado de la cola de espera por sobrepasar el tiempo limite.
%    * Loopear junto a la funcion send_to_agent/3, con el objetivo de constantemente
%      consultarle al Agente C el estado del pedido hecho anteriormente.%
% VERCION DE TESTEO %
job_request_inbox(Not_Socket) ->
    receive  
        {ok, Data} ->
            case string:tokens(Data, " ") of
                ["JOB_GRANTED" | _] -> valid_job; % FALTA COMPROBAR QUE SEA UN GRANTED RESPECTIVO AL JOB DE ID Job_Id

                ["JOB_DENIED" | Job_Id] -> io:fwrite("Job is on the queue by the C agent~n"),
                                            timer:sleep(5000),
                                            send_to_agent(Not_Socket, status, Job_Id); % El agente envia y recibe mensajes crudos.
                
                ["JOB_TIMEOUT" | _] -> io:fwrite("Job was timeouted by the C agent~n"),
                                       invalid_job;

                Any -> io:fwrite("Command error: ~p~n", [Any]),
                       job_request_inbox(Not_Socket)
            end
    end.

% Funcion auxiliar para enviar Keke 
% %
send_to_agent(Not_Socket, Message_Type, INFO) ->
    case Message_Type of                                                                    %EXAMPLES:
        request -> Not_Socket ! "JOB_REQUEST "++pid_to_list(self())++" "++INFO, %JOB_REQUEST <PID> Job_Id @192.168.1.2:cpu:2 @192.168.1.3:gpu:1
                   job_request_inbox(Not_Socket);
        status ->  Not_Socket ! "JOB_STATUS "++pid_to_list(self())++" "++INFO,  %JOB_STATUS <PID> Job_Id
                   job_request_inbox(Not_Socket);
        release -> Not_Socket ! "JOB_RELEASE "++pid_to_list(self())++" "++INFO, %JOB_RELEASE <PID> Job_Id
                   timer:sleep(5000)
    end.

% check_job_valid/4: Comprueba que un JOB {Job_Id, Job_Info} es valido.
%                  Un JOB es valido si el pedido de recursos no excede los valores 
%                  maximos de cada uno respectivamente.
% %
% manage_job_info/4: Administra un JOB valido; organiza la informacion respectiva de 
%                  ese JOB y calcula las cantidades de recursos a pedir a cada nodo.
%                  Luego, envia al Agente C el respectivo REQUEST calculado.
% %
% map_node_data/2: Funcion auxiliar que devuelve una lista de IPs a los Nodos.
% %
% map_node_data_to_int/2: Funcion auxiliar que devuelve una lista de <RESOURSE_TYPE> enteros.
% %
% ammout_to_ask/2: Funcion auxiliar que dado un <RESOURSE_TYPE>_NUM devuelve una lista de 2-uplas 
%                {<RESOURSE_TYPE>_NUM,INDEX}, que reprecenta la cantidad de <RESOURSE_TYPE> que 
%                se va a pedir a un determinado Nodo de INDEX i.
% %
% string_of_ip_request/3: Funcion auxiliar que parsea el resultado final a enviar a Agente C.
% %
check_job_valid(Not_Socket, Nodes_Info, Job_Id, Job_Info) ->
    io:fwrite("check_job_valid:~p~n",[Nodes_Info]),
    {Max_CPU, Max_MEM, Max_GPU, List_Nodes} = Nodes_Info,
    {CPU, MEM, GPU} = Job_Info,
    if 
        Max_CPU - CPU >= 0, Max_MEM - MEM >= 0, Max_GPU - GPU >= 0 ->
            manage_job_info(Not_Socket, List_Nodes, Job_Id, Job_Info);
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
    send_to_agent(Not_Socket, request, Job_Id++" "++String_To_Send). %JOB REQUEST @192.168.1.2:cpu:2 @192.168.1.3:gpu:1
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

% % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % 
%                                                               CLIENTE
% % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % %
%                                                               
% Recorrer ITH_LIST, haciendo JOB_INFO_ITH - ITH_LIST[ITH], hasta que JOB_INFO_ITH <= 0
% Da una lista de {indice_ip, cuanto_pedir}
% hacer un string "@"++IP_LIST[indice_ip]++":cpu:"++cuanto_pedir

% client_simulator/1: Simulador de un cliente que se comunica con el Scheduler para pedir recursos.
% do_job/2: Si recibe que su trabajo fue marcado valido por el Sch. entonces procede a simular trabajar
%         (espera 10 segundos) y responde al Sch. que terminó de trabajar.
% %
client_simulator(Scheduler) ->
    Job_Info = {rand:uniform(10),rand:uniform(10),rand:uniform(10)},
    Scheduler ! {new_job, self(), Job_Info},
    receive
        {given_jobid, Job_Id} -> 
            io:fwrite("HOLA?~n"),
            do_job(Job_Id, Scheduler),
            client_simulator(Scheduler);
        _ -> client_simulator(Scheduler)
    end.
% %
do_job(Job_Id, Scheduler) ->
    receive
        valid_job -> timer:sleep(5000),
                     Scheduler ! {job_finished, Job_Id},
                     client_simulator(Scheduler);
        invalid_job -> client_simulator(Scheduler);
        _ -> client_simulator(Scheduler)
    end.

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