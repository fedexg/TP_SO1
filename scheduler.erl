-module(scheluder).
-export([start/0]).

-export([start_scheduler/0]).


% Abrir conexion
% Pedir info de los nodos usando:
%   "GET_NODES"
% Recibir algo de la forma:                                 <- (El erlang decide a que nodo pedir sus recursos)
%   "NODES [NODE]"
%       con NODE :: "IP:PORT:cpu:NUM1:mem:NUM2:gpu:NUM3;" 
% Si hay suficientes recursos se lanza un job              
% El Job pide los recursos con "JOB_REQUEST" + Una ID
% Le puede llegar:
%   "JOB_GRANTED", se pone a laburar
%   "JOB_DENIED", se queda esperando
%   "JOB_TIMEOUT", se mata
% Erlang puede enviar "JOB_STATUS + ID"                     <- (¿Que hace?)
% El erlang libera los recursos con "JOB_RELEASE + ID"

% Erlang tiene: Scheduler y Un/os cliente/s
% Los clientes le piden al scheduler que quieren (le tiran cuantos recursos quieren) hacer una tarea
% Scheduler le pide los recursos a C
%  
% El/los cliente/s puede/n representarse como un Proceso que genera "jobs" aleatorios
% Con una cantidad de costos aleatorios
% El scheduler decide como repartir los recursos para los jobs
% Los jobs pueden guardarse en una QUEUE
% 
% La comunicacion erlang <--> C, depende del grupo 
%
% Con un Job cada cierto tiempo alcanza para lo que pide el tp
% 
% El deadlock lo mejor es liberarlo cuando se toma mucho tiempo (consultar con la gente)
%
% El puerto puede ser el mismo que el de Broadcast
% C debe determinar si el mensaje vino de erlang u otro agente C

% Guardar Clientes en un mapa que sea Key:Job_ID, Data:proces_Id
% Guardar en la Queue {Job_Id, Request}


% Job_Info is like
% {CPU, MEM, GPU}
%

start() ->
    Scheduler = spawn(?MODULE, start_scheduler, []),
    client_simulator(Scheduler).

start_scheduler() ->
    {ok, Socket} = gen_tcp:conect("localhost",1337),
    scheduler_loop(Socket, queue:new(), maps:new(), 1000).

scheduler_loop(Socket, Job_Queue, Clients_Map, N) ->
    Nodes_Info = request_nodes_info(Socket),
    case queue:is_empty(Job_Queue) of
        false -> {{Job_Id, Job_Info}, New_Job_Queue} = queue:out(Job_Queue),
                 Msg_to_client = check_job_valid(Socket, Nodes_Info, Job_Id, Job_Info),
                 maps:get(Job_Id, Clients_Map) ! Msg_to_client,
                 New_Clients_Map = maps:remove(Job_Id, Clients_Map),
                 scheduler_loop(Socket, New_Job_Queue, New_Clients_Map, N);
        _ -> ok
    end,
    receive
        {new_job, Client_Id, Request_Info} -> New_Job_Queue = queue:in({N, Request_Info},Job_Queue),
                                              New_Client_Map = maps:add(N, Client_Id, Clients_Map),
                                              Client_Id ! {given_jobid, N},
                                              scheduler_loop(Socket, New_Job_Queue, New_Client_Map, N+1);
        {job_finished, Job_Id} -> send_to_agent(Socket, release, Job_Id),
                                  scheduler_loop(Socket, Job_Queue, N);
        _ -> scheduler_loop(Socket, Job_Queue, N);
    end.

request_nodes_info(Socket) ->
    gen_tcp:send(Socket, "GET_NODES"),
    case gen_tcp:recv(Socket, 0) of
        {ok, Data} -> parse_node_info(Socket, Data);
        {error, closed} -> io:fwrite("Socket closed ~n");
        {error, Reason} -> io:fwrite("Error, reason: ~p~n", Reason)
    end.

parse_node_info(Socket, Data) ->
    case string:split(Data, " ") of
        ["NODES" | String_Nodes] -> List_Nodes = string:split(String_Nodes, ";"), %Elem of List_Nodes ~~ "IP:PORT:cpu:NUM1:mem:NUM2:gpu:NUM3"
                                    Request = manage_nodes_info(List_Nodes),
                                    sent_to_agent(Socket, request, Request);
        Any -> io:fwrite("Error in info: ~p~n" [Any])
    end.

fold_node_data(X, Total, Data_Index) ->
    Data = string:split(X, ":"),
    Total + erlang:list_to_integer(lists:nth(Data_Index, Data)).

manage_nodes_info(List_Nodes) ->
    Max_Nodes_CPU = lists:foldl(fun(X,Total) -> fold_node_data(X, Total, 4) end, 0, List_Nodes),
    Max_Nodes_MEM = lists:foldl(fun(X,Total) -> fold_node_data(X, Total, 6) end, 0, List_Nodes),
    Max_Nodes_GPU = lists:foldl(fun(X,Total) -> fold_node_data(X, Total, 8) end, 0, List_Nodes),
    {Max_Nodes_CPU, Max_Nodes_MEM, Max_Nodes_GPU, List_Nodes}. 

job_request_inbox(Socket) ->
    Data = gen_tcp:recv(Socket, 0),
    case string:split(Data, " ") of
        ["JOB_GRANTED" | Job_Id] -> valid_job;
        ["JOB_DENIED" | Job_Id] -> io:fwrite("Job is on the queue by the C agent~n"),
                                    timer:sleep(5000),
                                    send_to_agent(Socket, status, Job_Id);
        ["JOB_TIMEOUT" | Job_Id] -> io:fwrite("Job was timeouted by the C agent~n"),
                                    invalid_job;
        Any -> io:fwrite("Command error: ~p~n", [Any]),
                job_request_inbox(Socket)
    end.

send_to_agent(Socket, Message_Type, Job_Id) ->
    case Message_Type of
        request -> gen_tcp:send(Socket, "JOB_REQUEST "++Job_Id),
                    job_request_inbox(Socket);
        status -> gen_tcp:send(Socket, "JOB_STATUS "++Job_Id),
                    job_request_inbox(Socket);
        release -> gen_tcp:send(Socket, "JOB_RELEASE "++Job_Id),
                    timer:sleep(5000),
    end.

check_job_valid(Socket, Nodes_Info, Job_Id, Job_Info) ->
    {Max_CPU, Max_MEM, Max_GPU, List_Nodes} = Nodes_Info,
    {CPU, MEM, GPU} = Job_Info,
    if 
        Max_CPU - CPU >= 0, Max_MEM - MEM >= 0, Max_GPU - GPU >= 0 ->
            manage_job_info(Socket, List_Nodes, Job_Id, Job_Info);
        true ->
            invalid_job
    end.

map_node_data(List_Nodes, Data_Index) ->
    Data = string:split(X, ":"),
    lists:nth(Data_Index, Data).

map_node_data_to_int(List_Nodes, Data_Index) ->
    Data = string:split(X, ":"),
    erlang:list_to_integer(lists:nth(Data_Index, Data)).

ammout_to_ask(Amount, List, Index) ->
    case List of 
        [Num | Rest] ->
            if 
                Num - Amount < 0 -> % la cantidad buscada no es suficiente en este nodo
                    Rest_to_ask = Amount - Num,
                    lists:append([{Index, Num}],ammout_to_ask(Rest_to_ask, Rest, Index+1));
                true -> 
                    [{Index, Amount}]
            end;
        [] -> []
    end.

string_of_ip_request(IP_LIST, DATA_TO_ASK, DATA_TYPE) ->
    case DATA_TO_ASK of
        [{Index_IP, AMMOUT} | XS] -> "@"++nth(Index_IP, IP_LIST)++":"++DATA_TYPE++":"++integer_to_list(AMMOUT)++" "++string_of_ip_request(IP_LIST, XS, DATA_TYPE);
        [] -> ""
    end.

manage_job_info(Socket, List_Nodes, Job_Id, Job_Info) ->
    {CPU, MEM, GPU} = Job_Info,
    IP_LIST = lists:map(fun(N) -> map_node_data(N,1) end, List_Nodes),
    CPU_LIST = lists:map(fun(N) -> map_node_data_to_int(N,4) end, List_Nodes),
    MEM_LIST = lists:map(fun(N) -> map_node_data_to_int(N,6) end, List_Nodes),
    GPU_LIST = lists:map(fun(N) -> map_node_data_to_int(N,8) end, List_Nodes),
    CPU_TO_ASK = ammout_to_ask(CPU, CPU_LIST),
    MEM_TO_ASK = ammout_to_ask(MEM, MEM_LIST),
    GPU_TO_ASK = ammout_to_ask(GPU, GPU_LIST),
    String_To_Send = string_of_ip_request(IP_LIST, CPU_TO_ASK,"cpu")++string_of_ip_request(IP_LIST, MEM_TO_ASK,"mem")++string_of_ip_request(IP_LIST, GPU_TO_ASK, "gpu")
    send_to_agent(Socket, Message_Type, Job_Id++" "++String_To_Send).

% Recorrer ITH_LIST, haciendo JOB_INFO_ITH - ITH_LIST[ITH], hasta que JOB_INFO_ITH <= 0
% Da una lista de {indice_ip, cuanto_pedir}
% hacer un string "@"++IP_LIST[indice_ip]++":cpu:"++cuanto_pedir

client_simulator(Scheduler) ->
    Job_Request_Info = {rand:uniform(10),rand:uniform(10),rand:uniform(10)},
    Scheduler ! {new_job, self(), Job_Request_Info},
    receive
        {given_jobid, Job_Id} -> do_job(Job_Id, Scheduler);
        _ -> client_simulator(Scheduler)
    end,
    client_simulator(Scheduler).

do_job(Job_Id, Scheduler) ->
    receive
        valid_job -> sleep(10000),
                     Scheduler ! {job_finished, Job_Id},
                     client_simulator(Scheduler);
        invalid_job -> client_simulator(Scheduler);
        _ -> client_simulator(Scheduler)
    end.