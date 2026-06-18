-module(scheluder).
-export([start/0]).


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


start() ->
    {ok, Socket} = gen_tcp:conect("localhost",12529),
    client(Socket).

client(Socket) ->
    case gen_tcp:recv(Socket, 0) of
        {ok, Data} ->
            request_nodes_info(Socket, Data);
        {error, closed} -> io:fwrite("Socket closed ~n");
        {error, Reason} -> io:fwrite("Error, reason: ~p~n", Reason)
    end,
    client(Socket).

request_nodes_info(Socket, Data) ->
    gen_tcp:send(Socket, "GET_NODES"),
    case gen_tcp:recv(Socket, 0) of
        {ok, Data} -> parse_node_info(Socket, Data);
        {error, closed} -> io:fwrite("Socket closed ~n");
        {error, Reason} -> io:fwrite("Error, reason: ~p~n", Reason)
    end.

parse_node_info(Socket, Data) ->
    case string:split(Data, " ") of
        ["NODES" | String_Nodes] -> List_Nodes = string:split(String_Nodes, ";"), %Elem of List_Nodes ~~ "IP:PORT:cpu:NUM1:mem:NUM2:gpu:NUM3"
                                    manage_nodes_info(List_Nodes);
        Any -> io:fwrite("Error in info: ~p~n" [Any])
    end.

fold_node_data(X, Total, Data_Index) ->
    Data = string:split(X, ":"),
    Total + list_to_integer(lists:nth(Data_Index, Data)).

manage_nodes_info(List_Nodes) ->
    Max_Nodes_CPU = lists:foldl(fun(X,Total) -> fold_node_data(X, Total, 4) end, 0, List_Nodes),
    Max_Nodes_MEM = lists:foldl(fun(X,Total) -> fold_node_data(X, Total, 6) end, 0, List_Nodes),
    Max_Nodes_GPU = lists:foldl(fun(X,Total) -> fold_node_data(X, Total, 8) end, 0, List_Nodes),
    Job_Id = 1000 + (erlang:unique_integer([positive]) rem 1000),
    todo.

job_request_inbox(Socket) ->
    Data = gen_tcp:recv(Socket, 0),
    case string:split(Data, " ") of
        ["JOB_GRANTED" | Job_Id] -> do_job();
        ["JOB_DENIED" | Job_Id] -> io:fwrite("Job was denied by the C agent~n"),
                                    timer:sleep(5000),
                                    request_nodes_info(Socket);
        ["JOB_TIMEOUT" | Job_Id] -> io:fwrite("Job was put on timeout by the C agent~n"),
                                    timer:sleep(5000),
                                    send_to_agent(Socket, status); 
        Any -> io:fwrite("Command error: ~p~n", [Any])
    end.

send_to_agent(Socket, Message_Type, Job_Id) ->
    case Message_Type of
        request -> gen_tcp:send(Socket, "JOB_REQUEST "++Job_Id),
                    job_request_inbox(Socket);
        status -> gen_tcp:send(Socket, "JOB_STATUS "++Job_Id),
                    job_request_inbox(Socket);
        release -> gen_tcp:send(Socket, "JOB_RELEASE "++Job_Id),
                    timer:sleep(5000),
                    request_nodes_info(Socket)
    end.