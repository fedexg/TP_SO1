-module(scheluder).
-export([client/0]).


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
    {ok, ListenSocket} = gen_tcp:conect("localhost",12529),
    client_loop(ListenSocket).

cient_loop(Socket) ->
    case gen_tcp:recv(Socket, 0) of
        {ok, Data} ->
            request_nodes_info(Socket, Data);
        {error, closed} -> io:fwrite("Socket closed ~n");
        {error, Reason} -> io:fwrite("Error, reason: ~p~n", Reason)
    end,
    cient_loop(Socket).

request_nodes_info(Socket, Data) ->
    gen_tcp:send(Socket, "GET_NODES"),
    case gen_tcp:recv(Socket, 0) of
        {ok, Data} -> parse_node_info(Socket, Data);
        {error, closed} -> io:fwrite("Socket closed ~n");
        {error, Reason} -> io:fwrite("Error, reason: ~p~n", Reason)
    end,

parse_node_info(Socket, Data) ->
    case string:split(Data, " ") of
        ["NODES" | Lista_Nodos] -> 
        Any -> io:fwrite("Error in info: ~p~n" [Any])
    end.


job_request_inbox(Socket, Data) ->
    case string:split(Data, " ") of
        ["JOB_GRANTED" | Job_Id] ->
        ["JOB_DENIED" | Job_Id] ->
        ["JOB_TIMEOUT" | Job_Id] ->
        Any -> io:fwrite("Command error: ~p~n", [Any])
    end.