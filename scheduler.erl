-module(scheluder).
-export([client/0]).

start() ->
    {ok, ListenSocket} = gen_tcp:listen(12529, [{reuseaddr, true}]),
    accept_loop(ListenSocket).

accept_loop(ListenSocket) ->
    {ok, Socket} = gen_tcp:accept(ListenSocket),
    spawn(fun() -> handle_client(Socket) end).

handle_client(ListenSocket) ->
    case gen_tcp:recv(Socket, 0) of
        {ok, Data} ->
            gen_tcp:send(Socket, Reply), 
            handle_client(Socket);
        {error, closed} -> io:fwrite("Socket closed ~n");
        {error, Reason} -> io:fwrite("Error, reason: ~p~n", Reason)
    end,
    accept_loop(ListenSocket).
