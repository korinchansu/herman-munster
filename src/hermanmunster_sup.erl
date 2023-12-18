-module(hermanmunster_sup).

-export([init_ack/0, count_connections/1, release_connection/1]).

-record(state, {parent = nil, dbg = nil, lsocket = nil, ref = nil, mfa = nil, 
                child_type = nil, max_connections = nil, timeout = nil, should_accept = true}).

start_link(Opts) ->
    proc_lib:start_link(?MODULE, init, [self(), Opts]).

init(Parent, Opts) ->
    process_flag(trap_exit, true),
    proc_lib:init_ack(Parent, {ok, self()}),
    Dbg = sys:debug_options(maps:get(debug, Opts, [])),

    S = #state{parent = Parent,
               dbg = Dbg,
               lsocket = maps:get(socket, Opts),
               mfa = maps:get(mfa, Opts),
               child_type = maps:get(child_type, Opts),
               max_connections = maps:get(maximum_connections, Opts),
               timeout = maps:get(shutdown, Opts)},

    {ConnCount, NewS} = do_accept(0, S),
    loop(ConnCount, NewS).

loop(ConnCount, S = #state{parent = Parent, dbg = Dbg, lsocket = LSocket, 
                           child_type = ChildType, ref = Ref, mfa = Mfa, 
                           timeout = Timeout, should_accept = ShouldAccept}) ->
    receive
        {'inet_async', LSocket, Ref, {ok, Socket}} = Ev -> 
            NewDbg = sys:handle_debug(Dbg, fun write_debug/3, ShouldAccept, Ev),
            case configure_socket(Socket, S) of
                ok -> ok;
                {error, Reason} -> exit({error, Reason}, Timeout)
            end,
            {NewConnCount, UpdatedS} = 
                case spawn_child(S) of
                    {ok, Pid} ->
                        transfer_socket(Socket, Pid),
                        tentative_accept(accept_next, ConnCount + 1, S);
                    {error, Reason} ->
                        error({close_socket, Reason}, Socket),
                        tentative_accept(accept_next, ConnCount, S)
                end,
            loop(NewConnCount, UpdatedS#state{dbg = NewDbg});
        {'inet_async', LSocket, Ref, {error, closed}} -> exit({error, closed}, brutal_kill);
        {'inet_async', LSocket, Ref, {error, Reason}} = Ev -> 
            NewDbg = sys:handle_debug(Dbg, fun write_debug/3, ShouldAccept, Ev),
            {NewConnCount, UpdatedS} = error({inet_accept, Reason}, ConnCount, S),
            loop(NewConnCount, UpdatedS#state{dbg = NewDbg});

        {?MODULE, {From, Tag}, count_connections} ->
            From ! {count_connections, {self(), Tag}, ConnCount},
            loop(ConnCount, S);
        {?MODULE, {From, _Tag}, release_connection} = Ev ->
            NewDbg = sys:handle_debug(Dbg, fun write_debug/3, ShouldAccept, Ev),
            unlink(From),
            {NewConnCount, UpdatedS} = tentative_accept(release_call, ConnCount - 1, S),
            loop(NewConnCount, UpdatedS#state{dbg = NewDbg});

        {'$gen_call', {From, Tag}, count_children} -> 
            count_children(ChildType, From, Tag),
            loop(ConnCount, S);
        {'$gen_call', {From, Tag}, which_children} -> 
            which_children(Mfa, ChildType, From, Tag),
            loop(ConnCount, S);
        {'$gen_call', {From, Tag}, _} -> 
            From ! {Tag, {error, not_implemented}},
            loop(ConnCount, S);

        {'EXIT', Parent, Reason} -> terminate(Reason, Timeout);
        {'EXIT', Pid, _Reason} = Ev -> 
            NewDbg = sys:handle_debug(Dbg, fun write_debug/3, ShouldAccept, Ev),
            delete_child(Pid),
            {NewConnCount, UpdatedS} = tentative_accept(unlink_call, ConnCount - 1, S),
            loop(NewConnCount, UpdatedS#state{dbg = NewDbg});
        {system, From, Msg} -> 
            sys:handle_system_msg(Msg, From, Parent, ?MODULE, Dbg, [ConnCount, S]);

        Msg -> 
            error_logger:info_msg("Received unexpected message: ~p~n", [Msg]),
            loop(ConnCount, S)
    end.

system_continue(_, _, State) ->
    apply(?MODULE, loop, State).

system_terminate(Reason, _, _, [{_, #{timeout := Timeout}}]) ->
    terminate(Reason, Timeout).

system_code_change(Misc, _, _, _) ->
    {ok, Misc}.

system_get_state(State) ->
    {ok, State}.

system_replace_state(Replace, State) ->
    NewState = Replace(State),
    {ok, NewState, NewState}.

spawn_child(#{mfa := {M, F, A}}) ->
    case do_apply(M, F, A) of
        {ok, Pid} -> add_child(Pid);
        {ok, SupPid, _Pid} -> add_child(SupPid);
        Error -> Error
    end.

add_child(Pid) ->
    put(Pid, child),
    {ok, Pid}.

delete_child(Pid) ->
    erase(Pid).

do_apply(Module, Fun, Args) ->
    try apply(Module, Fun, Args)
    catch
        throw:RuntimeError -> {error, RuntimeError};
        error:UndefinedFunctionError -> {error, UndefinedFunctionError};
        _:Msg ->
            Reason = {Msg, erlang:get_stacktrace()},
            {error, Reason}
    end.

count_children(supervisor, To, Tag) ->
    Count = length(erlang:get_keys(child)),
    To ! {Tag, [{specs, 1}, {active, Count},
                {supervisors, Count}, {workers, 0}]}.

count_children(worker, To, Tag) ->
    Count = length(erlang:get_keys(child)),
    To ! {Tag, [{specs, 1}, {active, Count},
                {supervisors, 0}, {workers, Count}]}.

which_children({Mod, _}, ChildType, To, Tag) ->
    Children = lists:map(fun(Pid) -> {Mod, Pid, ChildType, [Mod]} end, erlang:get_keys(child)),
    To ! {Tag, Children}.

exit({error, Reason}, Timeout) ->
    error_logger:info_msg(
      "** Supervisor ~p terminating~n" ++
      "** Reason for termination == ~p~n", [self(), Reason]),
    terminate(Reason, Timeout).

terminate(Reason, Timeout) ->
    ChildPids = erlang:get_keys(child),
    case Timeout of
        brutal_kill ->
            lists:foreach(fun(Pid) -> exit(Pid, kill) end, ChildPids);
        infinity ->
            lists:foreach(fun shutdown_child/1, ChildPids),
            ok = terminate_loop(length(ChildPids));
        _ ->
            lists:foreach(fun shutdown_child/1, ChildPids),
            erlang:send_after(Timeout, self(), kill_all),
            ok = terminate_loop(length(ChildPids))
    end,
    exit(Reason).

shutdown_child(Pid) ->
    erlang:monitor(process, Pid),
    erlang:unlink(Pid),
    exit(Pid, shutdown).

terminate_loop(0) ->
    ok;
terminate_loop(ChildCount) ->
    receive
        {'EXIT', _Pid, _Reason} ->
            terminate_loop(ChildCount);
        {'DOWN', _, process, Pid, _} ->
            erase(Pid),
            terminate_loop(ChildCount - 1);
        kill_all ->
            lists:foreach(fun(Pid) -> exit(Pid, kill) end, erlang:get_keys(child)),
            ok;
        _ ->
            terminate_loop(ChildCount)
    end.

write_debug(Dev, {'inet_async', Sock, _, Msg}, ShouldAccept) ->
    IOString = io_lib:format("~p got event inet_async ~p with socket: ~p, should accept: ~p",
                             [self(), Msg, Sock, ShouldAccept]),
    write_event(IOString, Dev).

write_debug(Dev, {_, {From, _}, remove_connection}, ShouldAccept) ->
    IOString = io_lib:format("~p got event remove_connection from ~p, should accept: ~p",
                             [self(), From, ShouldAccept]),
    write_event(IOString, Dev).

write_debug(Dev, {'EXIT', Pid, Reason}, ShouldAccept) ->
    IOString = io_lib:format("~p got event 'EXIT' from ~p, reason: ~p, should accept: ~p",
                             [self(), Pid, Reason, ShouldAccept]),
    write_event(IOString, Dev).

write_event(Msg, Dev) ->
    io:format(Dev, ["*DBG* "|Msg]).

% Acceptor functions

do_accept(ConnCount, S = #{lsocket := Socket}) ->
    case prim_inet:async_accept(Socket, -1) of
        {ok, Ref} -> {ConnCount, S#{ref => Ref}};
        {error, Ref} ->
            Reason = inet:format_error(Ref),
            error({no_accept, Reason}, S),
            {ConnCount, S}
    end.

configure_socket(Socket, #{lsocket := LSocket}) ->
    inet_db:register_socket(Socket, inet_tcp),
    case prim_inet:getopts(LSocket, []) of
        {ok, Opts} -> prim_inet:setopts(Socket, Opts);
        {error, Reason} ->
            error({close_socket, Reason}, Socket),
            {error, Reason}
    end.

transfer_socket(Socket, Pid) ->
    case gen_tcp:controlling_process(Socket, Pid) of
        ok ->
            Pid ! {ack, Socket};
        {error, Reason} ->
            error({close_socket, Reason}, Socket),
            Pid ! {no_ack, error, Reason}
    end.

tentative_accept(accept_error, ConnCount, S) ->
    do_accept(ConnCount, S);
tentative_accept(accept_next, ConnCount, S = #{max_connections := MaxConnections}) ->
    case acceptor_status(ConnCount, MaxConnections) of
        ok -> do_accept(ConnCount, S);
        paused -> {ConnCount, S#{should_accept => false}}
    end;
tentative_accept(_, ConnCount, S = #{should_accept := ShouldAccept}) when ShouldAccept ->
    {ConnCount, S};
tentative_accept(_, ConnCount, S = #{max_connections := MaxConnections}) ->
    case acceptor_status(ConnCount, MaxConnections) of
        ok -> do_accept(ConnCount, S);
        paused -> {ConnCount, S}
    end.

acceptor_status(ConnCount, MaxConnections) when ConnCount < MaxConnections -> ok;
acceptor_status(ConnCount, MaxConnections) when ConnCount == MaxConnections -> paused.

error({inet_accept, Reason}, ConnCount, S = #{lsocket := LSocket, ref := Ref}) ->
    case Reason of
        emfile ->
            error({no_accept, Reason}, ConnCount, S),
            Msg = {inet_async, LSocket, Ref, {error, 'emfile delay'}},
            erlang:send_after(200, self(), Msg),
            {ConnCount, S};
        'emfile delay' ->
            tentative_accept(accept_error, ConnCount, S);
        _ ->
            error({no_accept, Reason}, ConnCount, S),
            tentative_accept(accept_error, ConnCount, S)
    end;
error({no_accept, Reason}, ConnCount, #{lsocket := Socket} = S) ->
    error_logger:info_msg(
      "** Acceptor error in ~p~n" ++
      "** Accept on socket ~p paused until next call~n" ++
      "** Reason for pause == ~p~n", [self(), Socket, Reason]),
    {ConnCount, S};
error({close_socket, Reason}, Socket) ->
    gen_tcp:close(Socket),
    error_logger:info_msg(
      "** Acceptor error in ~p~n" ++
      "** Socket ~p closed~n" ++
      "** Reason for close == ~p~n", [self(), Socket, {Reason, erlang:get_stacktrace()}]).

% API Helpers

init_ack() ->
    receive
        {ack, Socket} -> {ok, Socket};
        {no_ack, error, Reason} -> {error, Reason}
    end.

count_connections(PoolPid) ->
    Pids =
        lists:map(fun({_, Pid, _, _}) -> Pid end, supervisor:which_children(PoolPid)),
    count_connections(Pids, length(Pids), 0).

count_connections([Pid | Pids], PidCount, 0) ->
    Tag = erlang:monitor(process, Pid),
    Pid ! {?MODULE, {self(), Tag}, count_connections},
    count_connections(Pids, PidCount, 0);
count_connections([], 0, Acc) ->
    Acc;
count_connections([], PidCount, Acc) ->
    Acc1 =
        receive
            {count_connections, {_Pid, Tag}, Count} ->
                erlang:demonitor(Tag),
                Acc + Count;
            {'DOWN', _, process, _Pid, _} -> Acc
        end,
    count_connections([], PidCount - 1, Acc1).

release_connection(AcceptorPid) ->
    AcceptorPid ! {?MODULE, {self(), nil}, release_connection}.
