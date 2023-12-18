-module(hermanmunster).

-export([start_link/1, init_ack/0, connections/1, release_connection/1, child_spec/1, init/1]).

%% API

start_link(Opts) ->
    Opts1 = parse_opts(Opts),
    supervisor:start_link({local, Opts1#opts.name}, ?MODULE, Opts1).

init_ack() ->
    tcp_sup:init_ack().

connections(SupervisorPid) ->
    tcp_sup:count_connections(SupervisorPid).

release_connection(AcceptorPid) ->
    tcp_sup:release_connection(AcceptorPid).

child_spec(Opts) ->
    Opts1 = parse_opts(Opts),
    supervisor:child_spec(?MODULE, [Opts1], #{id => Opts1#opts.name}).

%% Supervisor

init(Opts) ->
    Children = children(Opts),
    supervisor:init(Children, {one_for_all, 0, 1}).

children(Opts) ->
    lists:map(fun({Opts1, Id}) ->
        supervisor:child_spec(tcp_sup, [Opts1], #{id => Id})
    end, children_params(Opts)).

children_params(Opts) ->
    {Name, Opts1} = lists:keydelete(name, 1, Opts),
    {NumberOfAcceptors, Opts2} = lists:keydelete(number_of_acceptors, 1, Opts1),
    {MaxConnections, Opts3} = lists:keydelete(maximum_connections, 1, Opts2),
    ConnectionLimits = connection_limits(NumberOfAcceptors, MaxConnections),
    Zipped = lists:zip(ConnectionLimits, acceptor_identifiers(Name, NumberOfAcceptors)),
    lists:map(fun(X) -> {proplists:set_property(maximum_connections, X, Opts3), X} end, Zipped).

%% Helpers

parse_opts(Opts) ->
    Opts1 = validate_opts(Opts),
    Name = proplists:get_value(name, Opts1, ?MODULE),
    MaxConnections = proplists:get_value(maximum_connections, Opts1),
    NumberOfAcceptors = proplists:get_value(number_of_acceptors, Opts1),
    Socket = proplists:get_value(socket, Opts1),
    ChildSpec = proplists:get_value(child_spec, Opts1),
    Debug = proplists:get_value(debug, Opts1, []),
    {_, {M, F, A}, _, Shutdown, ChildType, _} = ChildSpec,
    #{name => Name, maximum_connections => MaxConnections, number_of_acceptors => NumberOfAcceptors,
      socket => Socket, mfa => {M, F, A}, shutdown => Shutdown, child_type => ChildType, debug => Debug}.

validate_opts(Opts) ->
    Required = [maximum_connections, number_of_acceptors, child_spec],
    case lists:all(fun(Key) -> lists:member(Key, Opts) end, Required) of
        false -> exit("Missing required parameter(s).");
        true ->
            case proplists:is_defined(socket, Opts) of
                true -> Opts;
                false -> 
                    case proplists:is_defined(listen_port, Opts) of
                        true -> add_listener_socket(Opts);
                        false -> exit("Missing socket parameter(s).")
                    end
            end
    end.

add_listener_socket(Opts) ->
    SockOpts = [binary, {packet, raw}, {active, false}, {reuseaddr, true}],
    {ok, Socket} = gen_tcp:listen(proplists:get_value(listen_port, Opts), SockOpts),
    proplists:put_new(socket, Socket, Opts).

connection_limits(NumberOfAcceptors, MaxConnections) ->
    Remainder = rem(MaxConnections, NumberOfAcceptors),
    Quotient = round(div(MaxConnections, NumberOfAcceptors)),
    Duplicate0 = lists:duplicate(NumberOfAcceptors - Remainder, 0),
    Duplicate1 = lists:duplicate(Remainder, 1),
    lists:map(fun(X) -> X + Quotient end, Duplicate0 ++ Duplicate1).

acceptor_identifiers(Name, NumberOfAcceptors) ->
    lists:map(fun(X) -> {Name, X} end, lists:seq(1, NumberOfAcceptors)).
