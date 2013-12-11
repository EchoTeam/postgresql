%%% 
%%% Copyright (c) 2011 JackNyfe. All rights reserved.
%%% THIS SOFTWARE IS PROPRIETARY AND CONFIDENTIAL. DO NOT REDISTRIBUTE.
%%%
%%% vim: ts=4 sts=4 sw=4 expandtab:

-module(postgresql_upstream).

-export([
    do/2,
    do/3,
    equery/3,
    name/1,
    squery/2,
    start_link/5,
    transaction_equery/3,
    transaction_squery/2
]).

-include("postgresql_types.hrl").

-define(GET_CONNECTION_TIMEOUT, 5000).

-spec start_link(PoolName :: pool_name(), UpstreamName :: upstream_name(), Mode :: exec_mode(),
                    Connections :: pos_integer(), Credentials :: [proplists:property()]) ->
                        {'ok', pid()} | {'error', term()}.
start_link(PoolName, UpstreamName, Mode, Connections, Credentials) ->
    Name = name({PoolName, UpstreamName, Mode}),
    PGOpts = [{connect_timeout, ?GET_CONNECTION_TIMEOUT - 100} | Credentials],
    lager:info("[start_link] Calling pgsql_pool:start_link(~p, ~p,~n ~p)", [Name, Connections, PGOpts]),
    pgsql_pool:start_link(Name, Connections, PGOpts).

-spec name(ConnectionInfo :: connection_info()) -> atom().
name({PoolName, UpstreamName, Mode} = _ConnectionInfo) ->
    list_to_atom(string:join([atom_to_list(Term) || Term <- [?MODULE, PoolName, UpstreamName, Mode]], ":")).

-spec equery(ConnectionInfo :: connection_info(), Query :: pgsql_query(), Params :: pgsql_params()) -> upstream_query_result().
equery(ConnectionInfo, Query, Params) ->
    with_connection(ConnectionInfo, fun(Connection) ->
        transaction_equery(Connection, Query, Params)
    end, ?GET_CONNECTION_TIMEOUT).

-spec transaction_equery(Connection :: connection(), Query :: pgsql_query(), Params :: pgsql_params()) -> upstream_transaction_query_result().
transaction_equery(Connection, Query, Params) ->
    call_pgsql(equery, [Connection, Query, Params]).

-spec squery(ConnectionInfo :: connection_info(), Query :: pgsql_query()) -> upstream_query_result().
squery(ConnectionInfo, Query) ->
    with_connection(ConnectionInfo, fun(Connection) ->
        transaction_squery(Connection, Query)
    end, ?GET_CONNECTION_TIMEOUT).

-spec transaction_squery(Connection :: connection(), Query :: pgsql_query()) -> upstream_transaction_query_result().
transaction_squery(Connection, Query) ->
    call_pgsql(squery, [Connection, Query]).

-spec do(ConnectionInfo :: connection_info(), Callback :: upstream_transaction_callback(), Timeout :: timeout()) -> upstream_transaction_result().
do(ConnectionInfo, Callback) ->
    do(ConnectionInfo, Callback, ?GET_CONNECTION_TIMEOUT).
do(ConnectionInfo, Callback, Timeout) ->
    try
        with_connection(ConnectionInfo, fun(Connection) ->
            case pgsql:with_transaction(Connection, Callback) of
                {rollback, Reason} ->
                    {error, Reason};
                Any ->
                    Any
            end
        end, Timeout)
    catch
        % sometimes error in transaction can terminate postgresql-pool
        % when it happens the gen-server process will terminate
        % but pgsql module will try to do "ROLLBACK" query after it
        exit:{noproc, _} = Reason ->
            lager:warning("[postgresql_upstream:do] Timeout: ~p; Got exception: ~p", [Timeout, Reason]),
            {error, {closed, pgsql_transaction}}
    end.

% private functions

-spec call_pgsql(Fun :: 'equery' | 'squery', Args :: [term(), ...]) -> upstream_transaction_query_result().
call_pgsql(Fun, Args) ->
    try erlang:apply(pgsql, Fun, Args) of
        {error, Reason} ->
            {error, {Reason, pgsql}};
        Any ->
            Any
    catch
        C:R ->
            lager:warning("[pgsql:~s] Args: ~p; Got exception: ~p:~p", [Fun, Args, C, R]),
            case {C, R} of
                {throw, {error, Reason}} ->
                    {error, {Reason, pgsql}};
                {Class, Reason} ->
                    {error, {{Class, Reason}, pgsql}}
            end
    end.

-spec with_connection(ConnectionInfo :: connection_info(), Fun :: upstream_transaction_callback(), Timeout :: timeout()) ->
                        {'error', pgsql_pool_error_reason()} | term().
with_connection(ConnectionInfo, Fun, Timeout) ->
    case get_connection(ConnectionInfo, Timeout) of
        {ok, Connection} ->
            try
                Fun(Connection)
            after
                return_connection(ConnectionInfo, Connection)
            end;
        {error, _Reason} = Error ->
            Error
    end.

-spec get_connection(ConnectionInfo :: connection_info(), Timeout :: timeout()) ->
                        {'error', pgsql_pool_error_reason()} | {'ok', connection()}.
get_connection(ConnectionInfo, Timeout) ->
    try gen_server:call(name(ConnectionInfo), get_connection_nowait, Timeout) of
        {ok, _} = Ok ->
            Ok;
        {error, busy} ->
            {error, {busy, pgsql_pool}}
    catch
        C:R ->
            lager:warning("[postgresql_upstream:get_connection] Timeout: ~p; Got exception: ~p:~p", [Timeout, C, R, self()]),
            {error, {timeout, pgsql_pool}}
    end.

-spec return_connection(ConnectionInfo :: connection_info(), Connection :: connection()) -> 'ok'.
return_connection(ConnectionInfo, Connection) ->
    gen_server:cast(name(ConnectionInfo), {return_connection, Connection}).
