%%% 
%%% Copyright (c) 2010 JackNyfe. All rights reserved.
%%% THIS SOFTWARE IS PROPRIETARY AND CONFIDENTIAL. DO NOT REDISTRIBUTE.
%%%
%%% vim: ts=4 sts=4 sw=4 expandtab:

-module(postgresql).
-export([
    do/2,
    equery/3,
    equery/4,
    equery_local/3,
    equery_local/4,
    equery_nowait/3,
    equery_nowait/4,
    transaction_equery/3,

    make_array/1,
    of_array/1,

    squery/2,
    squery/3,
    squery_local/2,
    squery_local/3,
    squery_nowait/2,
    squery_nowait/3,
    transaction_squery/2,
    upstream_squery/2
]).

-ifdef(TEST).
-export([rpc_indirect/3]).
-endif.

-include("postgresql_types.hrl").

-define(PG_CONNECTOR_CALL_TIMEOUT, 65000). % ngat nodes lookup timeout
-define(HANDLER_MODULE, 'postgresql_pool'). % the module performing actual queries

-spec rpc_indirect(M :: atom(), F :: atom(), Args :: [term(), ...]) -> {'error', rpc_error_reasons()} | term().
rpc_indirect(M, F, Args) ->
    try
        {_Node, Answer} = node_pool:erpc(e2_gate_pool, node(), M, F, Args, ?PG_CONNECTOR_CALL_TIMEOUT),
        case Answer of
            {badrpc, Reason} ->
                {error, {Reason, rpc}};
            Any ->
                Any
        end
    catch
        C:R ->
            {error, {{C, R}, rpc}}
    end.

-spec pgsql_handler_call(Func :: 'equery' | 'equery_nowait' | 'squery' | 'squery_nowait' | 'do', 
                            Args :: [term(), ...]) -> postgresql_transaction_result().
pgsql_handler_call(Func, Args) ->
    case rpc_indirect(?HANDLER_MODULE, Func, Args) of
        {error, _} = Error ->
            log_pgsql_error(Error, Func, Args),
            Error;
        Result -> Result
    end.

%%%%

-spec equery(PoolName :: pool_name(), Query :: pgsql_query(), Params :: pgsql_params()) -> postgresql_query_result().
equery(PoolName, Query, Params) ->
    equery(PoolName, write, Query, Params).

-spec equery(PoolName :: pool_name(), Mode :: exec_mode(), Query :: pgsql_query(), Params :: pgsql_params()) -> postgresql_query_result().
equery(PoolName, Mode, Query, Params) when Mode == read; Mode == write ->
    pgsql_handler_call(equery, [PoolName, Mode, Query, Params]).

-spec equery_nowait(PoolName :: pool_name(), Query :: pgsql_query(), Params :: pgsql_params()) -> postgresql_query_result().
equery_nowait(PoolName, Query, Params) ->
    equery_nowait(PoolName, write, Query, Params).

-spec equery_nowait(PoolName :: pool_name(), Mode :: exec_mode(), Query :: pgsql_query(), Params :: pgsql_params()) -> postgresql_query_result().
equery_nowait(PoolName, Mode, Query, Params) when Mode == read; Mode == write ->
    pgsql_handler_call(equery_nowait, [PoolName, Mode, Query, Params]).

-spec equery_local(PoolName :: pool_name(), Query :: pgsql_query(), Params :: pgsql_params()) -> postgresql_query_result().
equery_local(PoolName, Query, Params) ->
    equery_local(PoolName, write, Query, Params).

-spec equery_local(PoolName :: pool_name(), Mode :: exec_mode(), Query :: pgsql_query(), Params :: pgsql_params()) -> postgresql_query_result().
equery_local(PoolName, Mode, Query, Params) when Mode == read; Mode == write ->
    postgresql_pool:equery(PoolName, Mode, Query, Params).

-spec transaction_equery(Connect :: connection(), Query :: pgsql_query(), Params :: pgsql_params()) -> upstream_transaction_query_result().
transaction_equery(Connect, Query, Params) ->
    postgresql_upstream:transaction_equery(Connect, Query, Params).

%%%%

-spec squery(PoolName :: pool_name(), Query :: pgsql_query()) -> postgresql_query_result().
squery(PoolName, Query) ->
    squery(PoolName, write, Query).

-spec squery(PoolName :: pool_name(), Mode :: exec_mode(), Query :: pgsql_query()) -> postgresql_query_result().
squery(PoolName, Mode, Query) when Mode == read; Mode == write ->
    pgsql_handler_call(squery, [PoolName, Mode, Query]).

-spec squery_nowait(PoolName :: pool_name(), Query :: pgsql_query()) -> postgresql_query_result().
squery_nowait(PoolName, Query) ->
    squery_nowait(PoolName, write, Query).

-spec squery_nowait(PoolName :: pool_name(), Mode :: exec_mode(), Query :: pgsql_query()) -> postgresql_query_result().
squery_nowait(PoolName, Mode, Query) when Mode == read; Mode == write ->
    pgsql_handler_call(squery_nowait, [PoolName, Mode, Query]).

-spec squery_local(PoolName :: pool_name(), Query :: pgsql_query()) -> postgresql_query_result().
squery_local(PoolName, Query) ->
    squery_local(PoolName, write, Query).

-spec squery_local(PoolName :: pool_name(), Mode :: exec_mode(), Query :: pgsql_query()) -> postgresql_query_result().
squery_local(PoolName, Mode, Query) when Mode == read; Mode == write ->
    postgresql_pool:squery(PoolName, Mode, Query).

-spec transaction_squery(Connect :: connection(), Query :: pgsql_query()) -> upstream_transaction_query_result().
transaction_squery(Connect, Query) ->
    postgresql_upstream:transaction_squery(Connect, Query).

-spec upstream_squery(ConnectionInfo :: connection_info(), Query :: pgsql_query()) -> postgresql_query_result().
upstream_squery({PoolName, Upstream, Mode}, Query) ->
    postgresql_upstream:squery({PoolName, Upstream, Mode}, Query).

% If CallBack raised an exception then rollback transaction
% otherwise -> commit transaction
-spec do(PoolName :: pool_name(), MFA :: do_mfa()) -> postgresql_transaction_result().
do(PoolName, {_Module, _Function, _Args} = MFA) ->
    pgsql_handler_call(do, [PoolName, write, MFA]).

-spec make_array(List :: [term()]) -> nonempty_string().
make_array([]) -> "{}";
make_array(List) ->
    EscapedList = [quote_utils:quote(type_utils:to_list(E)) || E <- List],
    lists:flatten(["{\"", string:join(EscapedList, "\",\""), "\"}"]).

% TODO: use correct version from http://code.google.com/p/erlang-psql-driver/source/browse/trunk/src/psql_lib.erl
-spec of_array(Data :: nonempty_string() | binary()) -> [binary()].
of_array(A) when is_binary(A) -> of_array(binary_to_list(A));
of_array("{}") -> [];
of_array("{" ++ Data) ->
    [list_to_binary(quote_utils:unquote(V)) || V <- string:tokens(string:strip(Data, right, $}), ",")].

% private functions

log_pgsql_error({error, Reason} = Error, Func, [PoolName | _] = Args) ->
    save_pgsql_error_stats(Reason, PoolName),
    LogFormat = "PoolName: ~p; Func: ~p~nArgs:~n~p~nError:~n~p",
    LogData = [PoolName, Func, Args, Error],
    lager:error(LogFormat, LogData).
    
save_pgsql_error_stats(Reason, PoolName) ->
    ErrorName = postgresql_errors:internal_error_reason_to_string(Reason),
    C = "postgresql.errors." ++ stats:safe_string(ErrorName),
    stats:notify({atom_to_list(PoolName), C, $e}, 1, meter).
