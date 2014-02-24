%%% vim: ts=4 sts=4 sw=4 expandtab:

-module(postgresql_config).

-export([
    % TODO: change functions names and configuration variables names
    customer_pool_names/0,
    customers_pool_name/0,
    pool_names/0,
    pools_upstreams/0,
    run_on_node/0,
    % internal for postgresql application
    master_upstream/1,
    pool_upstreams/1,
    pools_upstreams_connections/0,
    upstreams_info/1
]).

-include("postgresql_types.hrl").

%%
%% Public API
%%

-spec customer_pool_names() -> [pool_name(), ...].
customer_pool_names() -> get_pools(customer_pools).

-spec customers_pool_name() -> pool_name().
customers_pool_name() -> get_pools(customers_pool).

-spec pool_names() -> [pool_name(), ...].
pool_names() ->
    [get_pools(accounts_pool), customers_pool_name()]
    ++ customer_pool_names().

-spec pools_upstreams() -> [{pool_name(), [upstream_config(), ...]}, ...].
pools_upstreams() ->
    lists:map(fun(PoolName) ->
        {PoolName, pool_upstreams(PoolName)}
    end, pool_names()).

-spec master_upstream(PoolUpstreams :: [upstream_config(), ...]) -> upstream_config().
master_upstream(PoolUpstreams) ->
    [MasterUpstream] = [{UpstreamName, Props} || {UpstreamName, Props} <- PoolUpstreams,
                                                 proplists:get_value('write_connections', Props) /= 0],
    MasterUpstream.

pool_upstream_connections({_UpstreamName, UpstreamInfo}) ->
    ReadConnections =  proplists:get_value('read_connections', UpstreamInfo),
    WriteConnections =  proplists:get_value('write_connections', UpstreamInfo),
    {      
        proplists:get_value(host, proplists:get_value('credentials', UpstreamInfo, [])),
        ReadConnections + WriteConnections
    }.

pools_upstreams_connections() ->
    lists:flatmap(fun(PoolName) ->
        [pool_upstream_connections(Upstream) || Upstream <- pool_upstreams(PoolName)]
    end, pool_names()).


% internal functions for postgresql application

-spec pool_upstreams(PoolName:: pool_name()) -> [upstream_config(), ...].
pool_upstreams(PoolName) -> postgresql_config_zk:pool_upstreams(PoolName).

run_on_node() ->
    {ok, Config} = application:get_env(postgresql, nodes),
    lists:member(node(), Config).

upstreams_info({pool, PoolName}) ->
    upstreams_info({upstreams, pool_upstreams(PoolName)});

upstreams_info({upstreams, Upstreams}) ->
    lists:append([upstream_info(Upstream) || Upstream <- Upstreams]).

%%
%% Internal functions
%%

-spec get_pools(PoolType :: pool_type()) -> [pool_name(), ...] | pool_name().
get_pools(PoolType) ->
    {ok, Config} = application:get_env(postgresql, pools),
    proplists:get_value(PoolType, Config).

upstream_info({UpstreamName, UpstreamInfo} = _Upstream) ->
    lists:foldl(
        fun(Mode, Acc) ->
                ModeConnectionsAttr = list_to_atom(atom_to_list(Mode) ++ "_connections"),
                ModeConnections = proplists:get_value(ModeConnectionsAttr, UpstreamInfo, 0),
                Credentials = proplists:get_value(credentials, UpstreamInfo, []),
                case ModeConnections of
                    0 -> Acc;
                    _ -> [{UpstreamName, Mode, ModeConnections, Credentials} | Acc]
                end
        end,
        [], [write, read]).
