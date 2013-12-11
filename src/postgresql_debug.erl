%%% vim: ts=4 sts=4 sw=4 expandtab:

-module(postgresql_debug).

-export([
    disable_upstreams/1,
    enable_upstreams/1,
    stop_application/0,
    start_application/0,
    location/0
]).

-include("postgresql_types.hrl").

-spec disable_upstreams(PoolName :: pool_name()) -> 'ok'.
disable_upstreams(PoolName) ->
    local = location(),
    postgresql_pool:debug_disable_upstreams(PoolName).

-spec enable_upstreams(PoolName :: pool_name()) -> 'ok'.
enable_upstreams(PoolName) ->
    local = location(),
    postgresql_pool:debug_enable_upstreams(PoolName).

-spec stop_application() -> 'ok'.
stop_application() ->
    local = location(),
    ok = application:stop(postgresql),
    timer:sleep(500),
    ok.

-spec start_application() -> 'ok'.
start_application() ->
    local = location(),
    ok = application:start(postgresql),
    [ok = postgresql_pool:debug_wait_start(PoolName) || PoolName <- postgresql_config:pool_names()],
    ok.

-spec location() -> 'local' | 'remote'.
location() ->
    LocalNodes = [node()],
    case node_pool:lookup_nodes(e2_gate_pool, node()) of
        LocalNodes ->
            local;
        _ ->
            remote
    end.
