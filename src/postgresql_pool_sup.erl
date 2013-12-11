%%%
%%% Copyright (c) 2011 JackNyfe. All rights reserved.
%%% THIS SOFTWARE IS PROPRIETARY AND CONFIDENTIAL. DO NOT REDISTRIBUTE.
%%%
%%% vim: ts=4 sts=4 sw=4 expandtab:

-module(postgresql_pool_sup).

-behavior(supervisor).

-export([
    init/1,
    name/1,
    pool_status/1,
    start/1,
    start_link/1,
    status/1,
    stop/1,
    supervisor_spec/1,
    upstreams_status/1
]).

name(PoolName) when is_atom(PoolName) ->
    list_to_atom(atom_to_list(?MODULE) ++ ":" ++ atom_to_list(PoolName)).

start_link(PoolName) ->
    lager:info("[start_link] Starting PostgreSQL pool supervisor: ~p", [PoolName]),
    supervisor:start_link({local, name(PoolName)}, ?MODULE, PoolName).

init(PoolName) ->
    Upstreams = postgresql_config:pool_upstreams(PoolName),
    {ok, {
            {one_for_all, 10, 10},
            pool_upstreams_spec(PoolName, Upstreams) ++
            [{
                postgresql_pool:name(PoolName),
                {postgresql_pool, start_link, [PoolName, Upstreams]},
                permanent, 10000, worker,
                [postgresql_pool]
            }]
         }
    }.

upstreams_status(PoolName) ->
    [ begin
            UpstreamPid = postgresql_upstream:name({PoolName, UpstreamName, Mode}),
            {UpstreamPid, gen_server:call(UpstreamPid, status)}
      end || {UpstreamName, Mode, _, _} <- postgresql_config:upstreams_info({pool, PoolName}) ].

pool_status(PoolName) ->
    postgresql_pool:status(PoolName).

status(PoolName) ->
    [{pool, pool_status(PoolName)},
     {upstreams, upstreams_status(PoolName)}].

stop(PoolName) ->
    lager:info("[stop] Stopping PostgreSQL pool supervisor: ~p", [PoolName]),
    supervisor:terminate_child(postgresql_sup, name(PoolName)).

start(PoolName) ->
    lager:info("[start] Starting PostgreSQL pool supervisor: ~p", [PoolName]),
    supervisor:restart_child(postgresql_sup, name(PoolName)).

supervisor_spec(PoolName) ->
	{
		name(PoolName),
		{?MODULE, start_link, [PoolName]},
		permanent, 10000, supervisor,
		[?MODULE]
	}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

pool_upstreams_spec(PoolName, Upstreams) ->
    [ {
            postgresql_upstream:name({PoolName, UpstreamName, Mode}),
            {postgresql_upstream, start_link, [PoolName, UpstreamName, Mode, Connections, Credentials]},
            permanent, 10000, worker,
            [postgresql_upstream]
      } || {UpstreamName, Mode, Connections, Credentials} <- postgresql_config:upstreams_info({upstreams, Upstreams})
    ].
