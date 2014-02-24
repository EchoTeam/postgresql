%%% vim: set ts=4 sts=4 sw=4 expandtab:
-module(postgresql_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, reconfigure/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    Spec = case postgresql_config:run_on_node() of
        false -> [];
        true -> zookeeper() ++ postgresql_pools()
    end,
    {ok, {{one_for_one, 10, 10}, Spec}}.


postgresql_pools() ->
	[postgresql_pool_sup:supervisor_spec(PoolName) 
        || PoolName <- postgresql_config:pool_names()].

zookeeper() ->
	[	
		{
			postgresql_config_zk,
			{postgresql_config_zk, start_link, []},
			permanent, 10000, worker, [postgresql_config_zk]
		}
	].

reconfigure() ->
    superman:reconfigure_supervisor_init_args(?MODULE, []).
