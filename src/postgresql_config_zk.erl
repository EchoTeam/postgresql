%%%
%%% Copyright (c) 2012 JackNyfe. All rights reserved.
%%% THIS SOFTWARE IS PROPRIETARY AND CONFIDENTIAL. DO NOT REDISTRIBUTE.
%%%
%%% vim: ts=4 sts=4 sw=4 expandtab:

-module(postgresql_config_zk).

-behaviour(gen_server).

% Public API
-export([
    disable_upstream/2,
    get_pool_json/1,
    get_pools_json/0,
    enable_upstream/2,
    pool_upstreams/1,
    pools/0,
    restart_pool/1,
    restart_pools/0,
    set_connections/4,
    set_pools_json/1,
    set_upstream_props/3,
    update_upstreams/2
]).

% gen_server plumbing
-export([
    code_change/3,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    init/1,
    start_link/0,
    terminate/2
]).

-record(state, { pools = [], pools_info = [] }).

-define(REFETCH_AFTER, 2000). % ms.
-define(PGSQL_POOL_RESTART_AFTER, 1000). % ms.

%%
%% Public API
%%

disable_upstream(PoolName, UpstreamName) ->
    set_upstream_props(PoolName, UpstreamName, [{'disabled', true}]).

enable_upstream(PoolName, UpstreamName) ->
    set_upstream_props(PoolName, UpstreamName, [{'disabled', false}]).

get_pool_json(PoolName) ->
    gen_server:call(?MODULE, {get_pool_json, PoolName}).

get_pools_json() ->
    gen_server:call(?MODULE, get_pools_json).

pool_upstreams(PoolName) ->
    gen_server:call(?MODULE, {pool_upstreams, PoolName}).

pools() ->
    gen_server:call(?MODULE, pools).

restart_pools() ->
    gen_server:call(?MODULE, restart_pools).

restart_pool(PoolName) ->
    gen_server:call(?MODULE, {restart_pool, PoolName}).

set_connections(read, PoolName, UpstreamName, ConnectionsNum) ->
    set_upstream_props(PoolName, UpstreamName,
                                    [{'read_connections', ConnectionsNum}]);
set_connections(write, PoolName, UpstreamName, ConnectionsNum) ->
    set_upstream_props(PoolName, UpstreamName,
                                    [{'write_connections', ConnectionsNum}]).

set_pools_json(JSON) ->
    gen_server:call(?MODULE, {set_pools_json, JSON}).

set_upstream_props(PoolName, UpstreamName, Props) ->
    case check_upstream_props(PoolName, UpstreamName, Props) of
        ok ->
            gen_server:call(?MODULE,
                        {set_upstream_props, PoolName, UpstreamName, Props});
        Error -> Error
    end.

start_link() ->
    lager:info("[start_link] Starting PostgreSQL ZooKeeper configuration manager."),
    Pools = postgresql_config:pool_names(),
    gen_server:start_link({local, ?MODULE}, ?MODULE, Pools, []).

update_upstreams(PoolName, PoolUpstreams) ->
    gen_server:call(?MODULE, {update_upstreams, PoolName, PoolUpstreams}).

%%
%% gen_server callbacks
%%

init(Pools) ->
    process_flag(trap_exit, true),
    PoolsInfo = fetch_pools_info(self(), Pools, false),
    {ok, #state{pools = Pools, pools_info = PoolsInfo}}.

terminate(Reason, _State) ->
    lager:warning("[terminate] Terminating. Reason: ~p", [Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_call({get_pool_json, PoolName}, _From, State) ->
    {reply, pool_json(PoolName), State};

handle_call(get_pools_json, _From, #state{ pools = Pools } = State) ->
    PoolsJSON = [ "[", string:join(pools_json(Pools), ", "), "]" ],
    {reply, PoolsJSON, State};

handle_call({pool_upstreams, PoolName}, _From, #state{ pools_info = PoolsInfo } = State) ->
    PoolUpstreams = lists:keysort(1, proplists:get_value(PoolName, PoolsInfo, [])),
    {reply, PoolUpstreams, State};

handle_call(pools, _From, #state{ pools = Pools} = State) ->
    {reply, Pools, State};

handle_call({set_pools_json, JSON}, _From, State) ->
    {reply, set_pools_json_ll(JSON), State};

handle_call(restart_pools, _From, #state{ pools = Pools } = State) ->
    [schedule_postgresql_pool_restart(self(), PoolName) || PoolName <- Pools],
    {reply, ok, State};

handle_call({restart_pool, PoolName}, _From, #state{ pools = Pools } = State) ->
    Reply = case lists:member(PoolName, Pools) of
        true ->
            schedule_postgresql_pool_restart(self(), PoolName),
            ok;
        false -> {error, unknown_pool}
    end,
    {reply, Reply, State};

handle_call({set_upstream_props, PoolName, UpstreamName, Props}, _From, #state{ pools_info = PoolsInfo } = State) ->
    lager:info("[handle_call] set_upstream_props. Setting ~p for ~p:~p", [Props, PoolName, UpstreamName]),
    PoolUpstreams = proplists:get_value(PoolName, PoolsInfo, []),
    Upstream = proplists:get_value(UpstreamName, PoolUpstreams, []),
    Reply = case Upstream of
        [] -> {error, incorrect_upstream_or_pool_name};
        _ ->
            NewUpstream = lists:foldl(fun({Field, Value}, Acc) ->
                    [{Field, Value} | proplists:delete(Field, Acc)]
                end, Upstream, Props),
            NewPoolUpstreams = [{UpstreamName, NewUpstream} | proplists:delete(UpstreamName, PoolUpstreams)],
            update_pool_upstreams(PoolName, NewPoolUpstreams)
    end,
    {reply, Reply, State};

handle_call({update_upstreams, PoolName, PoolUpstreams}, _From, State) ->
    lager:info("[handle_call] update_upstreams. Updating upstreams for ~p to:~n~p", [PoolName, PoolUpstreams]),
    Reply = update_pool_upstreams(PoolName, PoolUpstreams),
    {reply, Reply, State}.

handle_cast(Request, State) ->
    lager:warning("[handle_cast] Unknown cast: ~p", [Request]),
    {noreply, State}.

handle_info({fetch_pools_info, RestartPgSQLPools}, #state{ pools = Pools } = State) ->
    lager:info("[handle_info] fetch_pools_info. RestartPgSQLPools: ~p", [RestartPgSQLPools]),
    PoolsInfo = fetch_pools_info(self(), Pools, RestartPgSQLPools),
    {noreply, State#state{pools_info = PoolsInfo}};

handle_info({restart_postgresql_pool, PoolName}, State) ->
    lager:info("[handle_info] restart_postgresql_pool ~p", [PoolName]),
    spawn(fun() -> restart_postgresql_pool(PoolName) end),
    {noreply, State};

handle_info({{watch_postgresql_config_changed, PoolName}, _ZKData}, #state{ pools_info = PoolsInfo } = State) ->
    lager:info("[handle_info] watch_postgresql_config_changed. Got a watch notification for: ~p", [PoolName]),
    NewPoolsInfo = case get_pool_upstreams(PoolName) of
        {ok, Upstreams} ->
            lager:info("[handle_info] watch_postgresql_config_changed. Successfully fetched up to date postgresql pool info."),
            [{PoolName, Upstreams} | proplists:delete(PoolName, PoolsInfo)];
        {error, _} = Error ->
            lager:warning("[handle_info] watch_postgresql_config_changed. Failed to fetch up to date PostgreSQL config: ~p. Use old PostgreSQL info.", [Error]),
            PoolsInfo
    end,
    lager:info("[handle_info] watch_postgresql_config_changed. Setup new watch for ~p", [PoolName]),
    ok = zk_setup_watch(self(), PoolName),
    schedule_postgresql_pool_restart(self(), PoolName),
    {noreply, State#state{pools_info = NewPoolsInfo}};

handle_info({watchlost, _, _}, State) ->
    lager:info("[handle_info] watchlost. Received watch lost message."),
    {noreply, State};

handle_info({zk_connection, down}, State) ->
    lager:warning("[handle_info] zk_connection. Received ZK connection down message."),
    {noreply, State};

handle_info({zk_connection, up}, State) ->
    lager:warning("[handle_info] zk_connection. Received ZK connection up message. Schedule ZK postgresql info refetching."),
    schedule_refetching(self()),
    {noreply, State};

handle_info(Info, State) ->
    lager:warning("[handle_info] Unknown message: ~p", [Info]),
    {noreply, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

set_pools_json_ll(JSON) ->
    lager:info("[set_pools_json_ll] Updating ZK pools information with data from JSON:~n~p", [JSON]),
    case echo_json:decode(JSON) of
        {ok, Pools, _} when is_list(Pools) ->
            lager:info("[set_pools_json_ll] Parsed pools info: ~p", [Pools]),
            try pools_upstreams_from_json(Pools) of
                PoolsUpstreams ->
                    lager:info("[set_pools_json_ll] Successfully converted JSON to pools upstreams info:~n~p", [PoolsUpstreams]),
                    ZKUpdateResponses = [{PoolName, update_pool_upstreams(PoolName, Upstreams)} || {PoolName, Upstreams} <- PoolsUpstreams],
                    lager:info("[set_pools_json_ll] ZK pool update responses: ~p", [ZKUpdateResponses]),
                    case [{PoolName, ZKResponse} || {PoolName, ZKResponse} <- ZKUpdateResponses, ZKResponse /= ok] of
                        [] -> ok;
                        Errors -> {error, {failed_zk_update, Errors}}
                    end
            catch C:R ->
                lager:warning("[set_pools_json_ll] Failed to convert JSON to PostgreSQL config: ~p:~p", [C, R]),
                {error, {json_conversion_failure, {C, R}}}
            end;
        {ok, _, _} -> {error, incorrect_json_format};
        Error ->
            lager:warning("[set_pools_json_ll] Failed to parse JSON data: ~p", [Error]),
            {error, {json_decoding_failure, Error}}
    end.

fetch_pools_info(Server, Pools, RestartPgSQLPools) ->
    lager:info("[fetch_pools_info] Fetching PostgreSQL pools info from ZooKeeper."),
    {Errors, PoolsInfo} = get_pools_upstreams(Pools),
    lager:info("[fetch_pools_info] ZK fetch completed. Errors: ~p; PoolsInfo: ~p", [Errors, PoolsInfo]),
    case Errors of
        [] ->
            lager:info("[fetch_pools_info] Successfully received ZK data. Setup watches."),
            setup_watches(Server, Pools),
            case RestartPgSQLPools of
                true ->
                    lager:info("[fetch_pools_info] Schedule PostgreSQL pools restart."),
                    [schedule_postgresql_pool_restart(self(), PoolName) || PoolName <- Pools];
                false -> ok
            end;
        _ ->
            lager:warning("[fetch_pools_info] Got errors when retrieving postgresql info from zookeeper. Schedule one more fetch."),
            schedule_refetching(Server)
    end,
    lager:info("[fetch_pools_info] Successfully fetched up to date ZK info and setup watches."),
    PoolsInfo.

get_pools_upstreams(Pools) ->
    {Errors, PoolsInfo} = lists:foldl(
        fun(PoolName, {ErrorsAcc, PoolsInfoAcc}) ->
            case get_pool_upstreams(PoolName) of
                {ok, Upstreams} ->
                    {ErrorsAcc, [{PoolName, Upstreams} | PoolsInfoAcc]};
                {error, _} ->
                    {[PoolName | ErrorsAcc], [{PoolName, []} | PoolsInfoAcc]}
            end
        end,
        {[], []}, Pools),
    {Errors, PoolsInfo}.

get_pool_upstreams(PoolName) ->
    case zk_get_pool_data(PoolName) of
        {ok, UpstreamsJSON} -> json_to_upstreams(UpstreamsJSON);
        Error -> Error
    end.

update_pool_upstreams(PoolName, Upstreams) ->
    try upstreams_to_json(Upstreams) of
        UpstreamsJSON -> zk_update_pool_data(PoolName, UpstreamsJSON)
    catch C:R ->
        {error, {failed_config_to_json_conversion, {C, R}}}
    end.

setup_watches(Server, Pools) ->
    [ok = zk_setup_watch(Server, PoolName) || PoolName <- Pools].

%%
%% JSON handling
%%

pool_json(PoolName) ->
    case zk_get_pool_data(PoolName) of
        {ok, Upstreams} ->
            ["{\"", atom_to_list(PoolName), "\": ", Upstreams, "}"];
        _ -> []
    end.

pools_json(Pools) ->
    lists:foldl(
        fun(PoolName, Acc) ->
            case pool_json(PoolName) of
                [] -> Acc;
                PoolJSON -> [PoolJSON | Acc]
            end
        end,
        [], Pools).

json_to_upstreams(Data) ->
    lager:info("[json_to_upstreams] Raw data: ~p", [Data]),
    case echo_json:decode(Data) of
        {ok, JSON, _} ->
            lager:info("[json_to_upstreams] Parsed JSON: ~p", [JSON]),
            try upstreams_from_json(JSON) of
                Upstreams ->
                    lager:info("[json_to_upstreams] Successfully converted JSON to PostgreSQL config"),
                    {ok, Upstreams}
            catch C:R ->
                lager:warning("[json_to_upstreams] Failed to convert JSON to PostgreSQL config: ~p:~p", [C, R]),
                {error, {json_conversion_failure, {C, R}}}
            end;
        Error ->
            lager:warning("[json_to_upstreams] Failed to parse data as JSON: ~p", [Error]),
            {error, {json_decoding_failure, Error}}
    end.

upstreams_to_json(Upstreams) -> list_to_binary(quote_utils:simpleObject(Upstreams)).

pools_upstreams_from_json(Pools) ->
    [ {list_to_atom(PoolName), upstreams_from_json(UpstreamsObj)} || {obj, [{PoolName, UpstreamsObj}]} <- Pools ].

upstreams_from_json({obj, Upstreams}) ->
    [upstream_from_json(Upstream) || Upstream <- Upstreams].

upstream_from_json({UpstreamName, {obj, UpstreamPropList}}) ->
    {list_to_atom(UpstreamName), [upstream_field_from_json(UField, UpstreamPropList) || UField <- ["read_connections", "write_connections", "disabled", "credentials"]]}.

upstream_field_from_json("read_connections", UpstreamPropList) ->
    {'read_connections', proplists:get_value("read_connections", UpstreamPropList, 0)};

upstream_field_from_json("write_connections", UpstreamPropList) ->
    {'write_connections', proplists:get_value("write_connections", UpstreamPropList, 0)};

upstream_field_from_json("disabled", UpstreamPropList) ->
    IsDisabled = case proplists:get_value("disabled", UpstreamPropList, false) of
        true -> true;
        false -> false;
        _ -> throw({incorrect_field_format, disabled})
    end,
    {'disabled', IsDisabled};

upstream_field_from_json("credentials", UpstreamPropList) ->
    {'credentials', credentials_from_json(proplists:get_value("credentials", UpstreamPropList, {obj, []}))}.

credentials_from_json({obj, CredentialsPL}) ->
    Credentials = [{list_to_atom(CField), proplists:get_value(CField, CredentialsPL, <<"">>)} || CField <- ["host", "port", "database", "username", "password"]],
    NormalizedCredentials = [{Field, normalized_credentials_value(Field, Value)} || {Field, Value} <- Credentials],
    ValidatedCredentials = [{Field, Value} || {Field, Value} <- NormalizedCredentials, valid_credentials_value(Field, Value) =:= true],
    lager:info("[credentials_from_json] Credentials:~n~p~nNormalized:~n~p~nValidated:~n~p", [Credentials, NormalizedCredentials, ValidatedCredentials]),
    % All the fields should be valid.
    ValidatedCredentials = NormalizedCredentials,
    ValidatedCredentials.

normalized_credentials_value(port, Value) -> type_utils:to_integer(Value);
normalized_credentials_value(Field, Value) when Field =:= host; Field =:= database; Field =:= username; Field =:= password ->
    type_utils:to_list(Value).

valid_credentials_value(port, Value) -> Value > 0;
valid_credentials_value(Field, Value) when Field =:= host; Field =:= database; Field =:= username; Field =:= password ->
    Value =/= "".

%%
%% ZooKeeper interface
%%

pool_name_to_zk_node(PoolName) ->
    "/postgresql/" ++ atom_to_list(PoolName).

zk_safely_execute(Fun, Args, Callback) ->
    lager:info("[zk_safely_execute] Calling '~p' with args: ~p", [Fun, Args]),
    try erlang:apply(echo_zookeeper, Fun, Args) of
        Result -> Callback(Result)
    catch C:R ->
        lager:warning("[zk_safely_execute] Failed to call '~p' with args: ~p~n~p:~p", [Fun, Args, C, R]),
        {error, {zk_exception, {C, R}}}
    end.

zk_pool_exists(PoolName) ->
    lager:info("[zk_pool_exists] Checking existence of ~p", [PoolName]),
    ZKNode = pool_name_to_zk_node(PoolName),
    zk_safely_execute(
        exists,
        [ZKNode],
        fun({ok, _}) ->
                lager:info("[zk_pool_exists] Pool (~p) exists", [PoolName]),
                {ok, true};
           ({error, no_dir}) ->
                lager:info("[zk_pool_exists] Pool (~p) does not exist", [PoolName]),
                {ok, false};
           (ZKResponse) ->
                lager:warning("[zk_pool_exists] Failed to check pool (~p) existence: ~p", [PoolName, ZKResponse]),
                {error, ZKResponse}
        end).

zk_create_pool(PoolName, Data) ->
    lager:info("[zk_create_pool] Creating ZK node for ~p", [PoolName]),
    ZKNode =  pool_name_to_zk_node(PoolName),
    zk_safely_execute(
        create,
        [ZKNode, Data],
        fun({ok, _}) ->
                lager:info("[zk_create_pool] Successfully created node for pool: ~p in ZooKeeper:~n~p", [PoolName, Data]),
                ok;
           (ZKResponse) ->
                lager:warning("[zk_create_pool] Failed to create node for pool: ~p in ZooKeeper: ~p", [PoolName, ZKResponse]),
                {error, ZKResponse}
        end).

zk_set_pool_data(PoolName, Data) ->
    lager:info("[zk_set_pool_data] Setting pool data for ~p", [PoolName]),
    ZKNode =  pool_name_to_zk_node(PoolName),
    zk_safely_execute(
        set,
        [ZKNode, Data],
        fun({ok, _}) ->
                lager:info("[zk_set_pool_data] Successfully set data for pool: ~p in ZooKeeper", [PoolName]),
                ok;
            (ZKResponse) ->
                lager:warning("[zk_set_pool_data] Failed to store data for pool: ~p in ZooKeeper: ~p", [PoolName, ZKResponse]),
                {error, ZKResponse}
        end).

zk_get_pool_data(PoolName) ->
    lager:info("[zk_get_pool_data] Getting pool data for ~p", [PoolName]),
    ZKNode =  pool_name_to_zk_node(PoolName),
    zk_safely_execute(
        get,
        [ZKNode],
        fun({ok, {UpstreamsJSON, _GetData}}) ->
                lager:info("[zk_get_pool_data] Received data from ZooKeeper: ~p", [UpstreamsJSON]),
                {ok, UpstreamsJSON};
           (ZKResponse) ->
                lager:warning("[zk_get_pool_data] Failed to fetch data from ZooKeeper: ~p", [ZKResponse]),
                {error, ZKResponse}
        end).

zk_update_pool_data(PoolName, Data) ->
    lager:info("[zk_update_pool_data] Updating pool data for ~p", [PoolName]),
    case zk_pool_exists(PoolName) of
        {ok, true} ->
            lager:info("[zk_update_pool_data] ZK node for ~p exists, call zk_set_pool_data.", [PoolName]),
            zk_set_pool_data(PoolName, Data);
        {ok, false} ->
            lager:info("[zk_update_pool_data] ZK node for ~p does not exist, create one.", [PoolName]),
            zk_create_pool(PoolName, Data);
        Error -> Error
    end.

zk_setup_watch(FromPid, PoolName) ->
    lager:info("[zk_setup_watch] Setting up a watch for pool: ~p", [PoolName]),
    ZKNode = pool_name_to_zk_node(PoolName),
    zk_safely_execute(
        getw,
        [ZKNode, FromPid, _FromTag = {watch_postgresql_config_changed, PoolName}],
        fun({ok, _}) ->
               lager:info("[zk_setup_watch] Successfully set watch for: ~p", [PoolName]),
               ok;
           (ZKResponse) ->
               lager:warning("[zk_setup_watch] Failed to set watch for: ~p", [PoolName]),
               ZKResponse
        end).

%%
%% Utils
%%

schedule_refetching(Server) ->
    timer:send_after(?REFETCH_AFTER, Server, {fetch_pools_info, true}).

schedule_postgresql_pool_restart(Server, PoolName) ->
    timer:send_after(?PGSQL_POOL_RESTART_AFTER, Server, {restart_postgresql_pool, PoolName}).

restart_postgresql_pool(PoolName) ->
    lager:info("[restart_postgresql_pool] Restarting PostgreSQL pool: ~p...", [PoolName]),
    postgresql_pool_sup:stop(PoolName),
    timer:sleep(500),
    postgresql_pool_sup:start(PoolName),
    lager:info("[restart_postgresql_pool] Restarted PostgreSQL pool: ~p", [PoolName]),
    ok.

master_upstream(PoolUpstreams) ->
    [MasterUpstreamName] = [UpstreamName || {UpstreamName, UpstreamInfo} <- PoolUpstreams, proplists:get_value('write_connections', UpstreamInfo, 0) /= 0],
    MasterUpstreamName.

check_upstream_props(PoolName, UpstreamName, Props) ->
    Errors = lists:foldl(fun
            ({'disabled', V}, Acc) when is_boolean(V) -> Acc;
            ({'disabled', V}, Acc) ->
                [{wrong_disabled_contract, V} | Acc];
            ({'read_connections', ConnectionsNum}, Acc)
                when not is_integer(ConnectionsNum); ConnectionsNum < 0 ->
                [{wrong_read_connections_contract, ConnectionsNum} | Acc];
            ({'read_connections', _}, Acc) -> Acc;
            ({'write_connections', ConnectionsNum}, Acc)
                when not is_integer(ConnectionsNum); ConnectionsNum < 0 ->
                [{wrong_write_connections_contract, ConnectionsNum} | Acc];
            ({'write_connections', ConnectionsNum}, Acc) ->
                PoolUpstreams = pool_upstreams(PoolName),
                MasterUpstreamName = master_upstream(PoolUpstreams),
                case MasterUpstreamName == UpstreamName of
                    true when ConnectionsNum == 0 ->
                        [{error, cant_set_writes_of_master_to_zero} | Acc];
                    true -> Acc;
                    false when ConnectionsNum > 0 ->
                        [{error, cant_set_write_connections_for_slave} | Acc];
                    false -> Acc
                end;
            (P, Acc) ->
                [{wrong_property_contract, P} | Acc]
        end, [], Props),
    case Errors of
        [] -> ok;
        _ -> {error, Errors}
    end.
