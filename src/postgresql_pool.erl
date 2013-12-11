%%% 
%%% Copyright (c) 2011 JackNyfe. All rights reserved.
%%% THIS SOFTWARE IS PROPRIETARY AND CONFIDENTIAL. DO NOT REDISTRIBUTE.
%%%
%%% vim: ts=4 sts=4 sw=4 expandtab:

-module(postgresql_pool).

-behavior(gen_server).

-export([
    do/3,
    equery/4,
    equery_nowait/4,

    name/1,

    squery/3,
    squery_nowait/3,

    start_link/2,
    status/1
]).

-export([
    debug_disable_upstreams/1,
    debug_enable_upstreams/1,
    debug_wait_start/1
]).

% for integration test
-export([call_upstream/6]).

-export([
    code_change/3,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

% If a request comes when all upstreams are busy,
% we are waiting for allocation of a request slot for that much time maximum.
-define(ALLOCATE_REQUEST_TIMEOUT, 60000).
-define(ALLOCATE_REQUEST_NOWAIT_TIMEOUT, 2000).
-define(CHECK_UPSTREAM_INIT, 1000).
-define(CHECK_UPSTREAM_AFTER, 15000).
-define(CHECK_UPSTREAM_CONNECT_TIMEOUT, 4900).

-include("postgresql_pool.hrl").

%%%%

-spec equery(PoolName :: pool_name(), ExecMode :: exec_mode(), Query :: pgsql_query(), Params :: pgsql_params()) -> pool_query_result().
equery(PoolName, ExecMode, Query, Params) ->
    call_upstream(PoolName, wait, ExecMode, equery, [Query, Params]).

-spec equery_nowait(PoolName :: pool_name(), ExecMode :: exec_mode(), Query :: pgsql_query(), Params :: pgsql_params()) -> pool_query_result().
equery_nowait(PoolName, ExecMode, Query, Params) ->
    call_upstream(PoolName, nowait, ExecMode, equery, [Query, Params]).

%%%%

-spec squery(PoolName :: pool_name(), ExecMode :: exec_mode(), Query :: pgsql_query()) -> pool_query_result().
squery(PoolName, ExecMode, Query) ->
    call_upstream(PoolName, wait, ExecMode, squery, [Query]).

-spec squery_nowait(PoolName :: pool_name(), ExecMode :: exec_mode(), Query :: pgsql_query()) -> pool_query_result().
squery_nowait(PoolName, ExecMode, Query) ->
    call_upstream(PoolName, nowait, ExecMode, squery, [Query]).

%%%%

-spec do(PoolName :: pool_name(), ExecMode :: exec_mode(), MFA :: do_mfa()) -> {'error', pool_error_reasons()} | upstream_transaction_result().
do(PoolName, ExecMode, {Module, Function, Args} = _MFA) ->
    Callback = fun(Connection) ->
        erlang:apply(Module, Function, [Connection | Args])
    end,
    call_upstream(PoolName, wait, ExecMode, do, [Callback]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

name(PoolName) when is_atom(PoolName) ->
    list_to_atom(atom_to_list(?MODULE) ++ ":" ++ atom_to_list(PoolName)).

start_link(PoolName, Upstreams) ->
    lager:info("[start_link] Starting PostgreSQL pool: ~p", [PoolName]),
    gen_server:start_link({local, name(PoolName)}, ?MODULE, {PoolName, Upstreams}, []).

-spec allocate_request_timeout(Timeout :: timeout_mode()) -> timeout().
allocate_request_timeout(wait) -> ?ALLOCATE_REQUEST_TIMEOUT;
allocate_request_timeout(nowait) -> ?ALLOCATE_REQUEST_NOWAIT_TIMEOUT;
allocate_request_timeout(Timeout) when is_integer(Timeout) -> Timeout.

-spec allocate_request(PoolName :: pool_name(), WaitMode :: wait_mode(), TimeoutMode :: timeout_mode(), ExecMode :: exec_mode()) ->
                        {'error', pool_error_reasons()} | {'ok', upstream_name(), reference()}.
allocate_request(PoolName, WaitMode, TimeoutMode, ExecMode) ->
    Timeout = allocate_request_timeout(TimeoutMode),
    try gen_server:call(name(PoolName), {allocate_request, ExecMode, WaitMode}, Timeout) of
        {ok, _, _} = Ok ->
            Ok;
        {error, busy} ->
            {error, {busy, allocate_request}}
    catch 
        C:R ->
            lager:warning("[allocate_request] Timeout: ~p; Got exception: ~p:~p; Call cancel wait: ~p", [Timeout, C, R, self()]),
            case {C, R} of
                {exit, {noproc, _}} ->
                    {error, {timeout, postgresql_pool}};
                _ ->
                    gen_server:cast(name(PoolName), {cancel_wait, self(), ExecMode}),
                    {error, {timeout, allocate_request}}
            end
    end.

release_request(PoolName, Ref) ->
    gen_server:cast(name(PoolName), {release_request, Ref}).

status(PoolName) ->
    gen_server:call(name(PoolName), status).

debug_disable_upstreams(PoolName) ->
    gen_server:call(name(PoolName), disable_upstream).

debug_enable_upstreams(PoolName) ->
    gen_server:call(name(PoolName), enable_upstream).

debug_wait_start(PoolName) ->
    debug_wait_start(PoolName, 5000, 100).

debug_wait_start(PoolName, Timeout, Interval) when Timeout > 0, Interval > 0 ->
    Until = make_until_timestamp(Timeout),
    NewTimeout = case whereis(name(PoolName)) of
        undefined ->
            timer:sleep(?CHECK_UPSTREAM_INIT),
            Timeout - ?CHECK_UPSTREAM_INIT;
        _ ->
            Timeout
    end,
    gen_server:call(name(PoolName), {wait_all_connect, Until, Interval}, NewTimeout + min(Interval, 1000)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({PoolName, Upstreams}) ->
    process_flag(trap_exit, true),
    PoolUpstreams = [
        #upstream{
            handle = UpstreamName,
            read_max = proplists:get_value('read_connections', UpstreamInfo),
            read_in_use = 0,
            write_max = proplists:get_value('write_connections', UpstreamInfo),
            write_in_use = 0,
            credentials = proplists:get_value('credentials', UpstreamInfo),
            connected = false,
            disabled = proplists:get_value('disabled', UpstreamInfo, false)
        } || {UpstreamName, UpstreamInfo} <- Upstreams
    ],
    erlang:send_after(?CHECK_UPSTREAM_INIT, self(), check_upstream_connect),
    State = #state{
        id            = name(PoolName),
        upstreams     = PoolUpstreams,
        monitors      = [],
        waiting_read  = queue:new(),
        waiting_write = queue:new()
    },
    {ok, State}.

handle_call(status, _From, State) ->
    {reply, State, State};

handle_call(disable_upstream, _From, State) ->
    {reply, ok, set_disabled_flag(State, true)};
handle_call(enable_upstream, _From, State) ->
    {reply, ok, set_disabled_flag(State, false)};

handle_call({allocate_request, ExecMode, WaitMode}, From, #state{waiting_read = WaitingReadQ, waiting_write = WaitingWriteQ, upstreams = Upstreams} = State) ->
    {TReadInUse, TReadMax, TWriteInUse, TWriteMax} = upstreams_connect_info(Upstreams),
    HasFreeConnections = case ExecMode of
        read -> TReadInUse < TReadMax;
        write -> TWriteInUse < TWriteMax
    end,
    case {TReadMax, TWriteMax, HasFreeConnections} of
        {0, _, false} -> {reply, {error, busy}, State};
        {_, 0, false} -> {reply, {error, busy}, State};
        {_, _, true} -> {noreply, acquire(From, ExecMode, State)};
        {_, _, false} ->
            case WaitMode of
                wait ->
                    Waiter = {From, ExecMode, now()},
                    {NWaitingReadQ, NWaitingWriteQ} = case ExecMode of
                        read -> {add_waiter(Waiter, WaitingReadQ), WaitingWriteQ};
                        write -> {WaitingReadQ, add_waiter(Waiter, WaitingWriteQ)}
                    end,
                    {noreply, State#state{waiting_read = NWaitingReadQ, waiting_write = NWaitingWriteQ}};
                nowait ->
                    {reply, {error, busy}, State}
            end
    end;

handle_call({wait_all_connect, Until, Interval}, From, State) ->
    self() ! {wait_all_connect, Until, Interval, From},
    {noreply, State};

handle_call(Request, _From, State) ->
    {stop, {unsupported_call, Request}, State}.

handle_cast({release_request, Monitor}, State) ->
    {noreply, release(Monitor, State)};

handle_cast({cancel_wait, Pid, ExecMode}, #state{waiting_read = WaitingReadQ, waiting_write = WaitingWriteQ} = State) ->
    {NWaitingReadQ, NWaitingWriteQ} = case ExecMode of
        read ->
            {filter_out_waiter_by_pid(Pid, WaitingReadQ), WaitingWriteQ};
        write ->
            {WaitingReadQ, filter_out_waiter_by_pid(Pid, WaitingWriteQ)}
    end,
    {noreply, State#state{waiting_read = NWaitingReadQ, waiting_write = NWaitingWriteQ}};

handle_cast(Request, State) ->
    {stop, {unsupported_cast, Request}, State}.

handle_info({'DOWN', Monitor, process, _Pid, _Info}, State) ->
    {noreply, release(Monitor, State)};

handle_info({upstream_connect_status, UpstreamName, CanConnect}, #state{upstreams = Upstreams} = State) ->
    UpdatedUpstreams = lists:map(
        fun(#upstream{handle = UpstreamHandle} = Upstream) ->
                case UpstreamHandle of
                    UpstreamName ->
                        Upstream#upstream{ connected = CanConnect };
                    _ -> Upstream
                end
        end,
        Upstreams),
    {noreply, State#state{ upstreams = UpdatedUpstreams }};

handle_info(check_upstream_connect, #state{id = PoolID, upstreams = Upstreams} = State) ->
    Server = self(),
    spawn_link(fun() ->
        lists:foreach(
            fun(#upstream{credentials = Credentials, handle = UpstreamName}) ->
                    [Host, Username, Password] = [proplists:get_value(N, Credentials) || N <- [host, username, password]],
                    CanConnect = can_connect_to_upstream(PoolID, UpstreamName, Host, Username, Password,
                                    [{connect_timeout, ?CHECK_UPSTREAM_CONNECT_TIMEOUT} | Credentials]),
                    Server ! {upstream_connect_status, UpstreamName, CanConnect}
            end,
            Upstreams),
        erlang:send_after(?CHECK_UPSTREAM_AFTER, Server, check_upstream_connect)
    end),
    {noreply, State};

handle_info({wait_all_connect, Until, Interval, From} = Message, #state{upstreams = Upstreams} = State) ->
    AllConnected = lists:all(fun(#upstream{connected = Connected}) ->
        Connected
    end, Upstreams),
    case AllConnected of
        true ->
            gen_server:reply(From, ok);
        false ->
            case calc_time_to_timestamp(Until) of
                0 ->
                    gen_server:reply(From, {error, timeout});
                _ ->
                    erlang:send_after(Interval, self(), Message)
            end
    end,
    {noreply, State};

% normal exit from a pgsql_connection process created during the check_upstream_connect call
handle_info({'EXIT', _Pid, normal}, State) ->
    {noreply, State};

handle_info(Info, State) ->
    {stop, {unsupported_info, Info}, State}.

terminate(_Reason, State) ->
    lager:info("[terminate] ~p terminating...", [State#state.id]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec call_upstream(PoolName :: pool_name(), WaitMode :: wait_mode(), TimeoutMode :: timeout_mode(),
                    ExecMode :: exec_mode(), Func :: 'equery' | 'squery' | 'do', Params :: [term(), ...]) -> {'error', pool_error_reasons()} | term().
call_upstream(PoolName, WaitMode, ExecMode, Func, Params) ->
    call_upstream(PoolName, WaitMode, WaitMode, ExecMode, Func, Params).

call_upstream(PoolName, WaitMode, TimeoutMode, ExecMode, Func, Params) ->
    case allocate_request(PoolName, WaitMode, TimeoutMode, ExecMode) of
        {ok, UpstreamName, Ref} ->
            try
                apply(postgresql_upstream, Func, [{PoolName, UpstreamName, ExecMode} | Params])
            after
                release_request(PoolName, Ref)
            end;
        {error, _} = Error ->
            Error
    end.

add_waiter(Waiter, Queue) ->
    queue:in(Waiter, Queue).

filter_out_waiter_by_pid(Pid, Queue) ->
    queue:filter(fun({{QPid, _Tag}, _Mode, _Now} = _Waiter) -> QPid =/= Pid end, Queue).

pending_request(Queue) ->
    case queue:out(Queue) of
        {{value, PendingRequest}, NewQueue} -> {PendingRequest, NewQueue};
        {empty, Queue} -> {undefined, Queue}
    end.

acquire({Pid, _Tag} = From, Mode, #state{upstreams = Upstreams, monitors = Monitors} = State) ->
    UpstreamName = choose_request_upstream(Mode, Upstreams),
    State2 = increase_connections(Mode, UpstreamName, State),
    Monitor = erlang:monitor(process, Pid),
    State3 = State2#state{monitors = [{Monitor, Mode, UpstreamName, now()} | Monitors]},
    gen_server:reply(From, {ok, UpstreamName, Monitor}),
    State3.

release(Ref, #state{monitors = Monitors, waiting_read = WaitingReadQ, waiting_write = WaitingWriteQ} = State) ->
    case lists:keytake(Ref, 1, Monitors) of
        false ->
            State;
        {value, {Monitor, Mode, UpstreamName, _Since}, Monitors2} ->
            erlang:demonitor(Monitor),
            State2 = State#state{monitors = Monitors2},
            State3 = decrease_connections(Mode, UpstreamName, State2),
            {Request, NWaitingReadQ, NWaitingWriteQ} = case Mode of
                read ->
                    {PendingRequest, ReadQ} = pending_request(WaitingReadQ),
                    {PendingRequest, ReadQ, WaitingWriteQ};
                write ->
                    {PendingRequest, WriteQ} = pending_request(WaitingWriteQ),
                    {PendingRequest, WaitingReadQ, WriteQ}
            end,
            State4 = case Request of
                {WFrom, WMode, _WSince} ->
                    acquire(WFrom, WMode, State3#state{waiting_read = NWaitingReadQ, waiting_write = NWaitingWriteQ});
                undefined ->
                    State3
            end,
            State4
    end.

choose_request_upstream(Mode, Upstreams) ->
    UList = upstream_candidates(Mode, Upstreams),
    % sort the upstreams by the number of read/write connections in use in ascending order...
    ConnSortedUpstreams = lists:keysort(2, UList),
    % group upstreams with the same number of connections...
    [LeastConnUpstreams | _] = list_utils:group_by_key(2, ConnSortedUpstreams),
    % and choose a random one from the list of upstreams with
    % the least number of connection in use to even the load
    Upstream = lists:nth(random:uniform(length(LeastConnUpstreams)), LeastConnUpstreams),
    {UpstreamName, _ConnectionsInUse} = Upstream,
    UpstreamName.

upstream_candidates(read, Upstreams) ->
    [ {Upstream, ReadInUse} ||
        #upstream{
            handle = Upstream,
            read_in_use = ReadInUse,
            read_max = ReadMax,
            connected = IsConnected,
            disabled = IsDisabled
        } <- Upstreams,
        IsConnected == true, IsDisabled == false,
        ReadInUse < ReadMax];

upstream_candidates(write, Upstreams) ->
    [ {Upstream, WriteInUse} ||
        #upstream{
            handle = Upstream,
            write_in_use = WriteInUse,
            write_max = WriteMax,
            connected = IsConnected,
            disabled = IsDisabled
        } <- Upstreams,
        IsConnected == true, IsDisabled == false,
        WriteInUse < WriteMax].

increase_connections(Mode, UpstreamName, State) ->
    update_connections(fun(Connections) -> Connections + 1 end, Mode, UpstreamName, State).

decrease_connections(Mode, UpstreamName, State) ->
    update_connections(fun(Connections) -> Connections - 1 end, Mode, UpstreamName, State).

update_connections(UpdaterFun, Mode, UpstreamName, #state{upstreams = Upstreams} = State) ->
    NewUpstreams = case lists:keytake(UpstreamName, 2, Upstreams) of
        {value, #upstream{read_in_use = ReadInUse, write_in_use = WriteInUse} = Upstream, _} ->
            NewUpstreamInfo = case Mode of
                read -> 
                    Upstream#upstream{read_in_use = UpdaterFun(ReadInUse)};
                write ->
                    Upstream#upstream{write_in_use = UpdaterFun(WriteInUse)}
            end,
            lists:keystore(UpstreamName, 2, Upstreams, NewUpstreamInfo)
    end,
    State#state{upstreams = NewUpstreams}.

upstreams_connect_info(Upstreams) ->
    UList = [ {ReadInUse, ReadMax, WriteInUse, WriteMax} ||
                #upstream{
                    read_max = ReadMax,
                    read_in_use = ReadInUse,
                    write_max = WriteMax,
                    write_in_use = WriteInUse,
                    connected = IsConnected,
                    disabled = IsDisabled
                } <- Upstreams, IsConnected == true, IsDisabled == false],
    lists:foldl(fun({ReadInUse, ReadMax, WriteInUse, WriteMax}, {TReadInUse, TReadMax, TWriteInUse, TWriteMax}) ->
                {TReadInUse + ReadInUse, TReadMax + ReadMax, TWriteInUse + WriteInUse, TWriteMax + WriteMax}
        end, {0, 0, 0, 0}, UList).

can_connect_to_upstream(PoolID, UpstreamName, Host, Username, Password, Options) ->
    can_connect_to_upstream(PoolID, UpstreamName, Host, Username, Password, Options, 0).

can_connect_to_upstream(_PoolID, _UpstreamName, _Host, _Username, _Password, _Options, 3) ->
    stats:notify("postgresql_pool.check_connect_error.attempt3.e", 1, meter),
    false;
can_connect_to_upstream(PoolID, UpstreamName, Host, Username, Password, Options, Attempt) ->
    sleep_before_check(Attempt),
    try pgsql:connect(Host, Username, Password, Options) of
        {ok, Connection} ->
            {ok, _, [{<<"pong">>}]} = pgsql:squery(Connection, "SELECT 'pong' as ping"),
            pgsql:close(Connection),
            true;
        {error, _} = E ->
            stats:notify("postgresql_pool.check_connect_error.attempt" ++ integer_to_list(Attempt) ++ ".e", 1, meter),
            lager:debug([{tag, pgsql_check_connect}], "[~p] ~p:~p:~p; Error: ~p", [node(), PoolID, UpstreamName, Attempt, E]),
            can_connect_to_upstream(PoolID, UpstreamName, Host, Username, Password, Options, Attempt + 1)
        catch C:R ->
            stats:notify("postgresql_pool.check_connect_error.attempt" ++ integer_to_list(Attempt) ++ ".e", 1, meter),
            lager:debug([{tag, pgsql_check_connect}], "[~p] ~p:~p:~p; Exception: ~p:~p", [node(), PoolID, UpstreamName, Attempt, C, R]),
            can_connect_to_upstream(PoolID, UpstreamName, Host, Username, Password, Options, Attempt + 1)
    end.

sleep_before_check(0) ->
    ok;
sleep_before_check(Attempt) ->
    Timeout = trunc(math:pow(2, Attempt) * 1000),
    timer:sleep(Timeout).

set_disabled_flag(#state{upstreams = UpstreamsOld} = State, Value) ->
    Upstreams = [ U#upstream{disabled = Value} || U <- UpstreamsOld ],
    State#state{upstreams = Upstreams}.

make_until_timestamp(Timeout) ->
    time_utils:micro2now(time_utils:now2micro(now()) + (Timeout * 1000)).

calc_time_to_timestamp(UntilTimestamp) ->
    case timer:now_diff(UntilTimestamp, now()) of
        UntilMicro when UntilMicro > 0 ->
            UntilMicro div 1000;
        _ ->
            0
    end.

-ifdef(TEST).

upstream_candidates_test() ->
    Upstreams = [
        #upstream{handle = master, read_max = 0, read_in_use = 0, write_max = 5, write_in_use = 0, connected = true, disabled = false},
        #upstream{handle = slave1, read_max = 5, read_in_use = 1, write_max = 0, write_in_use = 0, connected = true, disabled = false},
        #upstream{handle = slave2, read_max = 5, read_in_use = 2, write_max = 0, write_in_use = 0, connected = true, disabled = false},
        #upstream{handle = slave3, read_max = 5, read_in_use = 0, write_max = 0, write_in_use = 0, connected = false, disabled = false}, % disconnected upstream
        #upstream{handle = slave4, read_max = 5, read_in_use = 0, write_max = 0, write_in_use = 0, connected = true, disabled = true},   % disabled upstream
        #upstream{handle = slave5, read_max = 5, read_in_use = 5, write_max = 0, write_in_use = 0, connected = true, disabled = false}   % busy upstream
    ],
    ?assertEqual([{slave1, 1}, {slave2, 2}], upstream_candidates(read, Upstreams)),
    ?assertEqual([{master, 0}], upstream_candidates(write, Upstreams)).

choose_request_upstream_test() ->
    Upstreams = [
        #upstream{handle = master, read_max = 0, read_in_use = 0, write_max = 5, write_in_use = 0, connected = true, disabled = false},
        #upstream{handle = slave1, read_max = 5, read_in_use = 2, write_max = 0, write_in_use = 0, connected = true, disabled = false},
        #upstream{handle = slave2, read_max = 5, read_in_use = 1, write_max = 0, write_in_use = 0, connected = true, disabled = false},
        #upstream{handle = slave3, read_max = 5, read_in_use = 3, write_max = 0, write_in_use = 0, connected = true, disabled = false}
    ],
    ?assertEqual(slave2, choose_request_upstream(read, Upstreams)),
    ?assertEqual(master, choose_request_upstream(write, Upstreams)).

% upstreams with the same number of connections in use have the same probability of being selected
choose_request_upstream_equal_probability_test() ->
    Upstreams = [
        #upstream{handle = slave1, read_max = 5, read_in_use = 1, write_max = 0, write_in_use = 0, connected = true, disabled = false},
        #upstream{handle = slave2, read_max = 5, read_in_use = 1, write_max = 0, write_in_use = 0, connected = true, disabled = false},
        #upstream{handle = slave3, read_max = 5, read_in_use = 2, write_max = 0, write_in_use = 0, connected = true, disabled = false}
    ],
    UpstreamChoices =  [choose_request_upstream(read, Upstreams) || _N <- lists:seq(1, 10)],
    UniqueUpstreamChoices = list_utils:unique_list(UpstreamChoices),
    ?assertEqual([slave1, slave2], UniqueUpstreamChoices).

-endif.
