%%% vim: ts=4 sts=4 sw=4 expandtab:

-module(postgresql_pool_test_integration).

-export([
    test_integration/0
]).

-include("postgresql_pool.hrl").

test_integration() ->
    local = postgresql_debug:location(),
    PoolName = hd(postgresql_config:customer_pool_names()),

    test_integration_restart(PoolName),
    test_integration_without_postgresql(PoolName),
    test_integration_allocate_request(PoolName),
    test_integration_terminate_client(PoolName).

test_integration_allocate_request(PoolName) ->
    Status = postgresql_pool:status(PoolName),
    WriteMax = lists:foldl(fun(Upstream, Result) ->
        Result + Upstream#upstream.write_max
    end, 0, Status#state.upstreams),
    true = WriteMax > 0,
    Do = fun(WaitMode, TimeoutMode, Func, Params) ->
        postgresql_pool:call_upstream(PoolName, WaitMode, TimeoutMode, write, Func, Params)
    end,
    Refs = [jsk_async:run(fun() ->
        % sometimes one or some connects may be locked
        Do(nowait, nowait, do, [fun(_Connection) ->
            timer:sleep(1000),
            ok
        end])
    end) || _Id <- lists:seq(1, WriteMax + 10)],
    try
        % wait for async requests initialized
        timer:sleep(100),
        {error, {busy, allocate_request}} = Do(nowait, 1000, squery, ["SELECT 1"]),
        {error, {timeout, allocate_request}} = Do(wait, 500, squery, ["SELECT 1"])
    after
        [jsk_async:join(Ref) || Ref <- Refs]
    end.

test_integration_terminate_client(PoolName) ->
    Request = fun() ->
        postgresql_pool:call_upstream(PoolName, nowait, nowait, write, squery, ["SELECT 1"])
    end,

    {ok, _, [{<<"1">>}]} = Request(),

    meck:new(postgresql_upstream),
    meck:expect(postgresql_upstream, squery, fun(_ConnectionInfo, _Query) ->
        erlang:error(someerror)
    end),
    try
        Pid = spawn(fun() ->
            receive
                do_request ->
                    % here is an exception
                    Request()
            end
        end),
        try
            Monitor = erlang:monitor(process, Pid),
            Pid ! do_request,
            ok = receive
                {'DOWN', Monitor, process, Pid, _Info} ->
                    ok
            after 1000 ->
                timeout
            end,
            true = erlang:demonitor(Monitor)
        after
            exit(Pid, kill)
        end
    after
        meck:unload()
    end,
    % pool is alive
    {ok, _, [{<<"1">>}]} = Request().

test_integration_restart(PoolName) ->
    ok = postgresql_pool_sup:stop(PoolName),
    {ok, _} = postgresql_pool_sup:start(PoolName),
    ok = postgresql_pool:debug_wait_start(PoolName),
    ok.

test_integration_without_postgresql(PoolName) ->
    ok = postgresql_debug:stop_application(),
    try
        {error, {timeout, postgresql_pool}} = postgresql_pool:squery(PoolName, write, "SELECT 1")
    after
        ok = postgresql_debug:start_application()
    end.
