%%% vim: ts=4 sts=4 sw=4 expandtab:

-module(postgresql_upstream_test_integration).

-export([
    test_integration/0
]).

-define('TABLE', "test_integration").

test_integration() ->
    local = postgresql_debug:location(),
    PoolName = hd(postgresql_config:customer_pool_names()),
    PoolUpstreams = postgresql_config:pool_upstreams(PoolName),
    {UpstreamName, _Props} = postgresql_config:master_upstream(PoolUpstreams),
    ConnectionInfo = {PoolName, UpstreamName, write},

    test_integration_error_busy(ConnectionInfo),
    test_integration_error_timeout(ConnectionInfo),
    test_integration_error_closed(ConnectionInfo),
    test_integration_query_error(ConnectionInfo),
    test_integration_error_other(ConnectionInfo),
    test_integration_do_transaction(ConnectionInfo),
    test_integration_positive(ConnectionInfo),
    test_integration_transaction_fail(ConnectionInfo).

test_integration_error_busy(ConnectionInfo) ->
    Size =  proplists:get_value(size, get_status(ConnectionInfo), 0),
    true = Size > 0,
    Refs = [jsk_async:run(fun() ->
        % sometimes one or some connects may be locked
        postgresql_upstream:do(ConnectionInfo, fun(_Connection) ->
            timer:sleep(1000),
            ok
        end, 500)
    end) || _Id <- lists:seq(1, Size + 10)],
    Results = [jsk_async:join(Ref) || Ref <- Refs],
    OkResult = ok,
    BusyResult = {error, {busy, pgsql_pool}},
    try
        true = length([item || Result <- Results, Result =:= OkResult]) > 0,
        true = length([item || Result <- Results, Result =:= BusyResult]) > 0,
        [] = [Result || Result <- Results, Result =/= OkResult, Result =/= BusyResult]
    catch
        error:{badmatch, _Something} = Reason ->
            io:format("RESULTS:~n~p~n", [Results]),
            erlang:error(Reason)
    end.

test_integration_error_timeout(ConnectionInfo) ->
    {error, {timeout, pgsql_pool}} = postgresql_upstream:do(ConnectionInfo, fun(_Connection) ->
        ok
    end, 0).

test_integration_error_closed({PoolName, _UpstreamName, _Mode} = ConnectionInfo) ->
    try
        Tab = ets:new(data, [set, private]),
        try
            true = ets:insert_new(Tab, {done, false}),
            % transaction done, but we can not COMMIT
            {error, {closed, pgsql_transaction}} = postgresql_upstream:do(ConnectionInfo, fun(Connection) ->
                test_integration_error_closed_transaction(Connection),
                % set flag if transaction done
                true = ets:insert(Tab, {done, true})
            end, 1000),
            [{done, true}] = ets:lookup(Tab, done)
        after
            true = ets:delete(Tab)
        end
    after
        ok = postgresql_pool_sup:stop(PoolName),
        {ok, _} = postgresql_pool_sup:start(PoolName),
        ok = postgresql_pool:debug_wait_start(PoolName)
    end.

test_integration_error_closed_transaction(Connection) ->
    Query = fun() ->
        postgresql_upstream:transaction_squery(Connection, "SELECT 1")
    end,
    {ok, _, _} = Query(),
    meck:new(pgsql_connection, [passthrough]),
    % it is necessary for avoiding exception and to finish transaction
    meck:expect(pgsql_connection, squery, fun(_Connection, _Query) ->
        ok
    end),
    try
        Owner = self(),
        Ref = make_ref(),
        GetMessage = fun() ->
            receive
                {Ref, Message} ->
                    Message
            after 1000 ->
                {error, timeout}
            end
        end,
        Pid = spawn_link(fun() ->
            Owner ! {Ref, ready},
            % close connection
            receive
                execute_query ->
                    Owner ! {Ref, Query()}
            end
        end),
        try
            ready = GetMessage(),
            exit(Connection, normal),
            Pid ! execute_query,
            {error, {closed, pgsql}} = GetMessage()
        after
            true = unlink(Pid),
            true = exit(Pid, kill)
        end
    after
       meck:unload()
    end.

test_integration_query_error(ConnectionInfo) ->
    {error, Reason} = postgresql_upstream:squery(ConnectionInfo, "SELECT UNKNOWN"),
    <<"42703">> = postgresql_errors:query_error_code(Reason).

test_integration_error_other(ConnectionInfo) ->
    {ok, _, _} = postgresql_upstream:squery(ConnectionInfo, "SELECT 1"),
    meck:new(pgsql),
    meck:expect(pgsql, squery, fun(_Connection, _Query) ->
        exit(someexit)
    end),
    try
        {error, {{exit, someexit}, pgsql}} = postgresql_upstream:squery(ConnectionInfo, "SELECT 1")
    after
        meck:unload()
    end.

test_integration_do_transaction(ConnectionInfo) ->
    ok = postgresql_upstream:do(ConnectionInfo, fun(Connection) ->
        {ok, _, _} = postgresql_upstream:transaction_squery(Connection, "SELECT 1"),
        ok
    end),
    CreateAndSelect = fun(Field) ->
        postgresql_upstream:do(ConnectionInfo, fun(Connection) ->
            Query = fun(Q) ->
                postgresql_upstream:transaction_squery(Connection, Q)
            end,
            {ok, _, []} = Query("CREATE TEMPORARY TABLE " ++ ?TABLE ++ " (id integer)"),
            {ok, _, []} = Query("SELECT " ++ Field ++ " FROM " ++ ?TABLE),
            {ok, _, []} = Query("DROP TABLE " ++ ?TABLE),
            ok
        end)
    end,
    ok = CreateAndSelect("id"),
    {error, {badmatch, {error, Reason}}} = CreateAndSelect("user_id"),
    <<"42703">> = postgresql_errors:query_error_code(Reason),
    ok = CreateAndSelect("id"),
    ok.

test_integration_positive(ConnectionInfo) ->
    {ok, _, _} = postgresql_upstream:squery(ConnectionInfo, "SELECT 1"),
    {ok, _, _} = postgresql_upstream:squery(ConnectionInfo, <<"SELECT 1">>),
    ok = postgresql_upstream:do(ConnectionInfo, fun(Connection) ->
        Query = fun(Q, Params) ->
            postgresql_upstream:transaction_equery(Connection, Q, Params)
        end,
        {ok, [], []}     = Query("CREATE TEMPORARY TABLE " ++ ?TABLE ++ " (id integer)", []),
        {ok, 1}          = Query("INSERT INTO " ++ ?TABLE ++ " (id) VALUES ($1)", [1]),
        {ok, [_], [{1}]} = Query("SELECT id FROM " ++ ?TABLE, []),
        {ok, 1}          = Query("UPDATE " ++ ?TABLE ++ " SET id = $1", [2]),
        {ok, 1}          = Query("DELETE FROM " ++ ?TABLE ++ " WHERE id = $1", [2]),
        {ok, _, []}      = Query("DROP TABLE " ++ ?TABLE, []),
        ok
    end).

test_integration_transaction_fail(ConnectionInfo) ->
    {error, someerror} = postgresql_upstream:do(ConnectionInfo, fun(_Connection) ->
        erlang:error(someerror)
    end),
    ok = postgresql_upstream:do(ConnectionInfo, fun(Connection) ->
        Query = fun(Q, Params) ->
            postgresql_upstream:transaction_equery(Connection, Q, Params)
        end,
        InsertQuery = fun() ->
            Query("INSERT INTO " ++ ?TABLE ++ " (id) VALUES ($1)", [1])
        end,
        {ok, [], []} = Query("CREATE TEMPORARY TABLE " ++ ?TABLE ++ " (id integer)", []),
        {ok, [], []} = Query("ALTER TABLE " ++ ?TABLE ++ " ADD CONSTRAINT pkey PRIMARY KEY (id)", []),
        {ok, 1}      = InsertQuery(),
        {error, Reason1} = InsertQuery(),
        duplicate_key = postgresql_errors:query_error_code(Reason1),
        {error, Reason2}  = Query("DROP TABLE " ++ ?TABLE, []),
        <<"25P02">> =  postgresql_errors:query_error_code(Reason2),
        ok
    end).

% private functions

get_status(ConnectionInfo) ->
    gen_server:call(postgresql_upstream:name(ConnectionInfo), status).
