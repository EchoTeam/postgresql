%%% vim: ts=4 sts=4 sw=4 expandtab:

-module(postgresql_test_integration).
-export([
    test_integration/0
]).

-export([
    test_integration_do_callback_equery/1,
    test_integration_do_callback_squery/1
]).

test_integration() ->
    PoolName = hd(postgresql_config:customer_pool_names()),
    
    test_integration_positive(PoolName).

test_integration_positive(PoolName) ->
    [begin
        Query = fun(QueryString, Params) ->
            ParametrizedQuery = parametrize_query(QueryString, Params),
            Test(QueryString, ParametrizedQuery, Params)
        end,

        ok = test_integration_simple(Query)
    end || Test <- [
        fun(Query, _ParametrizedQuery, Params) ->
            postgresql:equery(PoolName, Query, Params)
        end,
        fun(Query, _ParametrizedQuery, Params) ->
            postgresql:equery(PoolName, write, Query, Params)
        end,
        fun(Query, _ParametrizedQuery, Params) ->
            postgresql:equery_nowait(PoolName, Query, Params)
        end,
        fun(Query, _ParametrizedQuery, Params) ->
            postgresql:equery_nowait(PoolName, write, Query, Params)
        end,
        fun(_Query, ParametrizedQuery, _Params) ->
            postgresql:squery(PoolName, ParametrizedQuery)
        end,
        fun(_Query, ParametrizedQuery, _Params) ->
            postgresql:squery(PoolName, write, ParametrizedQuery)
        end,
        fun(_Query, ParametrizedQuery, _Params) ->
            postgresql:squery_nowait(PoolName, ParametrizedQuery)
        end,
        fun(_Query, ParametrizedQuery, _Params) ->
            postgresql:squery_nowait(PoolName, write, ParametrizedQuery)
        end
    ]],

    ok = postgresql:do(PoolName, {?MODULE, test_integration_do_callback_equery, []}),
    ok = postgresql:do(PoolName, {?MODULE, test_integration_do_callback_squery, []}).

test_integration_do_callback_equery(Connection) ->
    Query = fun(QueryString, Params) ->
        postgresql:transaction_equery(Connection, QueryString, Params)
    end,
    test_integration_simple(Query).

test_integration_do_callback_squery(Connection) ->
    Query = fun(QueryString, Params) ->
        ParametrizedQuery = parametrize_query(QueryString, Params),
        postgresql:transaction_squery(Connection, ParametrizedQuery)
    end,
    test_integration_simple(Query).

% private function

test_integration_simple(Query) ->
    {ok, _, _} = Query("SELECT 1", []),
    ok.

parametrize_query(Query, Params) ->
    {ParametrizedQuery, _} = lists:foldl(fun(Param, {ResultQuery, Index}) ->
        {re:replace(ResultQuery, "\\$" ++ integer_to_list(Index), type_utils:to_list(Param), [{return, list}]), Index + 1}
    end, {Query, 1}, Params),
    ParametrizedQuery.
