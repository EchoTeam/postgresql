%%% vim: ts=4 sts=4 sw=4 expandtab:

-module(postgresql_upstream_tests).

-include_lib("eunit/include/eunit.hrl").

call_pgsql_test_() ->
    {foreach,
        fun call_pgsql_setup/0,
        fun call_pgsql_cleanup/1,
        test_call_pgsql_generator()
    }.

call_pgsql_setup() ->
    meck:new(pgsql),
    meck:expect(pgsql, squery, fun
        (_Connection, "ok") ->
            done;
        (_Connection, "timeout") ->
            throw({error, timeout});
        (_Connection, "closed") ->
            throw({error, closed});
        (_Connection, "somethrow") ->
            throw({error, somethrow});
        (_Connection, "someerror") ->
            erlang:error(someerror);
        (_Connection, "other") ->
            exit(someexit)
    end).

call_pgsql_cleanup(_) ->
    meck:unload().

test_call_pgsql_generator() ->
    [{Title, fun() ->
        ?assertEqual(Expectation, postgresql_upstream:transaction_squery(self(), Id))
    end} || {Title, {Expectation, Id}} <- [
        {"positive",
            {done, "ok"}},
        {"throw timeout",
            {{error, {timeout, pgsql}}, "timeout"}},
        {"throw closed",
            {{error, {closed, pgsql}}, "closed"}},
        {"throw other",
            {{error, {somethrow, pgsql}}, "somethrow"}},
        {"error",
            {{error, {{error, someerror}, pgsql}}, "someerror"}},
        {"other exception",
            {{error, {{exit, someexit}, pgsql}}, "other"}}
    ]].
