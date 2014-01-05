%%% vim: ts=4 sts=4 sw=4 expandtab:

-module(postgresql_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("epgsql/include/pgsql.hrl").

make_array_error_test() ->
    ?assertError(function_clause, postgresql:make_array(ok)).

make_array_test_() ->
    [{Title, fun() ->
        ?assertEqual(Expectation, postgresql:make_array(Value))
    end} || {Title, {Expectation, Value}} <- [
        {"empty list", 
            {"{}", []}},
        {"string",
            {"{\"v\"}", ["v"]}},
        {"atom",
            {"{\"v\"}", [v]}},
        {"string with quotes",
            {"{\"v\\'\"}", ["v'"]}},
        {"multiple items",
            {"{\"v1\",\"v2\"}", [v1, v2]}}
    ]].

of_array_error_test() ->
    ?assertError(function_clause, postgresql:of_array(ok)),
    ?assertError(function_clause, postgresql:of_array("ok")).

of_array_test_() ->
    [{Title, fun() ->
        ?assertEqual(Expectation, postgresql:of_array(Value))
    end} || {Title, {Expectation, Value}} <- [
        {"empty list",
            {[], "{}"}},
        {"not a string",
            {[], <<"{}">>}},
        {"string",
            {[<<"\"v\"">>], "{\"v\"}"}},
        {"string with quotes",
            {[<<"\"v'\"">>], "{\"v\\'\"}"}},
        {"multiple items",
            {[<<"\"v1\"">>,<<"\"v2\"">>], "{\"v1\",\"v2\"}"}}
    ]].

rpc_indirect_test_() ->
    {setup,
        fun rpc_indirect_setup/0,
        fun(_) -> meck:unload() end,
        test_rpc_indirect_generator()
    }.

rpc_indirect_setup() ->
    meck:new(node_pool),
    meck:expect(node_pool, erpc, fun(_ServerRef, _Key, _M, _F, A, _Timeout) ->
        case A of
            [nodedown] ->
                {node(), {badrpc, nodedown}};
            [timeout] ->
                {node(), {badrpc, timeout}};
            [unknown_error] ->
                {node(), {badrpc, unknown_error}};
            [error] ->
                erlang:error(someerror);
            [throw] ->
                throw(somethrow);
            [ok] ->
                {node(), ok}
        end
    end).

test_rpc_indirect_generator() ->
    [{atom_to_list(Arg), fun() ->
        ?assertEqual(Expectation, postgresql:rpc_indirect(undefined, undefined, [Arg]))
    end} || {Expectation, Arg} <- [
        {{error, {nodedown, rpc}},
            nodedown},
        {{error, {timeout, rpc}},
            timeout},
        {{error, {unknown_error, rpc}},
            unknown_error},
        {ok,
            ok},
        {{error, {{error, {case_clause, [undefined]}}, rpc}},
            undefined},
        {{error, {{error, someerror}, rpc}},
            error},
        {{error, {{throw, somethrow}, rpc}},
            throw},
        {ok,
            ok}
    ]].
