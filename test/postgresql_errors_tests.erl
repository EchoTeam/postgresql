%%% vim: ts=4 sts=4 sw=4 expandtab:

-module(postgresql_errors_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("epgsql/include/pgsql.hrl").

parse_error_reason_test() ->
    [begin
        ?assertEqual(Expectation, postgresql_errors:parse_error_reason(Reason))
    end || {Expectation, Reason} <- [
        {{known_code, duplicate_key}, make_pgsql_error_from_code(<<"23505">>)},
        {{unknown_code, {<<"23506">>, "message"}}, make_pgsql_error_from_code(<<"23506">>)},
        {{expected, {busy, service}}, {busy, service}},
        {{expected, {timeout, service}}, {timeout, service}},
        {{expected, {closed, service}}, {closed, service}},
        {{expected, {nodedown, service}}, {nodedown, service}},
        {{custom, someerror}, {custom, someerror}},
        {{unexpected, {undefined, service}}, {undefined, service}},
        {{unexpected, {busy, "s"}}, {busy, "s"}},
        {{unexpected, {custom, {a, b}}}, {custom, {a, b}}},
        {{unexpected, e}, e}
    ]].

internal_error_reason_to_string_test() ->
    [begin
        ?assertEqual(Expectation, postgresql_errors:internal_error_reason_to_string(Reason))
    end || {Expectation, Reason} <- [
        {"error_duplicate_key", make_pgsql_error_from_code(<<"23505">>)},
        {"error_23506", make_pgsql_error_from_code(<<"23506">>)},
        {"busy_service", {busy, service}},
        {"custom_someerror", {custom, someerror}},
        {"undefined", e}
    ]].

query_error_code_test() ->
    [begin
        ?assertEqual(Expectation, postgresql_errors:query_error_code(Reason))
    end || {Expectation, Reason} <- [
        {duplicate_key, make_pgsql_error_from_code(<<"23505">>)},
        {<<"23506">>, make_pgsql_error_from_code(<<"23506">>)},
        {undefined, {busy, service}},
        {undefined, {undefined, service}},
        {undefined, timeout}
    ]].

public_error_reason_test() ->
    [begin
        ?assertEqual(Expectation, postgresql_errors:public_error_reason(Reason))
    end || {Expectation, Reason} <- [
        {duplicate_key, make_pgsql_error_from_code(<<"23505">>)},
        {{<<"23506">>, "message"}, make_pgsql_error_from_code(<<"23506">>)},
        {{busy, service}, {busy, service}},
        {{timeout, service}, {timeout, service}},
        {{closed, service}, {closed, service}},
        {{nodedown, service}, {nodedown, service}},
        {{custom, e}, {custom, e}},
        {undefined, undefined}
    ]].

% private functions

make_pgsql_error_from_code(Code) ->
    {#error{code = Code, message = "message"}, pgsql}.
