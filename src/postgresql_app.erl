-module(postgresql_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, start/0, perf_test/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    postgresql_sup:start_link().

start() -> a_start(postgresql, permanent).

a_start(App, Type) ->
        start_ok(App, Type, application:start(App, Type)).

start_ok(_App, _Type, ok) -> ok;
start_ok(_App, _Type, {error, {already_started, _App}}) -> ok;
start_ok(App, Type, {error, {not_started, Dep}}) ->
        ok = a_start(Dep, Type),
        a_start(App, Type);
start_ok(App, _Type, {error, Reason}) ->
        erlang:error({app_start_failed, App, Reason}).

stop(_State) ->
    ok.

perf_test(0) -> unsuccess;
perf_test(N) ->
    Q = <<"SELECT applications.*,  applications.status AS cstatus, customers.customer, namespace, partners.partner FROM applications LEFT JOIN customers ON applications.customer_id=customers.customer_id LEFT JOIN account_namespaces ON account_namespaces.namespace_id = customers.namespace_id LEFT JOIN partners ON applications.partner_id = partners.partner_id WHERE customers.status='enabled' AND applications.status='enabled'">>,
    case N rem 100 of
        0 ->
	  io:format("Sleeping 1 sec ~p~n", [N]),
          timer:sleep(1000);
        _ -> continue
    end,
          
    Ret = [X || {_R, _C, D}  <- [postgresql_pool:equery('echo-customers', write, Q, [])], X <- D, is_binary(element(2, X))],

    case Ret of
        [] -> perf_test(N - 1);
        _ -> {success, N}
    end.
