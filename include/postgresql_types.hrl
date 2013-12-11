%%% vim: ts=4 sts=4 sw=4 expandtab:

-include_lib("epgsql/include/pgsql.hrl").

% common types

-type pool_name() :: atom().
-type exec_mode() :: 'read' | 'write'.

-type upstream_name() :: atom().
-type connection_info() :: {pool_name(), upstream_name(), exec_mode()}.
-type connection() :: pid().

-type pgsql_count() :: non_neg_integer().
-type pgsql_cols() :: [tuple()].
-type pgsql_rows() :: [tuple()].
-type pgsql_answer() :: 'done'
                        | {'ok', pgsql_count()}
                        | {'ok', pgsql_count(), pgsql_cols(), pgsql_rows()}
                        | {'ok', pgsql_cols(), pgsql_rows()}
                        | {'partial', pgsql_rows()}
                        | {'ok', pgsql_count(), pgsql_rows()}
                        | {'ok', pgsql_rows()}.

-type pgsql_query() :: nonempty_string() | binary().
-type pgsql_params() :: [term()].

-type pgsql_pool_error_reason() :: {'busy' | 'timeout', 'pgsql_pool'}.
-type pgsql_error_reason() :: {'timeout' | 'closed' | term(), 'pgsql'} | #error{}.
-type pgsql_transaction_error_reason() :: {'closed', 'pgsql_transaction'}.

% upstream types

-type upstream_transaction_query_result() :: {'error', pgsql_error_reason()} | pgsql_answer().
-type upstream_query_result() :: {'error', pgsql_pool_error_reason()} | upstream_transaction_query_result().
-type upstream_transaction_result() :: {'error', pgsql_pool_error_reason() | pgsql_transaction_error_reason()} | term().
-type upstream_transaction_callback() :: fun((connection()) -> term()).

-type upstream_error_reasons() :: {'busy', 'pgsql_pool'}
                                    | {'timeout', 'pgsql_pool' | 'pgsql'}
                                    | {'closed', 'pgsql' | 'pgsql_transaction'}
                                    | {#error{} | term(), 'pgsql'}.

% pool types

-type do_mfa() :: {atom(), atom(), [term()]}.

-type pool_error_reasons() :: {'busy', 'allocate_request'}
                                | {'timeout', 'allocate_request' | 'postgresql_pool'}.

-type pool_query_result() :: {'error', pool_error_reasons()} | upstream_query_result().

-type wait_mode() :: 'wait' | 'nowait'.
-type timeout_mode() :: wait_mode() | timeout().


% postgresql types

-type rpc_error_reasons() :: {'nodedown', 'rpc'}
                                | {'timeout', 'rpc'}
                                | {term(), 'rpc'}.

-type postgresql_error_reasons() :: rpc_error_reasons() | pool_error_reasons() | upstream_error_reasons().

-type postgresql_query_result() :: {'error', postgresql_error_reasons()} | pgsql_answer().
-type postgresql_transaction_result() :: postgresql_query_result() | term().

-type known_errors_codes() :: 'duplicate_key'.
-type unknown_errors_codes() :: binary().

-type public_error_reasons() :: {'query_error', known_errors_codes() | {unknown_errors_codes(), nonempty_string()}}
                                    | {'busy' | 'timeout' | 'closed' | 'nodedown', atom()}
                                    | term().
% config types

-type credentials() :: [{'host' | 'database' | 'username' | 'password', nonempty_list()}
                            | {'port', pos_integer()}, ...].

-type upstream_info() :: [{'read_connections' | 'write_connections', non_neg_integer()}
                            | {'disabled', boolean()}
                            | {'credentials', credentials()}, ...].

-type upstream_config() :: {upstream_name(), upstream_info()}.

-type pool_type() :: 'customers_pool' | 'accounts_pool' | 'customer_pools'.
