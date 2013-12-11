%%% vim: ts=4 sts=4 sw=4 expandtab:
-include("postgresql_types.hrl").

-record(upstream, {
    handle :: upstream_name(),
    read_max = 0 :: non_neg_integer(),
    read_in_use = 0 :: non_neg_integer(),
    write_max = 0 :: non_neg_integer(),
    write_in_use = 0 :: non_neg_integer(),
    credentials = [] :: [proplists:property()],
    connected = false :: boolean(),
    disabled = false :: boolean()
}).

-record(state, {
    id :: pool_name(),
    upstreams = [] :: [#upstream{}],
    monitors = [] :: [term()],
    waiting_read :: queue(),
    waiting_write :: queue()
}).
