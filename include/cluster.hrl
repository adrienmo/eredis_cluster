-define(REDIS_CLUSTER_HASH_SLOTS,16384).
-define(REDIS_CLUSTER_REQUEST_TTL,16).
-define(REDIS_CLUSTER_DEFAULT_TTIMEOUT,1).

-record(cluster_slot, {
    start_slot :: integer(),
    end_slot :: integer(),
    name :: atom(),
    index :: integer(),
    nodes :: term()
}).

-record(node, {
    address :: string(),
    port :: integer(),
    name :: atom(),
    connection :: pid()
}).