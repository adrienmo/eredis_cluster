-module(eredis_cluster_pools_sup).
-behaviour(supervisor).

%% ====================================================================
%% API functions
%% ====================================================================

-export([start_link/0]).
-export([create_eredis_pool/2]).
-export([stop_eredis_pool/1]).
-export([init/1]).

%% ====================================================================
%% Internal functions
%% ====================================================================

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec get_pool_name(Host::string(), Port::integer()) -> PoolName::atom().
get_pool_name(Host, Port) ->
    list_to_atom(Host ++ "#" ++ integer_to_list(Port)).

-spec create_eredis_pool(Host::string(), Port::integer()) ->
    {ok, PoolName::atom()} | {error, PoolName::atom()}.
create_eredis_pool(Host,Port) ->
	PoolName = get_pool_name(Host,Port),

    case whereis(PoolName) of
        undefined ->
            WorkerArgs = [{host, Host},{port, Port}],

        	Size = application:get_env(eredis_cluster, pool_size, 10),
        	MaxOverflow = application:get_env(eredis_cluster, pool_max_overflow, 0),

            PoolArgs = [{name, {local, PoolName}},
                        {worker_module, eredis_cluster_worker},
                        {size, Size},
                        {max_overflow, MaxOverflow}],

            ChildSpec = poolboy:child_spec(PoolName, PoolArgs, WorkerArgs),

            {Result, _} = supervisor:start_child(?MODULE,ChildSpec),
        	{Result, PoolName};
        _ ->
            {ok, PoolName}
    end.

-spec stop_eredis_pool(PoolName::atom()) -> ok.
stop_eredis_pool(PoolName) ->
    supervisor:terminate_child(?MODULE,PoolName),
    supervisor:delete_child(?MODULE,PoolName),
    ok.

init([]) ->
	{ok, {{one_for_one, 1, 5}, []}}.
