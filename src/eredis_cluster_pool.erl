-module(eredis_cluster_pool).
-behaviour(supervisor).

%% API.
-export([create/2]).
-export([stop/1]).
-export([transaction/2]).

%% Supervisor
-export([start_link/0]).
-export([init/1]).

-include("eredis_cluster.hrl").

-spec create(Host::string(), Port::integer()) ->
    {ok, PoolName::atom()} | {error, PoolName::atom()}.
create(Host, Port) ->
	PoolName = get_name(Host, Port),

    case whereis(PoolName) of
        undefined ->
            WorkerArgs = [{host, Host}, {port, Port}],

        	Size = application:get_env(eredis_cluster, pool_size, 10),
        	MaxOverflow = application:get_env(eredis_cluster, pool_max_overflow, 0),

            PoolArgs = [{name, {local, PoolName}},
                        {worker_module, eredis_cluster_pool_worker},
                        {size, Size},
                        {max_overflow, MaxOverflow}],

            ChildSpec = poolboy:child_spec(PoolName, PoolArgs, WorkerArgs),

            {Result, _} = supervisor:start_child(?MODULE,ChildSpec),
        	{Result, PoolName};
        _ ->
            {ok, PoolName}
    end.

-spec transaction(PoolName::atom(), fun((Worker::pid()) -> redis_result())) ->
    redis_result().
transaction(PoolName, Transaction) ->
    try
        poolboy:transaction(PoolName, Transaction)
    catch
        exit:_ ->
            {error, no_connection}
    end.

-spec stop(PoolName::atom()) -> ok.
stop(PoolName) ->
    supervisor:terminate_child(?MODULE,PoolName),
    supervisor:delete_child(?MODULE,PoolName),
    ok.

-spec get_name(Host::string(), Port::integer()) -> PoolName::atom().
get_name(Host, Port) ->
    list_to_atom(Host ++ "#" ++ integer_to_list(Port)).

-spec start_link() -> {ok, pid()}.
start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init([])
	-> {ok, {{supervisor:strategy(), 1, 5}, [supervisor:child_spec()]}}.
init([]) ->
	{ok, {{one_for_one, 1, 5}, []}}.
