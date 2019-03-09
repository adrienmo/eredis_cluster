-module(eredis_cluster_pool).
-behaviour(supervisor).

%% API.
-export([create/2]).
-export([stop/1]).
-export([transaction/2]).
-export([get_worker/1]).

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
            DataBase = application:get_env(eredis_cluster, database, 0),
            Password = application:get_env(eredis_cluster, password, ""),

            WorkerArgs = [Host, Port, DataBase, Password, 100, 5000],

        	Size = application:get_env(eredis_cluster, pool_size, 10),

            Worker = {eredis_client, WorkerArgs},

            %% Parametros del workers_pool
            PoolArgs = [PoolName,[{workers,Size},{worker,Worker}]],

            %% Creo un hijo de tipo wpool

            ChildSpec = #{ id => PoolName,
                           start => {wpool, start_pool, PoolArgs},
                           restart => temporary,
                           type => supervisor,
                           modules => [wpool]},

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

get_worker(PoolName) ->
    wpool_pool:next_worker(PoolName).

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
    wpool:start(),
	{ok, {{one_for_one, 1, 5}, []}}.
