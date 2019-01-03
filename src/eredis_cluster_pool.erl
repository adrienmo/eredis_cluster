-module(eredis_cluster_pool).
-behaviour(supervisor).

%% API.
-export([create/2]).
-export([stop/1]).
-export([transaction/2]).
-export([transaction_all/2]).

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
            WorkerArgs = [{host, Host},
                          {port, Port},
                          {database, DataBase},
                          {password, Password}
                         ],

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

%% this executes a transaction in all the pools  
%% used mainly for broadcasting readonly query to all the slaves
-spec transaction_all(PoolName::atom(), Command :: list()) ->
    ok.
transaction_all(PoolName, Command) ->
    try
        Workers = gen_server:call(PoolName, get_avail_workers),
        lists:foreach( fun(Worker) -> 
                eredis_cluster_pool_worker:query(Worker, Command)
            end,
            Workers
        ),
        ok
    catch
        exit:_ ->
        throw({error,cannot_issue_readonly_to_slaves})
    end.

-spec stop(PoolName::atom()) -> ok.
stop(PoolName) ->
    supervisor:terminate_child(?MODULE,PoolName),
    supervisor:delete_child(?MODULE,PoolName),
    ok.

-spec get_name(Host::string(), Port::integer()) -> PoolName::atom().
get_name(Host, Port) ->
    Random = rand:uniform(100000),
    list_to_atom(Host ++ "#" ++ integer_to_list(Port) ++ integer_to_list(Random)).

-spec start_link() -> {ok, pid()}.
start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init([])
	-> {ok, {{supervisor:strategy(), 1, 5}, [supervisor:child_spec()]}}.
init([]) ->
	{ok, {{one_for_one, 1, 5}, []}}.
