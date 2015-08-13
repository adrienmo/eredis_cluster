-module(eredis_cluster_pools_sup).
-behaviour(supervisor).
-define(POOLS_RESTART_COUNTER,eredis_cluster_pools_counter).
-define(MAX_RETRY,20).

%% ====================================================================
%% API functions
%% ====================================================================

-export([start_link/0]).
-export([create_eredis_pool/2]).
-export([stop_eredis_pool/1]).
-export([register_worker_connection/1]).
-export([init/1]).

%% ====================================================================
%% Internal functions
%% ====================================================================

start_link() ->
	ets:new(?POOLS_RESTART_COUNTER,[set,named_table,public]),
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

create_eredis_pool(Host,Port) ->
	PoolName = list_to_atom(Host ++ "#" ++ integer_to_list(Port)),

	ets:insert(?POOLS_RESTART_COUNTER,{PoolName,0}),

    WorkerArgs = [{host, Host},{port, Port},{pool_name,PoolName}],

    PoolArgs = [{name, {local, PoolName}},
                {worker_module, eredis_cluster_worker},
                {size, 10},
                {max_overflow, 0}],

    ChildSpec = poolboy:child_spec(PoolName, PoolArgs, WorkerArgs),

    {Result,_} = supervisor:start_child(?MODULE,ChildSpec),
	{Result,PoolName}.

register_worker_connection(PoolName) ->
	RestartCounter = ets:update_counter(?POOLS_RESTART_COUNTER,PoolName,1),
	if
		RestartCounter =:= ?MAX_RETRY ->
			stop_eredis_pool(PoolName);
		true ->
			ok
	end.

stop_eredis_pool(PoolName) ->
    supervisor:terminate_child(?MODULE,PoolName),
    supervisor:delete_child(?MODULE,PoolName),
    ok.

init([]) ->
	{ok, {{one_for_one, 1, 5}, []}}.
