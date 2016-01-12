-module(eredis_cluster_sup).
-behaviour(supervisor).

%% Supervisor.
-export([start_link/0]).
-export([init/1]).

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init([])
	-> {ok, {{supervisor:strategy(), 1, 5}, [supervisor:child_spec()]}}.
init([]) ->
    Procs = [{eredis_cluster_pool,
                {eredis_cluster_pool, start_link, []},
                permanent, 5000, supervisor, [dynamic]},
            {eredis_cluster_monitor,
                {eredis_cluster_monitor, start_link, []},
                permanent, 5000, worker, [dynamic]}
            ],
    {ok, {{one_for_one, 1, 5}, Procs}}.
