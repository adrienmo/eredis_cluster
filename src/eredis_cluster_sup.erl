-module(eredis_cluster_sup).
-behaviour(supervisor).

%% ====================================================================
%% API functions
%% ====================================================================

-export([start_link/0]).
-export([init/1]).

%% ====================================================================
%% Internal functions
%% ====================================================================

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
	Procs = [{eredis_cluster, 
			 	{eredis_cluster, start_link, []},
			 	permanent, 5000, worker, [dynamic]}],
	{ok, {{one_for_one, 1, 5}, Procs}}.
