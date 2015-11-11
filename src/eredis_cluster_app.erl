-module(eredis_cluster_app).
-behaviour(application).

%% ====================================================================
%% API functions
%% ====================================================================

-export([start/2]).
-export([stop/1]).

%% ====================================================================
%% Internal functions
%% ====================================================================

start(_Type, _Args) ->
    %% Start Supervisor
    eredis_cluster_sup:start_link().

stop(_State) ->
    ok.
