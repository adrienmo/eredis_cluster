-module(eredis_cluster_utils).

-export([lists_shift/2]).

lists_shift(List,Index) ->
    lists:sublist(List,Index) ++ lists:nthtail(Index,List).
