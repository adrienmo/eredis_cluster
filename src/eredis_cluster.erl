-module(eredis_cluster).

-define(REDIS_CLUSTER_REQUEST_TTL,16).
-define(REDIS_CLUSTER_HASH_SLOTS,16384).

-export([connect/1]).
-export([q/1]).
-export([qp/1]).
-export([has_same_key/1]).
-export([get_slots_map/0]).


connect(InitServers) ->
    eredis_cluster_monitor:connect(InitServers).

qp(Commands) ->
    q(Commands).
q(Command) ->
    q(Command,0,false).
q(_,?REDIS_CLUSTER_REQUEST_TTL,_) ->
    {error,no_connection};
q(Command,Counter,TryRandomNode) ->
	case get_key_from_command(Command) of
		undefined ->
			{error, invalid_cluster_command};
		Key ->
			Slot = get_key_slot(Key),

			Pool = if
				TryRandomNode =:= false ->
					eredis_cluster_monitor:get_pool_by_slot(Slot);
				true ->
					eredis_cluster_monitor:get_random_pool()
			end,

			case Pool of
				undefined ->
					q(Command,Counter+1,true);

				cluster_down ->
                    eredis_cluster_monitor:initialize_slots_cache(),
                    q(Command,Counter+1,false);

				Pool ->
					case query_eredis_pool(Pool, Command) of
                        {error,no_connection} ->
							eredis_cluster_monitor:remove_pool(Pool),
							q(Command,Counter+1,true);

						{error,<<"MOVED ",_RedirectionInfo/binary>>} ->
                            eredis_cluster_monitor:initialize_slots_cache(),
							q(Command,Counter+1,false);

                        Payload ->
                            Payload
					end
			end
	end.

query_eredis_pool(PoolName,[[X|Y]|Z]) when is_list(X) ->
    query_eredis_pool(PoolName,[[X|Y]|Z],qp);
query_eredis_pool(PoolName, Command) ->
    query_eredis_pool(PoolName,Command,q).
query_eredis_pool(PoolName, Params, Type) ->
    try
        poolboy:transaction(PoolName, fun(Worker) ->
            gen_server:call(Worker, {Type, Params})
        end)
    catch
        exit:_ ->
            {error,no_connection}
    end.

get_slots_map() ->
    eredis_cluster_monitor:get_slots_map().

%% =============================================================================
%% @doc Return the hash slot from the key
%% @end
%% =============================================================================

-spec get_key_slot(Key::string()) -> Slot::integer().
get_key_slot(Key) ->
	KeyToBeHased = case string:chr(Key,${) of
		0 ->
			Key;
		Start ->
			case string:chr(string:substr(Key,Start+1),$}) of
				0 ->
					Key;
				Length ->
					if
						Length =:= 1 ->
							Key;
						true ->
							string:substr(Key,Start+1,Length-1)
					end
			end
	end,
	eredis_cluster_hash:hash(KeyToBeHased).

%% =============================================================================
%% @doc Return the first key in the command arguments.
%%
%% Currently we just return the second argument
%% after the command name.
%%
%% This is indeed the key for most commands, and when it is not true
%% the cluster redirection will point us to the right node anyway.
%%
%% For commands that don't make sense in the context of cluster
%% return value will be undefined.
%% @end
%% =============================================================================

-spec get_key_from_command([string()]) -> string() | undefined.
get_key_from_command([[X|Y]|_]) when is_list(X) ->
    get_key_from_command([X|Y]);
get_key_from_command([Term1,Term2|_]) ->
	case string:to_lower(Term1) of
		"info" ->
			undefined;
		"multi" ->
			undefined;
		"exec" ->
			undefined;
		"slaveof" ->
			undefined;
		"config" ->
			undefined;
		"shutdown" ->
			undefined;
		_ ->
			Term2
	end;
get_key_from_command(_) ->
    undefined.

has_same_key([[_,Key|_]|T]) ->
    has_same_key(T,Key).
has_same_key([],_) ->
    true;
has_same_key([[_,Key|_]|T],Key) ->
    has_same_key(T,Key);
has_same_key(_,_) ->
    false.
