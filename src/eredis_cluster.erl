-module(eredis_cluster).
-behaviour(gen_server).

-include("cluster.hrl").

%% API.
-export([start/0]).
-export([start_link/0]).
-export([connect/2]).
-export([q/1]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-record(state, {
	slots_cache :: term(),
	slots_connections :: term()
}).

%% API.

start() ->
    application:start(?MODULE).

-spec start_link() -> {ok, pid()}.
start_link() ->
	gen_server:start_link({local,?MODULE}, ?MODULE, [], []).

-spec connect(string(),integer()) -> ok.
connect(Address,Port) ->
	gen_server:call(?MODULE,{connect,{Address,Port}}).

-spec q(Command::[any()]) ->
               {ok, any()} | {error, Reason::binary() | no_connection}.
q(Command) ->
	gen_server:call(?MODULE,{q,Command}).


%% private functions
-spec q(State::term(),Command::[any()]) ->
               {ok, any()} | {error, Reason::binary() | no_connection}.
q(State,Command) ->
	case get_key_from_command(Command) of
		undefined ->
			{error, <<"Command is not valid in a cluster context">>};
		Key ->
			Slot = get_key_slot(Key),
			Connection = get_connection_by_slot(Slot,State),
			eredis:q(Connection, Command)
	end.

%% ====================================================================
%% @doc Given a slot return the link (Redis instance) to the mapped
%% node. Make sure to create a connection with the node if we don't
%% have one.
%% @end
%% ====================================================================

-spec get_connection_by_slot(Slot::integer(),State::any()) -> Connection::pid().
get_connection_by_slot(Slot,State) ->
	Index = lists:nth(Slot+1,State#state.slots_cache),
	Cluster = lists:nth(Index,State#state.slots_connections),
	Node = lists:nth(1,Cluster#cluster_slot.nodes),
	Node#node.connection.


create_slots_cache(ClusterSlots) ->
    SlotsCache = [[{Index,ClusterSlot#cluster_slot.index} || Index <- lists:seq(ClusterSlot#cluster_slot.start_slot,
                ClusterSlot#cluster_slot.end_slot)] || ClusterSlot <- ClusterSlots],
    SlotsCacheF = lists:flatten(SlotsCache),
    SortedSlotsCache = lists:sort(SlotsCacheF),
    [ Index || {_,Index} <- SortedSlotsCache].

connect_all_slots(ClusterSlots) ->
    [ClusterSlot#cluster_slot{nodes=connect_nodes(ClusterSlot#cluster_slot.nodes)} ||
        ClusterSlot <- ClusterSlots].

connect_nodes(Nodes) ->
    [connect_node(Node) || Node <- Nodes].

connect_node(Node) ->
    case eredis:start_link(Node#node.address, Node#node.port) of
        {ok,Connection} ->
            Node#node{connection=Connection};
        _ ->
            Node#node{connection=undefined}
    end.

parse_cluster_slots(ClusterSlotsRaw) ->
    Length = erlang:length(ClusterSlotsRaw),
    ClusterSlotsRawI = lists:zip(ClusterSlotsRaw,lists:seq(1,Length)),
	ClusterSlots = [parse_cluster_slot(ClusterSlot,Index) || {ClusterSlot,Index} <- ClusterSlotsRawI],
    ClusterSlots.

parse_cluster_slot(ClusterSlot,Index) ->
	[StartSlot,EndSlot|Servers] = ClusterSlot,
	#cluster_slot{
        name = get_slot_name(StartSlot,EndSlot),
        index = Index,
		start_slot = binary_to_integer(StartSlot),
    	end_slot = binary_to_integer(EndSlot),
    	nodes = parse_nodes(Servers)
    }.

get_slot_name(StartSlot,EndSlot) ->
    ClusterNameStr = binary_to_list(StartSlot) ++
                      ":" ++
                      binary_to_list(EndSlot),
    list_to_atom(ClusterNameStr).

parse_nodes(Nodes) ->
	[to_node_record(Address,Port) || [Address,Port] <- Nodes].

to_node_record(AddressB, PortB) ->
    Address = binary_to_list(AddressB),
    Port = binary_to_integer(PortB),
    PortStr  = integer_to_list(Port),
    Name = list_to_atom(Address ++ ":" ++ PortStr),

    #node{address = Address,
           port = Port,
           name = Name}.

%% ====================================================================
%% @doc Return the hash slot from the key
%% @end
%% ====================================================================

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
	crc16:crc16(KeyToBeHased) rem ?REDIS_CLUSTER_HASH_SLOTS.


%% ====================================================================
%% @doc Return the first key in the command arguments.
%%
%% Currently we just return the second argument
%% after the command name.
%%
%% This is indeed the key for most commands, and when it is not true
%% the cluster redirection will point us to the right node anyway.
%%
%% For commands that don't make sense in the context of cluster
%% undefined is returned.
%% @end
%% ====================================================================

-spec get_key_from_command([string()]) -> string() | undefined.
get_key_from_command([]) ->
	undefined;
get_key_from_command(Command) ->
	case string:to_lower(lists:nth(1,Command)) of
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
			lists:nth(2,Command)
	end.

%% gen_server.

init(_Args) ->
	{ok, #state{}}.

connect_(Address,Port) ->
	{ok, C} = eredis:start_link(Address, Port),
    {ok, ClusterSlotsRaw} = eredis:q(C, ["CLUSTER", "SLOTS"]),
    eredis:stop(C),

    ClusterSlots = parse_cluster_slots(ClusterSlotsRaw),
    SlotsCache = create_slots_cache(ClusterSlots),

    SlotsConnection = connect_all_slots(ClusterSlots),

    #state{
		slots_cache = SlotsCache,
		slots_connections = SlotsConnection
	}.

handle_call({q, Command}, _From, State) ->
	{reply, q(State, Command), State};
handle_call({connect, {Address,Port}}, _From, _State) ->
	{reply, ok, connect_(Address,Port)};
handle_call(_Request, _From, State) ->
	{reply, ignored, State}.

handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.
