-module(eredis_cluster).
-behaviour(gen_server).

-define(REDIS_CLUSTER_HASH_SLOTS,16384).
-define(REDIS_CLUSTER_REQUEST_TTL,16).

-record(slots_map, {
    start_slot :: integer(),
    end_slot :: integer(),
    name :: atom(),
    index :: integer(),
    node :: node()
}).

-record(node, {
    address :: string(),
    port :: integer(),
    name :: atom(),
    connection :: pid()
}).

-record(state, {
	init_nodes :: [#node{}],
	slots :: [integer()],
	slots_maps :: [#slots_map{}],
	try_random_node :: boolean(),
	refresh_table_asap :: boolean()
}).

%% API.
-export([start/0]).
-export([start_link/0]).
-export([connect/1]).
-export([q/1]).
-export([qp/1]).
-export([transaction/2]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

%% API.

start() ->
  application:start(?MODULE).

-spec start_link() -> {ok, pid()}.
start_link() ->
	gen_server:start_link({local,?MODULE}, ?MODULE, [], []).

-spec connect([tuple()]) -> ok.
connect(InitServers) ->
	gen_server:call(?MODULE,{connect,InitServers}).

-spec q(Command::[any()]) ->
    {ok, any()} | {error, Reason::binary() | no_connection}.
q(Command) ->
	gen_server:call(?MODULE,{q,Command}).

qp(Commands) ->
    gen_server:call(?MODULE,{q, Commands}).

%% private functions
-spec q(State::#state{},Command::[any()]) ->
    {State::#state{}, Payload::any()}.
q(State,Command) ->
    q(State,Command,0).
q(State,_Command,?REDIS_CLUSTER_REQUEST_TTL) ->
    {State,{error,no_connection}};
q(State,Command,Counter) ->
	case get_key_from_command(Command) of
		undefined ->
			{State,{error, invalid_cluster_command}};
		Key ->
			Slot = get_key_slot(Key),

			Node = if
				State#state.try_random_node =:= false ->
					get_connection_by_slot(State,Slot);
				true ->
					get_random_connection(State)
			end,

			case Node of
				undefined ->
					q(State#state{try_random_node = true},Command,Counter+1);

				cluster_down ->
                    q(initialize_slots_cache(State),Command,Counter+1);

				Node ->
					case query_eredis_pool(Node#node.connection, Command) of
                        {error,no_connection} ->
							NewState = State#state{try_random_node=true},
							NewState2 = remove_connection(NewState,Node),
							q(NewState2,Command,Counter+1);

						{error,<<"MOVED ",_RedirectionInfo/binary>>} ->
                            NewState = State#state{try_random_node = false},
                            NewState2 = initialize_slots_cache(NewState),
							q(NewState2,Command,Counter+1);

                        {error,Reason} ->
                            NewState = State#state{try_random_node = false},
							{NewState,{error,Reason}};

                        Payload ->
							NewState = State#state{try_random_node = false},
							{NewState,Payload}
					end
			end
	end.

has_same_key([[_,Key|_]|T]) ->
    has_same_key(T,Key).
has_same_key([],_) ->
    true;
has_same_key([[_,Key|_]|T],Key) ->
    has_same_key(T,Key);
has_same_key(_,_) ->
    false.

transaction(Fun, Key) ->
	gen_server:call(?MODULE, {transaction, Fun, Key}).
transaction(State, Fun, Key) ->
    transaction(State, Fun, Key, 0).
transaction(State, _Fun, _Key, ?REDIS_CLUSTER_REQUEST_TTL) ->
    {State, {error, no_connection}};
transaction(State, Fun, Key, Counter) ->
    Slot = get_key_slot(Key),

    Node = if
        State#state.try_random_node =:= false ->
            get_connection_by_slot(State,Slot);
        true ->
            get_random_connection(State)
    end,

    case Node of
        undefined ->
            transaction(State#state{try_random_node = true}, Fun, Key, Counter+1);

        cluster_down ->
            transaction(initialize_slots_cache(State), Fun, Key, Counter+1);

        Node ->
            case query_eredis_pool_transaction(Node#node.connection, Fun) of
                {error,no_connection} ->
                    NewState = State#state{try_random_node=true},
                    NewState2 = remove_connection(NewState, Node),
                    transaction(NewState2, Fun, Key,Counter+1);

                {error,<<"MOVED ",_RedirectionInfo/binary>>} ->
                    NewState = State#state{try_random_node = false},
                    NewState2 = initialize_slots_cache(NewState),
                    transaction(NewState2, Fun, Key,Counter+1);

                {error,Reason} ->
                    NewState = State#state{try_random_node = false},
                    {NewState, {error, Reason}};

                Payload ->
                    NewState = State#state{try_random_node = false},
                    {NewState, {ok, Payload}}
            end
    end.

%% =============================================================================
%% @doc Given a slot return the link (Redis instance) to the mapped
%% node. Make sure to create a connection with the node if we don't
%% have one.
%% @end
%% =============================================================================

-spec get_connection_by_slot(State::any(),Slot::integer()) ->
    Connection::pid() | undefined.
get_connection_by_slot(State,Slot) ->
	Index = lists:nth(Slot+1,State#state.slots),
	Cluster = lists:nth(Index,State#state.slots_maps),
	Cluster#slots_map.node.

remove_connection(State,Node) ->
	SlotsMaps = State#state.slots_maps,
    eredis_cluster_pools_sup:stop_eredis_pool(Node#node.connection),
	NewSlotsMaps = [remove_node(SlotsMap,Node) || SlotsMap <- SlotsMaps],
	State#state{slots_maps=NewSlotsMaps}.

remove_node(SlotsMap,Node) ->
	if
		SlotsMap#slots_map.node =:= Node ->
			SlotsMap#slots_map{node=undefined};
		true ->
			SlotsMap
	end.

%% =============================================================================
%% @doc Return a link to a random node, or raise an error if no node can be
%% contacted. This function is only called when we can't reach the node
%% associated with a given hash slot, or when we don't know the right
%% mapping.
%% @end
%% =============================================================================

get_random_connection(State) ->
	SlotsMaps = State#state.slots_maps,
	NbSlotsRange = erlang:length(SlotsMaps),
	Index = random:uniform(NbSlotsRange),
    ArrangedList = lists_shift(SlotsMaps,Index),
	find_connection(ArrangedList).

find_connection([]) ->
    cluster_down;
find_connection([H|T]) ->
    if
        H#slots_map.node =/= undefined ->
            H#slots_map.node;
        true ->
            find_connection(T)
    end.

lists_shift(List,Index) ->
    lists:sublist(List,Index) ++ lists:nthtail(Index,List).


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
	eredis_cluster_crc16:crc16(KeyToBeHased) rem ?REDIS_CLUSTER_HASH_SLOTS.


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
get_key_from_command([[X|Y]|Z]) when is_list(X) ->
    HasSameKey = has_same_key([[X|Y]|Z]),
    if
        HasSameKey =:= true ->
            get_key_from_command([X|Y]);
        true ->
            undefined
    end;
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

initialize_slots_cache(State) ->
	[close_connection(SlotsMap) || SlotsMap <- State#state.slots_maps],

	ClusterInfo = get_cluster_info(State#state.init_nodes),

    SlotsMaps = parse_cluster_info(ClusterInfo),
    ConnectedSlotsMaps = connect_all_slots(SlotsMaps),
    Slots = create_slots_cache(ConnectedSlotsMaps),

	State#state{
		slots = Slots,
		slots_maps = ConnectedSlotsMaps
	}.

get_cluster_info([]) ->
	throw({error,cannot_connect_to_cluster});
get_cluster_info([Node|T]) ->
	case safe_eredis_start_link(Node#node.address, Node#node.port) of
		{ok,Connection} ->
  		case eredis:q(Connection, ["CLUSTER", "SLOTS"]) of
            {error,<<"ERR unknown command 'CLUSTER'">>} ->
                cluster_info_from_single_node(Node);
            {error,<<"ERR This instance has cluster support disabled">>} ->
                cluster_info_from_single_node(Node);
			{ok, ClusterInfo} ->
				eredis:stop(Connection),
				ClusterInfo;
			_ ->
				eredis:stop(Connection),
				get_cluster_info(T)
		end;
		_ ->
			get_cluster_info(T)
  end.

cluster_info_from_single_node(Node) ->
    [[<<"0">>,
    integer_to_binary(?REDIS_CLUSTER_HASH_SLOTS-1),
    [list_to_binary(Node#node.address),
    integer_to_binary(Node#node.port)]]].

close_connection(SlotsMap) ->
	Node = SlotsMap#slots_map.node,
	if
		Node =/= undefined ->
			try eredis_cluster_pools_sup:stop_eredis_pool(Node#node.connection) of
                _ ->
                    ok
            catch
                _ ->
                    ok
            end;
		true ->
			ok
	end.

connect_node(Node) ->
    case eredis_cluster_pools_sup:create_eredis_pool(Node#node.address, Node#node.port) of
        {ok,Connection} ->
            Node#node{connection=Connection};
        _ ->
            undefined
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

query_eredis_pool_transaction(PoolName, Fun) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        try
            {ok, <<"OK">>} = gen_server:call(Worker, {q, ["MULTI"]}),
            Fun(Worker),
            gen_server:call(Worker, {q, ["EXEC"]})
        catch Klass:Reason ->
            {ok, <<"OK">>} = gen_server:call(Worker, {q, ["DISCARD"]}),
            io:format("Error in redis transaction. ~p:~p", [Klass, Reason]),
            {Klass, Reason}
        end
    end).

safe_eredis_start_link(Address,Port) ->
    process_flag(trap_exit, true),
    Payload = eredis:start_link(Address, Port),
    process_flag(trap_exit, false),
    Payload.

create_slots_cache(SlotsMaps) ->
  SlotsCache = [[{Index,SlotsMap#slots_map.index}
		|| Index <- lists:seq(SlotsMap#slots_map.start_slot,
			SlotsMap#slots_map.end_slot)]
		|| SlotsMap <- SlotsMaps],
  SlotsCacheF = lists:flatten(SlotsCache),
  SortedSlotsCache = lists:sort(SlotsCacheF),
  [ Index || {_,Index} <- SortedSlotsCache].

connect_all_slots(ClusterSlots) ->
  [ClusterSlot#slots_map{node=connect_node(ClusterSlot#slots_map.node)}
		|| ClusterSlot <- ClusterSlots].


parse_cluster_info(ClusterInfo) ->
	Length = erlang:length(ClusterInfo),
	ClusterInfoI = lists:zip(ClusterInfo,lists:seq(1,Length)),
	ClusterSlots = [parse_cluster_slot(ClusterSlot,Index)
		|| {ClusterSlot,Index} <- ClusterInfoI],
	ClusterSlots.

parse_cluster_slot(ClusterSlot,Index) ->
	[StartSlot,EndSlot|Nodes] = ClusterSlot,
	#slots_map{
        name = get_slot_name(StartSlot,EndSlot),
        index = Index,
        start_slot = binary_to_integer(StartSlot),
        end_slot = binary_to_integer(EndSlot),
        node = parse_node(Nodes)
    }.

get_slot_name(StartSlot,EndSlot) ->
    ClusterNameStr = binary_to_list(StartSlot)
        ++ ":"
        ++ binary_to_list(EndSlot),
    list_to_atom(ClusterNameStr).

parse_node(Nodes) ->
	[Address,Port] = lists:nth(1,Nodes),
	to_node_record(Address,Port).

to_node_record(AddressB, PortB) ->
    Address = binary_to_list(AddressB),
    Port = binary_to_integer(PortB),
    PortStr  = integer_to_list(Port),
    Name = list_to_atom(Address ++ ":" ++ PortStr),

    #node{
        address = Address,
        port = Port,
        name = Name
    }.

connect_([]) ->
    #state{};
connect_(InitNodes) ->

	Nodes = [#node{address = A,port = P} || {A,P} <- InitNodes],

	State = #state{
		slots = undefined,
		slots_maps = [],
		try_random_node = false,
		init_nodes = Nodes
	},

	initialize_slots_cache(State).

%% gen_server.

init(_Args) ->
    InitNodes = application:get_env(eredis_cluster, init_nodes, []),
	{ok, connect_(InitNodes)}.

handle_call({q, Command}, _From, State) ->
    {NewState,Result} = q(State, Command),
    {reply, Result, NewState};
handle_call({transaction, Fun, Key}, _From, State) ->
    {NewState, Result} = transaction(State, Fun, Key),
    {reply, Result, NewState};
handle_call({connect, InitServers}, _From, _State) ->
	{reply, ok, connect_(InitServers)};
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
