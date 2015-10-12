-module(eredis_cluster_monitor).
-behaviour(gen_server).

-define(REDIS_CLUSTER_HASH_SLOTS,16384).

-record(node, {
    address :: string(),
    port :: integer(),
    pool :: atom()
}).

-record(slots_map, {
    start_slot :: integer(),
    end_slot :: integer(),
    name :: atom(),
    index :: integer(),
    node :: #node{}
}).

-record(state, {
	init_nodes :: [#node{}],
	slots :: [integer()],
	slots_maps :: [#slots_map{}],
    version :: integer()
}).

%% API.
-export([start/0]).
-export([start_link/0]).
-export([connect/1]).
-export([refresh_mapping/1]).
-export([get_pool_by_slot/1]).

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

connect(InitServers) ->
	gen_server:call(?MODULE,{connect,InitServers}).

refresh_mapping(Version) ->
    gen_server:call(?MODULE,{reload_slots_map,Version}).

%% =============================================================================
%% @doc Given a slot return the link (Redis instance) to the mapped
%% node.
%% @end
%% =============================================================================

get_pool_by_slot(Slot) ->
    [{cluster_state, State}] = ets:lookup(?MODULE, cluster_state),
	Index = lists:nth(Slot+1,State#state.slots),
	Cluster = lists:nth(Index,State#state.slots_maps),
    if
        Cluster#slots_map.node =/= undefined ->
            {State#state.version,Cluster#slots_map.node#node.pool};
        true ->
            {State#state.version,undefined}
    end.

reload_slots_map(State) ->
	[close_connection(SlotsMap) || SlotsMap <- State#state.slots_maps],

	ClusterInfo = get_cluster_info(State#state.init_nodes),

    SlotsMaps = parse_cluster_info(ClusterInfo),
    ConnectedSlotsMaps = connect_all_slots(SlotsMaps),
    Slots = create_slots_cache(ConnectedSlotsMaps),

	NewState = State#state{
		slots = Slots,
		slots_maps = ConnectedSlotsMaps,
        version = State#state.version + 1
	},

    true = ets:insert(?MODULE, [{cluster_state, NewState}]),

    NewState.

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
			try eredis_cluster_pools_sup:stop_eredis_pool(Node#node.pool) of
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
        {ok,Pool} ->
            Node#node{pool=Pool};
        _ ->
            undefined
    end.

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
    #node{
        address = binary_to_list(Address),
        port = binary_to_integer(Port)
    }.

connect_([]) ->
    #state{};
connect_(InitNodes) ->

	Nodes = [#node{address = A,port = P} || {A,P} <- InitNodes],

	State = #state{
		slots = undefined,
		slots_maps = [],
		init_nodes = Nodes,
        version = 0
	},

	reload_slots_map(State).

%% gen_server.

init(_Args) ->
    ets:new(?MODULE, [protected, set, named_table, {read_concurrency, true}]),
    InitNodes = application:get_env(eredis_cluster, init_nodes, []),
	{ok, connect_(InitNodes)}.

handle_call({reload_slots_map,Version}, _From, #state{version=Version} = State) ->
	{reply, ok, reload_slots_map(State)};
handle_call({reload_slots_map,_}, _From, State) ->
	{reply, ok, State};
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
