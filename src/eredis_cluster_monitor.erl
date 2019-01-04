-module(eredis_cluster_monitor).
-behaviour(gen_server).

%% API.
-export([start_link/0]).
-export([connect/1]).
-export([connect/2]).
-export([refresh_mapping/1]).
-export([get_state/0, get_state_version/1]).
-export([get_pool_by_slot/1, get_pool_by_slot/2, get_pool_by_slot/3]).
-export([get_write_pool_by_slot/2]).
-export([get_all_master_pools/0]).
-export([get_all_pools/0]).


%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).



%% Type definition.
-include("eredis_cluster.hrl").

-record(state, {
    init_nodes :: [#node{}],
    slots :: tuple(), %% whose elements are integer indexes into slots_maps
    slots_maps :: list(), %% whose elements are #slots_map{}
    version :: integer(),
    replica_read_flag :: boolean()
}).

%% API.
-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local,?MODULE}, ?MODULE, [], []).

connect(InitServers) ->
    connect(InitServers, false).

connect(InitServers, ReplicaReadsFlag) ->
    gen_server:call(?MODULE,{connect,InitServers, ReplicaReadsFlag}).

refresh_mapping(Version) ->
    gen_server:call(?MODULE,{reload_slots_map,Version}).

%% =============================================================================
%% @doc Given a slot return the link (Redis instance) to the mapped
%% node.
%% @end
%% =============================================================================

-spec get_state() -> #state{}.
get_state() ->
    [{cluster_state, State}] = ets:lookup(?MODULE, cluster_state),
    State.

get_state_version(State) ->
    State#state.version.

-spec get_all_pools() -> [pid()].
get_all_pools() ->
    State = get_state(),
    SlotsMapList = tuple_to_list(State#state.slots_maps),
    [SlotsMap#slots_map.node#node.pool || SlotsMap <- SlotsMapList,
        SlotsMap#slots_map.node =/= undefined].

-spec get_all_master_pools() -> [pid()].
get_all_master_pools() ->
    State = get_state(),
    SlotsMapList = tuple_to_list(State#state.slots_maps),
    [SlotsMap#slots_map.node#node.pool || SlotsMap <- SlotsMapList,
        SlotsMap#slots_map.node =/= undefined, SlotsMap#slots_map.type == master].

%% =============================================================================
%% @doc Get cluster pool by slot. Optionally, a memoized State can be provided
%% to prevent from querying ets inside loops.
%% @end
%% =============================================================================
-spec get_pool_by_slot(Slot::integer(), State::#state{}, CommandType::atom()) ->
    {PoolName::atom() | undefined, Version::integer()}.
get_pool_by_slot(Slot, State, CommandType) ->
    Index = element(Slot+1,State#state.slots),
    Cluster =  case {CommandType, State#state.replica_read_flag}  of 
        {read, true} -> 
            ReadClusters = [Cluster || Cluster <- tuple_to_list(State#state.slots_maps), Cluster#slots_map.index == Index, Cluster#slots_map.type == replica],
            lists:nth(util:get_random_number(length(ReadClusters)), ReadClusters); %% Gets a random element from the read replicas
        _ ->
            hd([Cluster || Cluster <- tuple_to_list(State#state.slots_maps), Cluster#slots_map.index == Index, Cluster#slots_map.type == master])
    end,    
    if
        Cluster#slots_map.node =/= undefined ->
            {Cluster#slots_map.node#node.pool, State#state.version};
        true ->
            {undefined, State#state.version}
    end.

-spec get_pool_by_slot(Slot::integer(), CommandType::atom()) ->
    {PoolName::atom() | undefined, Version::integer()}.
get_pool_by_slot(Slot, CommandType) when is_atom(CommandType)->
    State = get_state(),
    get_pool_by_slot(Slot, State, CommandType).

-spec get_pool_by_slot(Slot::integer()) ->
    {PoolName::atom() | undefined, Version::integer()}.
get_pool_by_slot(Slot) ->
    get_pool_by_slot(Slot, write).

-spec get_write_pool_by_slot(Slot::integer(), State::#state{}) ->
    {PoolName::atom() | undefined, Version::integer()}.
get_write_pool_by_slot(Slot, State) -> 
    get_pool_by_slot(Slot, State, write).


-spec reload_slots_map(State::#state{}) -> NewState::#state{}.
reload_slots_map(State) ->
    [close_connection(SlotsMap)
        || SlotsMap <- tuple_to_list(State#state.slots_maps)],

    ClusterSlots = get_cluster_slots(State#state.init_nodes),

    SlotsMaps = parse_cluster_slots(ClusterSlots, State#state.replica_read_flag),
    ConnectedSlotsMaps = connect_all_slots(SlotsMaps, State#state.replica_read_flag),

    Slots = create_slots_cache(ConnectedSlotsMaps),
    NewState = State#state{
        slots = list_to_tuple(Slots),
        slots_maps = list_to_tuple(ConnectedSlotsMaps),
        version = State#state.version + 1
    },

    true = ets:insert(?MODULE, [{cluster_state, NewState}]),
    
    NewState.

-spec get_cluster_slots([#node{}]) -> [[bitstring() | [bitstring()]]].
get_cluster_slots([]) ->
    throw({error,cannot_connect_to_cluster});
get_cluster_slots([Node|T]) ->
    case safe_eredis_start_link(Node#node.address, Node#node.port) of
        {ok,Connection} ->
          case eredis:q(Connection, ["CLUSTER", "SLOTS"]) of
            {error,<<"ERR unknown command 'CLUSTER'">>} ->
                get_cluster_slots_from_single_node(Node);
            {error,<<"ERR This instance has cluster support disabled">>} ->
                get_cluster_slots_from_single_node(Node);
            {ok, ClusterInfo} ->
                eredis:stop(Connection),
                ClusterInfo;
            _ ->
                eredis:stop(Connection),
                get_cluster_slots(T)
        end;
        _ ->
            get_cluster_slots(T)
  end.

-spec get_cluster_slots_from_single_node(#node{}) ->
    [[bitstring() | [bitstring()]]].
get_cluster_slots_from_single_node(Node) ->
    [[<<"0">>, integer_to_binary(?REDIS_CLUSTER_HASH_SLOTS-1),
    [list_to_binary(Node#node.address), integer_to_binary(Node#node.port)]]].

-spec parse_cluster_slots([[bitstring() | [bitstring()]]], boolean()) -> [#slots_map{}].
parse_cluster_slots(ClusterInfo, ReplicaReadsFlag) ->
    parse_cluster_slots(ClusterInfo, 1, [], ReplicaReadsFlag).

parse_cluster_slots([[StartSlot, EndSlot | [[Address, Port | _] | ReplicaSlots]] | T], Index, Acc, ReplicaReadsFlag) ->
    SlotsMap =
        #slots_map{
            index = Index,
            start_slot = binary_to_integer(StartSlot),
            end_slot = binary_to_integer(EndSlot),
            type = master,
            node = #node{
                address = binary_to_list(Address),
                port = binary_to_integer(Port)
            }
        },
    ReplicaCluster = case ReplicaReadsFlag of 
        true ->  
            parse_replica_slots(ReplicaSlots, Index, {StartSlot, EndSlot}, []);
        _ -> 
            []
    end,
    parse_cluster_slots(T, Index+1, [SlotsMap | ReplicaCluster ++ Acc], ReplicaReadsFlag);
parse_cluster_slots([], _Index, Acc, _) ->
    lists:reverse(Acc).

parse_replica_slots([[Address, Port, _] | Slots], Index, {StartSlot, EndSlot}, Acc) ->
    ReplicaSlotsMaps = #slots_map{
        index = Index,
        type = replica,
        start_slot = binary_to_integer(StartSlot),
        end_slot = binary_to_integer(EndSlot),
        node = #node{
            address = binary_to_list(Address),
            port = binary_to_integer(Port)
        }
    },
    parse_replica_slots(Slots, Index, {StartSlot, EndSlot}, [ReplicaSlotsMaps | Acc]);
parse_replica_slots([], _, _, Acc) ->
    Acc.  

-spec close_connection(#slots_map{}) -> ok.
close_connection(SlotsMap) ->
    Node = SlotsMap#slots_map.node,
    if
        Node =/= undefined ->
            try eredis_cluster_pool:stop(Node#node.pool) of
                _ ->
                    ok
            catch
                _ ->
                    ok
            end;
        true ->
            ok
    end.

-spec connect_node(#node{}) -> #node{} | undefined.
connect_node(Node) ->
    case eredis_cluster_pool:create(Node#node.address, Node#node.port) of
        {ok, Pool} ->
            Node#node{pool=Pool};
        _ ->
            undefined
    end.

safe_eredis_start_link(Address,Port) ->
    process_flag(trap_exit, true),
    DataBase = application:get_env(eredis_cluster, database, 0),
    Password = application:get_env(eredis_cluster, password, ""),
    Payload = eredis:start_link(Address, Port, DataBase, Password),
    process_flag(trap_exit, false),
    Payload.

-spec create_slots_cache([#slots_map{}]) -> [integer()].
create_slots_cache(SlotsMaps) ->
  SlotsCache = [[{Index,SlotsMap#slots_map.index}
        || Index <- lists:seq(SlotsMap#slots_map.start_slot,
            SlotsMap#slots_map.end_slot)]
        || SlotsMap <- SlotsMaps, SlotsMap#slots_map.type == master], %% Only make cache slot from master nodes
  SlotsCacheF = lists:flatten(SlotsCache),
  SortedSlotsCache = lists:sort(SlotsCacheF),
  [ Index || {_,Index} <- SortedSlotsCache].

-spec connect_all_slots([#slots_map{}], ReplicaReadsFlag::boolean()) -> [integer()].
connect_all_slots(SlotsMapList, false) ->
    [SlotsMap#slots_map{node=connect_node(SlotsMap#slots_map.node)}
        || SlotsMap <- SlotsMapList];
    
connect_all_slots(SlotsMapList, true) ->
    ConnectedSlotsMaps = [SlotsMap#slots_map{node=connect_node(SlotsMap#slots_map.node)}
        || SlotsMap <- SlotsMapList],
    lists:foreach(
        fun(ConnectedSlot) -> 
            Node = ConnectedSlot#slots_map.node,
            Pool = Node#node.pool,
            Command = ["READONLY"],
            ok = eredis_cluster_pool:transaction_all(Pool, Command)
        end,
        ConnectedSlotsMaps
    ),
    ConnectedSlotsMaps.

-spec connect_([{Address::string(), Port::integer()}], ReplicaReadsFlag::boolean()) -> #state{}.
connect_([], ReplicaReadsFlag) ->
    #state{
        replica_read_flag = ReplicaReadsFlag
    };
connect_(InitNodes, ReplicaReadsFlag) ->
    State = #state{
        slots = undefined,
        slots_maps = {},
        init_nodes = [#node{address = A, port = P} || {A,P} <- InitNodes],
        version = 0,
        replica_read_flag = ReplicaReadsFlag
    },
    reload_slots_map(State).

%% gen_server.

init(_Args) ->
    ets:new(?MODULE, [protected, set, named_table, {read_concurrency, true}]),
    InitNodes = application:get_env(eredis_cluster, init_nodes, []),
    ReplicaReadsFlag = case application:get_env(eredis_cluster, replica_read_flag, undefined) of 
        [] -> false;
        [true] -> true;
        true -> true;
        _ -> false
    end,
    {ok, connect_(InitNodes, ReplicaReadsFlag)}.

handle_call({reload_slots_map,Version}, _From, #state{version=Version} = State) ->
    {reply, ok, reload_slots_map(State)};
handle_call({reload_slots_map,_}, _From, State) ->
    {reply, ok, State};
handle_call({connect, InitServers, ReplicaReadsFlag}, _From, _State) ->
    {reply, ok, connect_(InitServers, ReplicaReadsFlag)};
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
