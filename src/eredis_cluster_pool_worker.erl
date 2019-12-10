-module(eredis_cluster_pool_worker).
-behaviour(gen_server).
-behaviour(poolboy_worker).

%% API.
-export([start_link/1]).
-export([query/2, query_nor/2]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-record(state, {conn}).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init(Args) ->
    Hostname = proplists:get_value(host, Args),
    Port = proplists:get_value(port, Args),
    DataBase = proplists:get_value(database, Args, 0),
    Password = proplists:get_value(password, Args, ""),

    process_flag(trap_exit, true),
    Result = eredis:start_link(Hostname,Port, DataBase, Password),
    process_flag(trap_exit, false),

    Conn = case Result of
        {ok,Connection} ->
            Connection;
        _ ->
            undefined
    end,

    {ok, #state{conn=Conn}}.

query(Worker, Commands) ->
    gen_server:call(Worker, {'query', Commands}).

query_nor(Worker, Commands) ->
    gen_server:cast(Worker, {'query_nor', Commands}).

handle_call({'query', _}, _From, #state{conn = undefined} = State) ->
    {reply, {error, no_connection}, State};
handle_call({'query', [[X|_]|_] = Commands}, _From, #state{conn = Conn} = State)
    when is_list(X); is_binary(X) ->
    {reply, eredis:qp(Conn, Commands), State};
handle_call({'query', Command}, _From, #state{conn = Conn} = State) ->
    {reply, eredis:q(Conn, Command), State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({'query_nor', Command}, #state{conn = Conn} = State) ->
    eredis:q_noreply(Conn, Command),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{conn=Conn}) ->
    ok = eredis:stop(Conn),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
