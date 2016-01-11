-module(eredis_cluster_worker).
-behaviour(gen_server).
-behaviour(poolboy_worker).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
        code_change/3]).

-record(state, {conn}).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init(Args) ->
    Hostname = proplists:get_value(host, Args),
    Port = proplists:get_value(port, Args),

    process_flag(trap_exit, true),
    Result = eredis:start_link(Hostname,Port),
    process_flag(trap_exit, false),

    Conn = case Result of
        {ok,Connection} ->
            Connection;
        _ ->
            undefined
    end,

    {ok, #state{conn=Conn}}.

handle_call({q, _}, _From, #state{conn=undefined}=State) ->
    {reply, {error,no_connection}, State};
handle_call({q, Params}, _From, #state{conn=Conn}=State) ->
    {reply, eredis:q(Conn,Params), State};
handle_call({qp, _}, _From, #state{conn=undefined}=State) ->
    {reply, {error,no_connection}, State};
handle_call({qp, Params}, _From, #state{conn=Conn}=State) ->
    {reply, eredis:qp(Conn,Params), State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{conn=Conn}) ->
    ok = eredis:stop(Conn),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
