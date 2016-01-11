-module(eredis_cluster).
-behaviour(application).

-define(REDIS_CLUSTER_REQUEST_TTL,16).
-define(REDIS_RETRY_DELAY,100).

-export([start/0, start/2]).
-export([stop/0, stop/1]).
-export([connect/1]).
-export([q/1, qp/1, transaction/1]).

-type anystring() :: string() | bitstring().

-type redis_simple_command() :: [anystring()].
-type redis_pipeline_command() :: [redis_simple_command()].
-type redis_command() :: redis_simple_command() | redis_pipeline_command().

-type redis_error_result() :: Reason::bitstring() | no_connection
    | invalid_cluster_command.
-type redis_success_result() :: Result::bitstring().
-type redis_simple_result() :: {ok, redis_success_result()}
    | {error, redis_error_result()}.
-type redis_pipeline_result() :: [redis_simple_result()].
-type redis_transaction_result() :: {ok, [redis_success_result()]}
    | {error, redis_error_result()}.
-type redis_result() :: redis_simple_result() | redis_pipeline_result().

-spec start(StarType::application:start_type(), StartArgs::term()) ->
    {ok, pid()}.
start(_Type, _Args) ->
    eredis_cluster_sup:start_link().

-spec stop(State::term()) -> ok.
stop(_State) ->
    ok.

-spec start() -> ok | {error, Reason::term()}.
start() ->
    application:start(?MODULE).

-spec stop() -> ok | {error, Reason::term()}.
stop() ->
    application:stop(?MODULE).

-spec connect(InitServers::term()) -> Result::term().
connect(InitServers) ->
    eredis_cluster_monitor:connect(InitServers).

-spec q(redis_command()) -> redis_result().
q(Command) ->
    q(Command,0).
q(_,?REDIS_CLUSTER_REQUEST_TTL) ->
    {error,no_connection};
q(Command,Counter) ->

    %% Throttle retries
    if
        Counter > 1 ->
            timer:sleep(?REDIS_RETRY_DELAY);
        true ->
            ok
    end,

    %% Extract key from request
    case get_key_from_command(Command) of
        undefined ->
            {error, invalid_cluster_command};
        Key ->
            Slot = get_key_slot(Key),

            case eredis_cluster_monitor:get_pool_by_slot(Slot) of
                {Version, undefined} ->
                    eredis_cluster_monitor:refresh_mapping(Version),
                    q(Command, Counter+1);

                {Version, Pool} ->
                    case query_eredis_pool(Pool, Command) of
                        {error, no_connection} ->
                            eredis_cluster_monitor:refresh_mapping(Version),
                            q(Command, Counter+1);

                        {error, <<"MOVED ", _RedirectionInfo/binary>>} ->
                            eredis_cluster_monitor:refresh_mapping(Version),
                            q(Command, Counter+1);

                        Payload ->
                            Payload
                    end
            end
    end.

-spec qp(redis_pipeline_command()) -> redis_pipeline_result().
qp(Commands) ->
    q(Commands).

-spec transaction(redis_pipeline_command()) -> redis_transaction_result().
transaction(Commands) ->
    Transaction = [["multi"]|Commands] ++ [["exec"]],
    Result = qp(Transaction),
    lists:last(Result).

-spec query_eredis_pool(atom(), redis_command()) -> redis_result().
query_eredis_pool(PoolName, [[X|Y]|Z]) when is_list(X); is_binary(X) ->
    query_eredis_pool(PoolName, [[X|Y]|Z], qp);
query_eredis_pool(PoolName, Command) ->
    query_eredis_pool(PoolName, Command, q).
query_eredis_pool(PoolName, Params, Type) ->
    try
        poolboy:transaction(PoolName, fun(Worker) ->
            gen_server:call(Worker, {Type, Params})
        end)
    catch
        exit:_ ->
            {error,no_connection}
    end.

%% =============================================================================
%% @doc Return the hash slot from the key
%% @end
%% =============================================================================

-spec get_key_slot(Key::anystring()) -> Slot::integer().
get_key_slot(Key) when is_bitstring(Key) ->
    get_key_slot(bitstring_to_list(Key));
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
%% In a normal query, the second term will be returned
%%
%% If it is a pipeline query we will use the second term of the first term, we
%% will assume that all keys are in the same server and the query can be
%% performed
%%
%% If the pipeline query starts with multi (transaction), we will look at the
%% second term of the second command
%%
%% For commands that don't make sense in the context of cluster
%% return value will be undefined.
%% @end
%% =============================================================================

-spec get_key_from_command(redis_command()) -> string() | undefined.
get_key_from_command([[X|Y]|Z]) when is_bitstring(X) ->
    get_key_from_command([[bitstring_to_list(X)|Y]|Z]);
get_key_from_command([[X|Y]|Z]) when is_list(X) ->
    case string:to_lower(X) of
        "multi" ->
            get_key_from_command(Z);
        _ ->
            get_key_from_command([X|Y])
    end;
get_key_from_command([Term1,Term2|Rest]) when is_bitstring(Term1) ->
    get_key_from_command([bitstring_to_list(Term1),Term2|Rest]);
get_key_from_command([Term1,Term2|Rest]) when is_bitstring(Term2) ->
    get_key_from_command([Term1,bitstring_to_list(Term2)|Rest]);
get_key_from_command([Term1,Term2|Rest]) ->
    case string:to_lower(Term1) of
        "info" ->
            undefined;
        "config" ->
            undefined;
        "shutdown" ->
            undefined;
        "slaveof" ->
            undefined;
        "eval" ->
            get_key_from_rest(Rest);
        "evalsha" ->
            get_key_from_rest(Rest);
        _ ->
            Term2
    end;
get_key_from_command(_) ->
    undefined.

-spec get_key_from_rest([anystring()]) -> string() | undefined.
get_key_from_rest([_,KeyName|_]) when is_bitstring(KeyName) ->
    bitstring_to_list(KeyName);
get_key_from_rest([_,KeyName|_]) when is_list(KeyName) ->
    KeyName;
get_key_from_rest(_) ->
    undefined.
