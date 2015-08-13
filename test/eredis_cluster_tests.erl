-module(eredis_cluster_tests).

-include_lib("eunit/include/eunit.hrl").

-define(Setup, fun() ->
    application:start(eredis_cluster)
end).
-define(Clearnup, fun(_) -> application:stop(eredis_cluster)  end).

basic_test_() ->
    {inparallel,
        {setup, ?Setup, ?Clearnup,
        [
            { "get and set",
            fun() ->
                ?assertEqual({ok, <<"OK">>}, eredis_cluster:q(["SET", "key", "value"])),
                ?assertEqual({ok, <<"value">>}, eredis_cluster:q(["GET","key"])),
                ?assertEqual({ok, undefined}, eredis_cluster:q(["GET","nonexists"]))
            end
            },

            { "delete test",
            fun() ->
                ?assertMatch({ok, _}, eredis_cluster:q(["DEL", "a"])),
                ?assertEqual({ok, <<"OK">>}, eredis_cluster:q(["SET", "b", "a"])),
                ?assertEqual({ok, <<"1">>}, eredis_cluster:q(["DEL", "b"])),
                ?assertEqual({ok, undefined}, eredis_cluster:q(["GET", "b"]))
            end
            },

            { "pipeline",
            fun () ->
                ?assertNotMatch([{ok, _},{ok, _},{ok, _}], eredis_cluster:qp([["SET", "a1", "aaa"], ["SET", "a2", "aaa"], ["SET", "a3", "aaa"]])),
                ?assertMatch([{ok, _},{ok, _},{ok, _}], eredis_cluster:qp([["LPUSH", "a", "aaa"], ["LPUSH", "a", "bbb"], ["LPUSH", "a", "ccc"]]))
            end
            }

      ]
    }
}.

transaction_test_() ->
    {inparallel,
        {setup, ?Setup, ?Clearnup,
        [
            { "transaction",
            fun () ->
                ?assertMatch({ok, _},  eredis_cluster:transaction(fun (Worker) ->
                    gen_server:call(Worker, {q, ["SET", "aa", "111"]}),
                    gen_server:call(Worker, {q, ["DEL", "aa"]})
                end, "aa")),
<<<<<<< HEAD

                %%TODO this test may not pass in certain cluster configuration
=======
>>>>>>> e00f4b00095514a5e2e5ea4cfd3da9f44428e488
                ?assertMatch({error, _},  eredis_cluster:transaction(fun (Worker) ->
                    gen_server:call(Worker, {q, ["SET", "aa", "111"]}),
                    gen_server:call(Worker, {q, ["DEL", "bb"]})
                end, "aa"))
            end
            }
        ]
        }
    }.
