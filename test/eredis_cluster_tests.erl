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

            { "binary",
            fun() ->
                ?assertEqual({ok, <<"OK">>}, eredis_cluster:q([<<"SET">>, <<"key_binary">>, <<"value_binary">>])),
                ?assertEqual({ok, <<"value_binary">>}, eredis_cluster:q([<<"GET">>,<<"key_binary">>])),
                ?assertEqual([{ok, <<"value_binary">>},{ok, <<"value_binary">>}], eredis_cluster:qp([[<<"GET">>,<<"key_binary">>],[<<"GET">>,<<"key_binary">>]]))
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
            },

            { "transaction",
            fun () ->
                ?assertMatch({ok,[_,_,_]}, eredis_cluster:transaction([["get","abc"],["get","abc"],["get","abc"]])),
                ?assertMatch({error,_}, eredis_cluster:transaction([["get","abc"],["get","abcde"],["get","abcd1"]]))
            end
            },

            { "eval key",
            fun () ->
                eredis_cluster:q(["del", "foo"]),
                eredis_cluster:q(["eval","return redis.call('set',KEYS[1],'bar')", "1", "foo"]),
                ?assertEqual({ok, <<"bar">>}, eredis_cluster:q(["GET", "foo"]))
            end
            },

            { "evalsha",
            fun () ->
                eredis_cluster:q(["del", "foo2"]),
                {ok, Hash} = eredis_cluster:q(["script","load","return redis.call('set',KEYS[1],'bar')"]),
                erlang:display({hash,Hash}),
                erlang:display(eredis_cluster:q(["evalsha", Hash, 1, "foo2"])),
                ?assertEqual({ok, <<"bar">>}, eredis_cluster:q(["GET", "foo2"]))
            end
            },

            { "bitstring support",
            fun () ->
                eredis_cluster:q([<<"set">>, <<"bitstring">>,<<"support">>]),
                ?assertEqual({ok, <<"support">>}, eredis_cluster:q([<<"GET">>, <<"bitstring">>]))
            end
            }

      ]
    }
}.
