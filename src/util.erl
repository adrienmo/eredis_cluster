-module(util).

-export([get_random_number/1]).

-ifdef(ERLANG_OTP_VERSION_17).
get_random_number(N) -> random:seed(now()), random:uniform(N).
-else.
get_random_number(N) -> rand:uniform(N).
-endif.