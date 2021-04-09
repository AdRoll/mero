-module(mero_stat).

%% noop/1 intentionally discards its argument.
-hank([{unnecessary_function_arguments, [{noop, 1, 1}]}]).

-export([noop/1, incr/1]).

incr(Key) ->
    {StatMod, StatFun} = mero_conf:stat_callback(),
    erlang:apply(StatMod, StatFun, [Key, 1]).

noop(_Key) ->
    ok.
