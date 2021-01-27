-module(mero_stat).

%% noop/1 intentionally discards its argument.
-hank([unused_ignored_function_params]).

-export([noop/1, incr/1]).

incr(Key) ->
    {StatMod, StatFun} = mero_conf:stat_callback(),
    erlang:apply(StatMod, StatFun, [Key, 1]).

noop(_Key) ->
    ok.
