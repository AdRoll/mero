-module(mero_util).

-export([foreach/2]).

%%%===================================================================
%%% API
%%%===================================================================

-spec foreach(fun((Elem :: term()) -> {break, term()} | continue),
              list()) -> term().
foreach(Fun, L) ->
    foreach(continue, Fun, L).


%%%===================================================================
%%% Internal Functions
%%%===================================================================

foreach({break, Reason}, _Fun, _L) ->
    Reason;
foreach(continue, _Fun, []) ->
    ok;
foreach(continue, Fun, [H|Rest]) ->
    foreach(Fun(H), Fun, Rest).
