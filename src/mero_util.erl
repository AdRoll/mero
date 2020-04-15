-module(mero_util).

-export([foreach/2, to_int/1, to_bin/1]).

%%%===================================================================
%%% API
%%%===================================================================

-spec foreach(fun((Elem :: term()) -> {break, term()} | continue), list()) -> term().
foreach(Fun, L) ->
    foreach(continue, Fun, L).

to_int(Value) when is_integer(Value) ->
    Value;
to_int(Value) when is_list(Value) ->
    list_to_integer(Value);
to_int(Value) when is_binary(Value) ->
    to_int(binary_to_list(Value)).

to_bin(Value) when is_binary(Value) ->
    Value;
to_bin(Value) when is_integer(Value) ->
    to_bin(integer_to_list(Value));
to_bin(Value) when is_list(Value) ->
    list_to_binary(Value).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

foreach({break, Reason}, _Fun, _L) ->
    Reason;
foreach(continue, _Fun, []) ->
    ok;
foreach(continue, Fun, [H | Rest]) ->
    foreach(Fun(H), Fun, Rest).
