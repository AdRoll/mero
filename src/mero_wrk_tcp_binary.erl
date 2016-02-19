%% Copyright (c) 2014, AdRoll
%% All rights reserved.
%%
%% Redistribution and use in source and binary forms, with or without
%% modification, are permitted provided that the following conditions are met:
%%
%% * Redistributions of source code must retain the above copyright notice, this
%% list of conditions and the following disclaimer.
%%
%% * Redistributions in binary form must reproduce the above copyright notice,
%% this list of conditions and the following disclaimer in the documentation
%% and/or other materials provided with the distribution.
%%
%% * Neither the name of the {organization} nor the names of its
%% contributors may be used to endorse or promote products derived from
%% this software without specific prior written permission.
%%
%% THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
%% AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
%% IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
%% DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
%% FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
%% DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
%% SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
%% CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
%% OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
%% OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
%%
-module(mero_wrk_tcp_binary).

-author('Miriam Pena <miriam.pena@adroll.com>').

-include_lib("mero/include/mero.hrl").


%%% Start/stop functions
-export([connect/3,
    controlling_process/2,
    transaction/3,
    close/2]).


-record(client, {socket, pool, event_callback :: module()}).

-define(SOCKET_OPTIONS, [binary,
    {packet, raw},
    {active, false},
    {reuseaddr, true},
    {nodelay, true}]).

%%%=============================================================================
%%% External functions
%%%=============================================================================

%% API functions
connect(Host, Port, CallbackInfo) ->
    case gen_tcp:connect(Host, Port, ?SOCKET_OPTIONS) of
        {ok, Socket} ->
            ?LOG_EVENT(CallbackInfo, [socket, connect, ok]),
            {ok, #client{socket = Socket, event_callback = CallbackInfo}};
        {error, Reason} ->
            ?LOG_EVENT(CallbackInfo, [socket, connect, error, {reason, Reason}]),
            {error, Reason}
    end.


controlling_process(Client, Pid) ->
    case gen_tcp:controlling_process(Client#client.socket, Pid) of
        ok ->
            ok;
        {error, Reason} ->
            ?LOG_EVENT(Client#client.event_callback, [socket, controlling_process, error, {reason, Reason}]),
            {error, Reason}
    end.

transaction(Client, delete, [Key, TimeLimit]) ->
    case send_receive(Client, {?MEMCACHE_DELETE, {Key}}, TimeLimit) of
        {ok, {<<>>, <<>>}} ->
            {Client, ok};
        {ok, {<<>>, undefined}} ->
            {Client, {error, not_found}};
        {ok, {error, Reason}} ->
            {Client, {error, Reason}};
        {error, Reason} ->
            {error, Reason}
    end;


transaction(Client, increment_counter, [Key, Value, Initial, ExpTime, TimeLimit]) ->
    case send_receive(Client, {?MEMCACHE_INCREMENT, {Key, Value, Initial, ExpTime}}, TimeLimit) of
        {error, Reason} ->
            {error, Reason};
        {ok, {error, Reason}} ->
            {Client, {error, Reason}};
        {ok, {<<>>, ActualValue}} ->
            {Client, {ok, to_integer(ActualValue)}}
    end;


transaction(Client, set, [Key, Value, ExpTime, TimeLimit]) ->
    case send_receive(Client, {?MEMCACHE_SET, {Key, Value, ExpTime}}, TimeLimit) of
        {ok, {<<>>, <<>>}} ->
            {Client, ok};
        {ok, {error, Reason}} ->
            {Client, {error, Reason}};
        {error, Reason} ->
            {error, Reason}
    end;

transaction(Client, add, [Key, Value, ExpTime, TimeLimit]) ->
    case send_receive(Client, {?MEMCACHE_ADD, {Key, Value, ExpTime}}, TimeLimit) of
        {ok, {<<>>, <<>>}} ->
            {Client, ok};
        {ok, {error, Reason}} ->
            {Client, {error, Reason}};
        {error, Reason} ->
            {error, Reason}
    end;

transaction(Client, flush_all, [TimeLimit]) ->
    case send_receive(Client, {?MEMCACHE_FLUSH_ALL, {}}, TimeLimit) of
        {ok, {<<>>, <<>>}} ->
            {Client, ok};
        {ok, {error, Reason}} ->
            {Client, {error, Reason}};
        {error, Reason} ->
            {error, Reason}
    end;

transaction(Client, async_mget, [Keys]) ->
    case async_mget(Client, Keys) of
        {error, Reason} ->
            {error, Reason};
        {ok, {error, Reason}} ->
            {Client, {error, Reason}};
        {ok, ok} ->
            {Client, ok}
    end;

transaction(Client, async_increment, [Keys]) ->
    case async_increment(Client, Keys) of
        {error, Reason} ->
            {error, Reason};
        {ok, {error, Reason}} ->
            {Client, {error, Reason}};
        {ok, ok} ->
            {Client, ok}
    end;

transaction(Client, async_delete, [Keys]) ->
    case async_delete(Client, Keys) of
        {error, Reason} ->
            {error, Reason};
        {ok, {error, Reason}} ->
            {Client, {error, Reason}};
        {ok, ok} ->
            {Client, ok}
    end;

transaction(Client, async_response, [Keys, Timeout]) ->
    case async_response(Client, Keys, Timeout) of
        {error, Reason} ->
            {error, Reason};
        {ok, {error, Reason}} ->
            {Client, {error, Reason}};
        {ok, Results} ->
            {Client, Results}
    end;

transaction(Client, async_blank_response, [Keys, Timeout]) ->
    case async_blank_response(Client, Keys, Timeout) of
        {error, Reason} ->
            {error, Reason};
        {ok, {error, Reason}} ->
            {Client, {error, Reason}};
        {ok, Results} ->
            {Client, Results}
    end.

close(Client, Reason) ->
    ?LOG_EVENT(Client#client.event_callback, [closing_socket, {reason, Reason}]),
    gen_tcp:close(Client#client.socket).


%%%=============================================================================
%%% Internal functions
%%%=============================================================================

send_receive(Client, {Op, _Args} = Cmd, TimeLimit) ->
    try
        Data = pack(Cmd),
        ok = send(Client, Data),
        {ok, receive_response(Client, Op, TimeLimit)}
    catch
        throw:{failed, Reason} ->
            {error, Reason}
    end.


pack({?MEMCACHE_INCREMENT, {Key, Value, Initial, ExpTime}}) ->
    IntValue = value_to_integer(Value),
    IntInitial = value_to_integer(Initial),
    IntExpTime = value_to_integer(ExpTime),
    pack(<<IntValue:64, IntInitial:64, IntExpTime:32>>, ?MEMCACHE_INCREMENT, Key);

pack({?MEMCACHE_INCREMENTQ, {Key, Value, Initial, ExpTime}}) ->
    IntValue = value_to_integer(Value),
    IntInitial = value_to_integer(Initial),
    IntExpTime = value_to_integer(ExpTime),
    pack(<<IntValue:64, IntInitial:64, IntExpTime:32>>, ?MEMCACHE_INCREMENTQ, Key);

pack({?MEMCACHE_DELETE, {Key}}) ->
    pack(<<>>, ?MEMCACHE_DELETE, Key);

pack({?MEMCACHE_DELETEQ, {Key}}) ->
    pack(<<>>, ?MEMCACHE_DELETEQ, Key);

pack({?MEMCACHE_ADD, {Key, Value, ExpTime}}) ->
    IntExpTime = value_to_integer(ExpTime),
    pack(<<16#DEADBEEF:32, IntExpTime:32>>, ?MEMCACHE_ADD, Key, Value);

pack({?MEMCACHE_SET, {Key, Value, ExpTime}}) ->
    IntExpTime = value_to_integer(ExpTime),
    pack(<<16#DEADBEEF:32, IntExpTime:32>>, ?MEMCACHE_SET, Key, Value);

pack({?MEMCACHE_FLUSH_ALL, {}}) ->
    %% Flush inmediately by default
    ExpirationTime = 16#00,
    pack(<<ExpirationTime:32>>, ?MEMCACHE_FLUSH_ALL, <<>>);

pack({getkq, Key}) ->
    pack(<<>>, ?MEMCACHE_GETKQ, Key);

pack({getk, Key}) ->
    pack(<<>>, ?MEMCACHE_GETK, Key).


pack(Extras, Operator, Key) ->
    pack(Extras, Operator, Key, <<>>).

pack(Extras, Operator, Key, Value) ->
    KeySize = size(Key),
    ExtrasSize = size(Extras),
    Body = <<Extras:ExtrasSize/binary, Key/binary, Value/binary>>,
    BodySize = size(Body),
    <<16#80:8, Operator:8, KeySize:16,
    ExtrasSize:8, 16#00:8, 16#00:16,
    BodySize:32, 16#00:32, 16#00:64, Body:BodySize/binary>>.


send(Client, Data) ->
    case gen_tcp:send(Client#client.socket, Data) of
        ok ->
            ok;
        {error, Reason} ->
            ?LOG_EVENT(Client#client.event_callback, [memcached_send_error, {reason, Reason}]),
            throw({failed, {send, Reason}})
    end.


receive_response(Client, Op, TimeLimit) ->
    case recv_bytes(Client, 24, TimeLimit) of
        <<16#81:8, Op:8, KeySize:16, ExtrasSize:8, _DT:8, Status:16,
        BodySize:32, _Opq:32, _CAS:64>> ->
            case recv_bytes(Client, BodySize, TimeLimit) of
                <<_Extras:ExtrasSize/binary, Key:KeySize/binary, Value/binary>> ->
                    case Status of
                        16#0001 ->
                            {Key, undefined};
                        16#0000 ->
                            {Key, Value};
                        16#0002 ->
                            {error, already_exists};
                        16#0003 ->
                            {error, value_too_large};
                        16#0004 ->
                            {error, invalid_arguments};
                        16#0005 ->
                            {error, not_stored};
                        16#0006 ->
                            {error, incr_decr_on_non_numeric_value};
                        16#0081 ->
                            {error, unknown_command};
                        16#0082 ->
                            {error, out_of_memory};
                        Status ->
                            throw({failed, {response_status, Status}})
                    end;
                Data ->
                    throw({failed, {unexpected_body, Data}})
            end;
        Data ->
            throw({failed, {unexpected_header, Data, {expected, Op}}})
    end.


recv_bytes(_Client, 0, _TimeLimit) ->
    <<>>;
recv_bytes(Client, NumBytes, TimeLimit) ->
    Timeout = mero_conf:millis_to(TimeLimit),
    case gen_tcp_recv(Client#client.socket, NumBytes, Timeout) of
        {ok, Bin} ->
            Bin;
        {error, Reason} ->
            ?LOG_EVENT(Client#client.event_callback, [memcached_receive_error, {reason, Reason}]),
            throw({failed, {receive_bytes, Reason}})
    end.


value_to_integer(Value) ->
    value_to_integer(Value, []).

value_to_integer(<<>>, Acc) ->
    list_to_integer(lists:reverse(Acc));
value_to_integer(<<0, Rest/binary>>, Acc) ->
    value_to_integer(Rest, Acc);
value_to_integer(<<K, Rest/binary>>, Acc) ->
    value_to_integer(Rest, [K | Acc]).

to_integer(Binary) when is_binary(Binary) ->
    Size = bit_size(Binary),
    <<Value:Size/integer>> = Binary,
    Value.

gen_tcp_recv(Socket, NumBytes, Timeout) ->
    gen_tcp:recv(Socket, NumBytes, Timeout).


async_mget(Client, Keys) ->
    try
        {ok, send_gets(Client, Keys)}
    catch
        throw:{failed, Reason} ->
            {error, Reason}
    end.

async_delete(Client, Keys) ->
    try
        {ok, lists:foldl(fun(K, ok) -> send(Client, pack({?MEMCACHE_DELETEQ, {K}})) end, ok, Keys)}
    catch
        throw:{failed, Reason} ->
            {error, Reason}
    end.

async_increment(Client, Keys) ->
    try
        {ok, lists:foreach(fun({K, Value, Initial, ExpTime}) ->
                                   send(Client, pack({?MEMCACHE_INCREMENTQ, {K, Value, Initial, ExpTime}}))
                           end, Keys)}
    catch
        throw:{failed, Reason} ->
            {error, Reason}
    end.

send_gets(Client, [Key]) ->
    ok = send(Client, pack({getk, Key}));
send_gets(Client, [Key | Keys]) ->
    ok = send(Client, pack({getkq, Key})),
    send_gets(Client, Keys).


async_response(Client, Keys, TimeLimit) ->
    try
        {ok, receive_response(Client, TimeLimit, Keys, [])}
    catch
        throw:{failed, Reason} ->
            {error, Reason}
    end.

receive_response(Client, TimeLimit, Keys, Acc) ->
    case recv_bytes(Client, 24, TimeLimit) of
        <<16#81:8, Op:8, KeySize:16, ExtrasSize:8, _DT:8, Status:16,
        BodySize:32, _Opq:32, _CAS:64>> = Data ->
            case recv_bytes(Client, BodySize, TimeLimit) of
                <<_Extras:ExtrasSize/binary, Key:KeySize/binary, ValueReceived/binary>> ->
                    {Key, Value} = filter_by_status(Status, Op, Key, ValueReceived),
                    Responses = [{Key, Value} | Acc],
                    NKeys = lists:delete(Key, Keys),
                    case Op of
                        %% On silent we expect more values
                        ?MEMCACHE_GETKQ ->
                            receive_response(Client, TimeLimit, NKeys, Responses);
                        %% This was the last one!
                        ?MEMCACHE_GETK ->
                            Responses ++ [{KeyIn, undefined} || KeyIn <- NKeys]
                    end;
                Data ->
                    throw({failed, {unexpected_body, Data}})
            end;
        Data ->
            throw({failed, {unexpected_header, Data}})
    end.

async_blank_response(_Client, _Keys, _TimeLimit) ->
    {ok, [ok]}.

filter_by_status(16#0000, _Op, Key, ValueReceived)  -> {Key, ValueReceived};
filter_by_status(16#0001, _Op, Key, _ValueReceived) -> {Key, undefined};
filter_by_status(Status, _Op, _Key, _ValueReceived) -> throw({failed, {response_status, Status}}).
