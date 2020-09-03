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

-behavior(mero_pool).

%%% Start/stop functions
-export([connect/3, controlling_process/2, transaction/3, close/2]).

-record(client, {socket, pool, event_callback :: {module(), atom(), [term()]}}).

-define(SOCKET_OPTIONS,
        [binary,
         {packet, raw},
         {recbuf, 1024 * 32},
         {active, false},
         {reuseaddr, true},
         {nodelay, true}]).

%%%=============================================================================
%%% External functions
%%%=============================================================================

%% API functions
connect(Host, Port, CallbackInfo) ->
    ?LOG_EVENT(CallbackInfo, [socket, connecting]),
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
            ?LOG_EVENT(Client#client.event_callback,
                       [socket, controlling_process, error, {reason, Reason}]),
            {error, Reason}
    end.

transaction(Client, delete, [Key, TimeLimit]) ->
    case send_receive(Client, {?MEMCACHE_DELETE, {Key}}, TimeLimit) of
        {ok, #mero_item{key = <<>>, value = <<>>}} ->
            {Client, ok};
        {ok, #mero_item{key = <<>>, value = undefined}} ->
            {Client, {error, not_found}};
        {ok, {error, Reason}} ->
            {Client, {error, Reason}};
        {error, Reason} ->
            {error, Reason}
    end;
transaction(Client, increment_counter, [Key, Value, Initial, ExpTime, TimeLimit]) ->
    case send_receive(Client,
                      {?MEMCACHE_INCREMENT, {Key, Value, Initial, ExpTime}},
                      TimeLimit)
        of
        {error, Reason} ->
            {error, Reason};
        {ok, {error, Reason}} ->
            {Client, {error, Reason}};
        {ok, #mero_item{key = <<>>, value = ActualValue}} ->
            {Client, {ok, to_integer(ActualValue)}}
    end;
transaction(Client, get, [Key, TimeLimit]) ->
    case send_receive(Client, {?MEMCACHE_GET, {[Key]}}, TimeLimit) of
        {ok, #mero_item{key = <<>>} = Result} ->
            {Client, Result#mero_item{key = Key}};
        {ok, #mero_item{} = Result} ->
            {Client, Result};
        {ok, {error, Reason}} ->
            {Client, {error, Reason}};
        {error, Reason} ->
            {error, Reason}
    end;
transaction(Client, set, [Key, Value, ExpTime, TimeLimit, CAS]) ->
    case send_receive(Client, {?MEMCACHE_SET, {Key, Value, ExpTime, CAS}}, TimeLimit) of
        {ok, #mero_item{key = <<>>, value = <<>>}} ->
            {Client, ok};
        {ok, #mero_item{key = <<>>, value = undefined}} when CAS /= undefined ->
            %% attempt to set a key using CAS, but key wasn't present.
            {Client, {error, not_found}};
        {ok, {error, Reason}} ->
            {Client, {error, Reason}};
        {error, Reason} ->
            {error, Reason}
    end;
transaction(Client, add, [Key, Value, ExpTime, TimeLimit]) ->
    case send_receive(Client, {?MEMCACHE_ADD, {Key, Value, ExpTime}}, TimeLimit) of
        {ok, #mero_item{key = <<>>, value = <<>>}} ->
            {Client, ok};
        {ok, {error, Reason}} ->
            {Client, {error, Reason}};
        {error, Reason} ->
            {error, Reason}
    end;
transaction(Client, flush_all, [TimeLimit]) ->
    case send_receive(Client, {?MEMCACHE_FLUSH_ALL, {}}, TimeLimit) of
        {ok, #mero_item{key = <<>>, value = <<>>}} ->
            {Client, ok};
        {ok, {error, Reason}} ->
            {Client, {error, Reason}};
        {error, Reason} ->
            {error, Reason}
    end;
transaction(Client, async_mset, [KVECs]) ->
    case async_mset(Client, KVECs) of
        {error, Reason} ->
            {error, Reason};
        {ok, ok} ->
            {Client, ok}
    end;
transaction(Client, async_madd, [KVECs]) ->
    case async_madd(Client, KVECs) of
        {error, Reason} ->
            {error, Reason};
        {ok, ok} ->
            {Client, ok}
    end;
transaction(Client, async_mget, [Keys]) ->
    case async_mget(Client, Keys) of
        {error, Reason} ->
            {error, Reason};
        {ok, ok} ->
            {Client, ok}
    end;
transaction(Client, async_increment, [Keys]) ->
    case async_increment(Client, Keys) of
        {error, Reason} ->
            {error, Reason};
        {ok, ok} ->
            {Client, ok}
    end;
transaction(Client, async_delete, [Keys]) ->
    case async_delete(Client, Keys) of
        {error, Reason} ->
            {error, Reason};
        {ok, ok} ->
            {Client, ok}
    end;
transaction(Client, async_mget_response, [Keys, Timeout]) ->
    case async_mget_response(Client, Keys, Timeout) of
        {error, Reason} ->
            {error, Reason};
        {ok, Results} ->
            {Client, Results}
    end;
transaction(Client, async_blank_response, [Keys, Timeout]) ->
    {ok, Results} = async_blank_response(Client, Keys, Timeout),
    {Client, Results};
transaction(Client, async_mset_response, [Items, Timeout]) ->
    case async_mset_response(Client, Items, Timeout) of
        {error, Reason} ->
            {error, Reason};
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
        {ok, receive_response(Client, Op, <<>>, TimeLimit)}
    catch
        {failed, Reason} ->
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
    pack({?MEMCACHE_ADD, {0, Key, Value, ExpTime, undefined}});
pack({?MEMCACHE_SET, {Key, Value, ExpTime, CAS}}) ->
    pack({?MEMCACHE_SET, {0, Key, Value, ExpTime, CAS}});
pack({?MEMCACHE_GET, {[Key]}}) ->
    pack(<<>>, ?MEMCACHE_GET, Key);
pack({Op, Key}) when Op == ?MEMCACHE_GETKQ; Op == ?MEMCACHE_GETK ->
    pack(<<>>, Op, Key);
pack({Op, {N, Key, Value, ExpTime, CAS}})
    when Op == ?MEMCACHE_SETQ;
         Op == ?MEMCACHE_SET;
         Op == ?MEMCACHE_ADDQ;
         Op == ?MEMCACHE_ADD ->
    IntExpTime = value_to_integer(ExpTime),
    pack(<<16#DEADBEEF:32, IntExpTime:32>>, Op, Key, Value, CAS, N);
pack({?MEMCACHE_FLUSH_ALL, {}}) ->
    %% Flush inmediately by default
    ExpirationTime = 16#00,
    pack(<<ExpirationTime:32>>, ?MEMCACHE_FLUSH_ALL, <<>>).

pack(Extras, Operator, Key) ->
    pack(Extras, Operator, Key, <<>>).

pack(Extras, Operator, Key, Value) ->
    pack(Extras, Operator, Key, Value, undefined).

pack(Extras, Operator, Key, Value, CAS) ->
    pack(Extras, Operator, Key, Value, CAS, 0).

pack(Extras, Operator, Key, Value, CAS, Index) when is_integer(Index), Index >= 0 ->
    KeySize = size(Key),
    ExtrasSize = size(Extras),
    Body = <<Extras:ExtrasSize/binary, Key/binary, Value/binary>>,
    BodySize = size(Body),
    CASValue =
        case CAS of
            undefined ->
                16#00;
            CAS when is_integer(CAS) ->
                CAS
        end,
    <<16#80:8,      % magic (0)
      Operator:8,   % opcode (1)
      KeySize:16,   % key length (2,3)
      ExtrasSize:8, % extra length (4)
      16#00:8,      % data type (5)
      16#00:16,     % reserved (6,7)
      BodySize:32,  % total body (8-11)
      Index:32,     % opaque (12-15)
      CASValue:64,  % CAS (16-23)
      Body:BodySize/binary>>.

send(Client, Data) ->
    case gen_tcp:send(Client#client.socket, Data) of
        ok ->
            ok;
        {error, Reason} ->
            ?LOG_EVENT(Client#client.event_callback, [memcached_send_error, {reason, Reason}]),
            throw({failed, {send, Reason}})
    end.

cas_value(16#00) ->
    undefined;
cas_value(Value) when is_integer(Value) andalso Value > 0 ->
    Value.

receive_response(Client, Op, Buffer, TimeLimit) ->
    case recv_bytes(Client, 24, Buffer, TimeLimit) of
        {<<16#81:8,       % magic (0)
           Op:8,          % opcode (1)
           KeySize:16,    % key length (2,3)
           ExtrasSize:8,  % extra length (4)
           _DT:8,         % data type (5)
           StatusCode:16, % status (6,7)
           BodySize:32,   % total body (8-11)
           _Opq:32,       % opaque (12-15)
           CAS:64>>,      % CAS (16-23)
         Rest} ->
            case recv_bytes(Client, BodySize, Rest, TimeLimit) of
                {<<_Extras:ExtrasSize/binary, Key:KeySize/binary, Value/binary>>, <<>>} ->
                    case response_status(StatusCode) of
                        ok ->
                            #mero_item{key = Key, value = Value, cas = cas_value(CAS)};
                        {error, not_found} ->
                            #mero_item{key = Key, cas = cas_value(CAS)};
                        Error ->
                            Error
                    end;
                Data ->
                    throw({failed, {unexpected_body, Data}})
            end;
        Data ->
            throw({failed, {unexpected_header, Data, {expected, Op}}})
    end.

recv_bytes(Client, NumBytes, Buffer, TimeLimit) ->
    case Buffer of
        <<Data:NumBytes/binary, Rest/binary>> ->
            {Data, Rest};
        _ ->
            Timeout = mero_conf:millis_to(TimeLimit),
            case gen_tcp_recv(Client#client.socket, Timeout) of
                {ok, Bin} ->
                    recv_bytes(Client, NumBytes, <<Buffer/binary, Bin/binary>>, TimeLimit);
                {error, Reason} ->
                    ?LOG_EVENT(Client#client.event_callback,
                               [memcached_receive_error, {reason, Reason}]),
                    throw({failed, {receive_bytes, Reason}})
            end
    end.

value_to_integer(Value) ->
    binary_to_integer(binary:replace(Value, <<0>>, <<>>)).

to_integer(Binary) when is_binary(Binary) ->
    Size = bit_size(Binary),
    <<Value:Size/integer>> = Binary,
    Value.

gen_tcp_recv(Socket, Timeout) ->
    gen_tcp:recv(Socket, 0, Timeout).

async_mset(Client, NKVECs) ->
    try
        {ok, send_mset(Client, NKVECs)}
    catch
        {failed, Reason} ->
            {error, Reason}
    end.

async_madd(Client, NKVECs) ->
    try
        {ok, send_madd(Client, NKVECs)}
    catch
        {failed, Reason} ->
            {error, Reason}
    end.

async_mget(Client, Keys) ->
    try
        {ok, send_gets(Client, Keys)}
    catch
        {failed, Reason} ->
            {error, Reason}
    end.

async_delete(Client, Keys) ->
    try
        {ok,
         lists:foldl(fun (K, ok) ->
                             send(Client, pack({?MEMCACHE_DELETEQ, {K}}))
                     end,
                     ok,
                     Keys)}
    catch
        {failed, Reason} ->
            {error, Reason}
    end.

async_increment(Client, Keys) ->
    try
        {ok,
         lists:foreach(fun ({K, Value, Initial, ExpTime}) ->
                               send(Client,
                                    pack({?MEMCACHE_INCREMENTQ, {K, Value, Initial, ExpTime}}))
                       end,
                       Keys)}
    catch
        {failed, Reason} ->
            {error, Reason}
    end.

multipack([Item], _QuietOp, NoisyOp) ->
    [pack({NoisyOp, Item})];
multipack([Item | Rest], QuietOp, NoisyOp) ->
    [pack({QuietOp, Item}) | multipack(Rest, QuietOp, NoisyOp)].

send_quietly_butlast(Client, Items, QuietOp, NoisyOp) ->
    ok = send(Client, multipack(Items, QuietOp, NoisyOp)).

send_mset(Client, Items) ->
    send_quietly_butlast(Client, Items, ?MEMCACHE_SETQ, ?MEMCACHE_SET).

send_madd(Client, Items) ->
    send_quietly_butlast(Client, Items, ?MEMCACHE_ADDQ, ?MEMCACHE_ADD).

send_gets(Client, Keys) ->
    send_quietly_butlast(Client, Keys, ?MEMCACHE_GETKQ, ?MEMCACHE_GETK).

async_blank_response(_Client, _Keys, _TimeLimit) ->
    {ok, [ok]}.

async_mget_response(Client, Keys, TimeLimit) ->
    try
        {ok, receive_mget_response(Client, TimeLimit, Keys, <<>>, [])}
    catch
        {failed, Reason} ->
            {error, Reason}
    end.

receive_mget_response(Client, TimeLimit, Keys, Buffer, Acc) ->
    case recv_bytes(Client, 24, Buffer, TimeLimit) of
        {<<16#81:8,      % magic (0)
           Op:8,         % opcode (1)
           KeySize:16,   % key length (2,3)
           ExtrasSize:8, % extra length (4)
           _DT:8,        % data type (5)
           Status:16,    % status (6,7)
           BodySize:32,  % total body (8-11)
           _Opq:32,      % opaque (12-15)
           CAS:64>>,        % CAS (16-23)
         BufferRest} =
            Data ->
            case recv_bytes(Client, BodySize, BufferRest, TimeLimit) of
                {<<_Extras:ExtrasSize/binary, Key:KeySize/binary, ValueReceived/binary>>,
                 BufferRest2} ->
                    {Key, Value} = filter_by_status(Status, Op, Key, ValueReceived),
                    Responses = [#mero_item{key = Key, value = Value, cas = cas_value(CAS)} | Acc],
                    NKeys = lists:delete(Key, Keys),
                    case Op of
                        %% On silent we expect more values
                        ?MEMCACHE_GETKQ ->
                            receive_mget_response(Client, TimeLimit, NKeys, BufferRest2, Responses);
                        %% This was the last one!. Ensure there is no further data
                        ?MEMCACHE_GETK when BufferRest2 == <<>> ->
                            Responses ++ [#mero_item{key = KeyIn} || KeyIn <- NKeys]
                    end;
                Data ->
                    throw({failed, {unexpected_body, Data}})
            end;
        Data ->
            throw({failed, {unexpected_header, Data}})
    end.

filter_by_status(?NO_ERROR, _Op, Key, ValueReceived) ->
    {Key, ValueReceived};
filter_by_status(?NOT_FOUND, _Op, Key, _ValueReceived) ->
    {Key, undefined};
filter_by_status(Status, _Op, _Key, _ValueReceived) ->
    throw({failed, {response_status, Status}}).

async_mset_response(Client, NKVECs, TimeLimit) ->
    try
        {ok, receive_mset_response(Client, TimeLimit, NKVECs, <<>>, [])}
    catch
        {failed, Reason} ->
            {error, Reason}
    end.

receive_mset_response(Client, TimeLimit, NKVECs, Buffer, Acc) ->
    %% when sending the original requests, we had numbered each with a 1-based index
    %% according to its position in the original unsharded list of items, using the opaque
    %% field (which we get back with each response).  we used quiet mode for all but the
    %% last request, so we'll get at a minimum a single response, plus a response for each
    %% of the other requests which failed.
    %%
    %% NKVECs is of the form:
    %%
    %%     {N, Key, ...}
    %%
    %% where N was set as opaque data in the request associated with Key.
    %%
    case recv_bytes(Client, 24, Buffer, TimeLimit) of
        {<<16#81:8,       % magic (0)
           Op:8,          % opcode (1)
           KeySize:16,    % key length (2,3)
           ExtrasSize:8,  % extra length (4)
           _DT:8,         % data type (5)
           StatusCode:16, % status (6,7)
           BodySize:32,   % total body (8-11)
           Index:32,      % opaque (12-15)
           _CAS:64>>,     % CAS (16-23)
         Rest} ->
            case recv_bytes(Client, BodySize, Rest, TimeLimit) of
                {<<_Extras:ExtrasSize/binary, _Key:KeySize/binary, _Value/binary>>, Rest2} ->
                    %% the response to set/add/replace should have no extras, no key, and
                    %% no value, but may have a body if an error occurred.
                    NAcc = [{Index, response_status(StatusCode)} | Acc],
                    NItems = lists:keydelete(Index, 1, NKVECs),
                    case Op of
                        Op when Op == ?MEMCACHE_SETQ; Op == ?MEMCACHE_ADDQ ->
                            %% we are receiving a response for a 'quiet' command, meaning the
                            %% associated request failed.
                            receive_mset_response(Client, TimeLimit, NItems, Rest2, NAcc);
                        Op
                            when Rest2 == <<>>, Op == ?MEMCACHE_SET;
                                 Rest2 == <<>>, Op == ?MEMCACHE_ADD ->
                            %% last response.  any other request which had no response was
                            %% successful.
                            NAcc ++ [{element(1, Item), ok} || Item <- NItems]
                    end;
                Data ->
                    throw({failed, {unexpected_body, Data}})
            end;
        Data ->
            throw({failed, {unexpected_header, Data}})
    end.

response_status(?NO_ERROR) ->
    ok;
response_status(?NOT_FOUND) ->
    {error, not_found};
response_status(?KEY_EXISTS) ->
    {error, already_exists};
response_status(?VALUE_TOO_LARGE) ->
    {error, value_too_large};
response_status(?INVALID_ARGUMENTS) ->
    {error, invalid_arguments};
response_status(?NOT_STORED) ->
    {error, not_stored};
response_status(?NON_NUMERIC_INCR) ->
    {error, incr_decr_on_non_numeric_value};
response_status(?UNKNOWN_COMMAND) ->
    {error, unknown_command};
response_status(?OOM) ->
    {error, out_of_memory};
response_status(StatusCode) ->
    throw({failed, {response_status, StatusCode}}).
