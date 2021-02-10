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
-module(mero_wrk_tcp_txt).

-author('Miriam Pena <miriam.pena@adroll.com>').

-include_lib("mero/include/mero.hrl").

-behavior(mero_pool).

%%% Start/stop functions
-export([connect/3, controlling_process/2, transaction/3, close/2]).

-record(client, {socket, event_callback}).

-define(SOCKET_OPTIONS,
        [binary, {packet, raw}, {active, false}, {reuseaddr, true}, {nodelay, true}]).

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

transaction(Client, increment_counter, [Key, Value, Initial, ExpTime, TimeLimit]) ->
    %% First attempt
    case send_receive(Client, {?MEMCACHE_INCREMENT, {Key, Value}}, TimeLimit) of
        {error, not_found} ->
            %% Key does not exist, create the key
            case send_receive(Client, {?MEMCACHE_ADD, {Key, Initial, ExpTime}}, TimeLimit) of
                {ok, stored} ->
                    {Client, {ok, to_integer(Initial)}};
                {error, not_stored} ->
                    %% Key was already created by other thread,
                    %% Second attempt (just in case of someone added right at that time)
                    case send_receive(Client, {?MEMCACHE_INCREMENT, {Key, Value}}, TimeLimit) of
                        {error, Reason} ->
                            {error, Reason};
                        {ok, NValue} ->
                            {Client, {ok, to_integer(NValue)}}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason};
        {ok, NValue} ->
            {Client, {ok, to_integer(NValue)}}
    end;
transaction(Client, delete, [Key, TimeLimit]) ->
    case send_receive(Client, {?MEMCACHE_DELETE, {Key}}, TimeLimit) of
        {ok, deleted} ->
            {Client, ok};
        {error, not_found} ->
            {Client, {error, not_found}};
        {error, Reason} ->
            {error, Reason}
    end;
transaction(Client, mdelete, [Keys, TimeLimit]) ->
    Resp =
        mero_util:foreach(fun(Key) ->
                             case send_receive(Client, {?MEMCACHE_DELETE, {Key}}, TimeLimit) of
                                 {ok, deleted} ->
                                     continue;
                                 {error, not_found} ->
                                     continue;
                                 {error, Reason} ->
                                     {break, {error, Reason}}
                             end
                          end,
                          Keys),
    {Client, Resp};
transaction(Client, add, [Key, Value, ExpTime, TimeLimit]) ->
    case send_receive(Client, {?MEMCACHE_ADD, {Key, Value, ExpTime}}, TimeLimit) of
        {ok, stored} ->
            {Client, ok};
        {error, not_stored} ->
            {Client, {error, not_stored}};
        {error, Reason} ->
            {error, Reason}
    end;
transaction(Client, get, [Key, TimeLimit]) ->
    case send_receive(Client, {?MEMCACHE_GET, {[Key]}}, TimeLimit) of
        {ok, [Found]} ->
            {Client, Found};
        {error, Reason} ->
            {error, Reason}
    end;
transaction(Client, set, [Key, Value, ExpTime, TimeLimit, CAS]) ->
    case send_receive(Client, {?MEMCACHE_SET, {Key, Value, ExpTime, CAS}}, TimeLimit) of
        {ok, stored} ->
            {Client, ok};
        {error, already_exists} ->
            {Client, {error, already_exists}};
        {error, not_found} ->
            {Client, {error, not_found}};
        {error, Reason} ->
            {error, Reason}
    end;
transaction(Client, flush_all, [TimeLimit]) ->
    case send_receive(Client, {?MEMCACHE_FLUSH_ALL, {}}, TimeLimit) of
        {ok, ok} ->
            {Client, ok};
        {error, Reason} ->
            {error, Reason}
    end;
%% mset/madd are currently only supported by the binary protocol implementation.
transaction(Client, async_mset, _) ->
    {Client, {error, unsupported_operation}};
transaction(Client, async_madd, _) ->
    {Client, {error, unsupported_operation}};
transaction(Client, async_mget, [Keys]) ->
    case async_mget(Client, Keys) of
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
transaction(Client, async_increment, [Keys]) ->
    async_increment(Client, Keys);
transaction(Client, async_blank_response, [Keys, Timeout]) ->
    {ok, Results} = async_blank_response(Client, Keys, Timeout),
    {Client, Results};
transaction(Client, async_mget_response, [Keys, Timeout]) ->
    case async_mget_response(Client, Keys, Timeout) of
        {error, Reason} ->
            {error, Reason};
        {ok, {ok, FoundItems}} ->
            FoundKeys = [Key || #mero_item{key = Key} <- FoundItems],
            NotFoundKeys = lists:subtract(Keys, FoundKeys),
            Result =
                [#mero_item{key = Key, value = undefined} || Key <- NotFoundKeys] ++ FoundItems,
            {Client, Result}
    end.

close(Client, Reason) ->
    ?LOG_EVENT(Client#client.event_callback, [closing_socket, {reason, Reason}]),
    gen_tcp:close(Client#client.socket).

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

send_receive(Client, {_Op, _Args} = Cmd, TimeLimit) ->
    try
        Data = pack(Cmd),
        ok = send(Client, Data),
        receive_response(Client, Cmd, TimeLimit, <<>>, [])
    catch
        {failed, Reason} ->
            {error, Reason}
    end.

pack({?MEMCACHE_DELETE, {Key}}) when is_binary(Key) ->
    [<<"delete ">>, Key, <<"\r\n">>];
pack({?MEMCACHE_DELETEQ, {Key}}) when is_binary(Key) ->
    [<<"delete ">>, Key, <<" noreply ">>, <<"\r\n">>];
pack({?MEMCACHE_ADD, {Key, Initial, ExpTime}}) ->
    NBytes = integer_to_list(size(Initial)),
    [<<"add ">>,
     Key,
     <<" ">>,
     <<"00">>,
     <<" ">>,
     ExpTime,
     <<" ">>,
     NBytes,
     <<"\r\n">>,
     Initial,
     <<"\r\n">>];
pack({?MEMCACHE_SET, {Key, Initial, ExpTime, undefined}}) ->
    NBytes = integer_to_list(size(Initial)),
    [<<"set ">>,
     Key,
     <<" ">>,
     <<"00">>,
     <<" ">>,
     ExpTime,
     <<" ">>,
     NBytes,
     <<"\r\n">>,
     Initial,
     <<"\r\n">>];
pack({?MEMCACHE_SET, {Key, Initial, ExpTime, CAS}}) when is_integer(CAS) ->
    %% note: CAS should only be supplied if setting a value after looking it up.  if the
    %% value has changed since we looked it up, the result of a cas command will be EXISTS
    %% (otherwise STORED).
    NBytes = integer_to_list(size(Initial)),
    [<<"cas ">>,
     Key,
     <<" ">>,
     <<"00">>,
     <<" ">>,
     ExpTime,
     <<" ">>,
     NBytes,
     <<" ">>,
     integer_to_binary(CAS),
     <<"\r\n">>,
     Initial,
     <<"\r\n">>];
pack({?MEMCACHE_INCREMENT, {Key, Value}}) ->
    [<<"incr ">>, Key, <<" ">>, Value, <<"\r\n">>];
pack({?MEMCACHE_FLUSH_ALL, {}}) ->
    [<<"flush_all\r\n">>];
pack({?MEMCACHE_GET, {Keys}}) when is_list(Keys) ->
    Query = lists:foldr(fun(Key, Acc) -> [Key, <<" ">> | Acc] end, [], Keys),
    [<<"gets ">>, Query, <<"\r\n">>].

send(Client, Data) ->
    case gen_tcp:send(Client#client.socket, Data) of
        ok ->
            ok;
        {error, Reason} ->
            ?LOG_EVENT(Client#client.event_callback, [socket, send, error, {reason, Reason}]),
            throw({failed, Reason})
    end.

receive_response(Client, Cmd, TimeLimit, AccBinary, AccResult) ->
    case gen_tcp_recv(Client, 0, TimeLimit) of
        {ok, Data} ->
            NAcc = <<AccBinary/binary, Data/binary>>,
            case parse_reply(Client, Cmd, NAcc, TimeLimit, AccResult) of
                {ok, Atom, []} when (Atom == stored) or (Atom == deleted) or (Atom == ok) ->
                    {ok, Atom};
                {ok, Binary} when is_binary(Binary) ->
                    {ok, Binary};
                {ok, finished, Result} ->
                    {ok, Result};
                {ok, NBuffer, NewCmd, NAccResult} ->
                    receive_response(Client, NewCmd, TimeLimit, NBuffer, NAccResult);
                {error, Reason} ->
                    ?LOG_EVENT(Client#client.event_callback,
                               [socket, rcv, error, {reason, Reason}]),
                    throw({failed, Reason})
            end;
        {error, Reason} ->
            ?LOG_EVENT(Client#client.event_callback, [socket, rcv, error, {reason, Reason}]),
            throw({failed, Reason})
    end.

process_result({?MEMCACHE_GET, {Keys}}, finished, Result) ->
    Result ++ [#mero_item{key = Key} || Key <- Keys];
process_result(_Cmd, _Status, Result) ->
    Result.

%% This is so extremely shitty that I will do the binary prototol no matter what :(
parse_reply(Client, Cmd, Buffer, TimeLimit, AccResult) ->
    case split_command(Buffer) of
        {error, uncomplete} ->
            {ok, Buffer, Cmd, AccResult};
        {Command, BinaryRest} ->
            case {Cmd, parse_command(Command)} of
                {_, {ok, Status}} when is_atom(Status) ->
                    {ok, Status, process_result(Cmd, Status, AccResult)};
                {{?MEMCACHE_GET, {Keys}}, {ok, {value, Key, Bytes, CAS}}} ->
                    case parse_value(Client, Key, Bytes, CAS, TimeLimit, BinaryRest) of
                        {ok, Item, NewBinaryRest} ->
                            parse_reply(Client,
                                        {?MEMCACHE_GET, {lists:delete(Key, Keys)}},
                                        NewBinaryRest,
                                        TimeLimit,
                                        [Item | AccResult]);
                        {error, Reason} ->
                            {error, Reason}
                    end;
                {_, {ok, Binary}} when is_binary(Binary) ->
                    {ok, Binary};
                {_, {error, Reason}} ->
                    {error, Reason}
            end
    end.

parse_value(Client, Key, Bytes, CAS, TimeLimit, Buffer) ->
    case Buffer of
        <<Value:Bytes/binary, "\r\n", RestAfterValue/binary>> ->
            {ok,
             #mero_item{key = Key,
                        value = Value,
                        cas = CAS},
             RestAfterValue};
        _ when size(Buffer) < Bytes + size(<<"\r\n">>) ->
            case gen_tcp_recv(Client, 0, TimeLimit) of
                {ok, Data} ->
                    parse_value(Client, Key, Bytes, CAS, TimeLimit, <<Buffer/binary, Data/binary>>);
                {error, Reason} ->
                    {error, Reason}
            end;
        _ ->
            {error, invalid_value_size}
    end.

split_command(Buffer) ->
    case binary:split(Buffer, [<<"\r\n">>], []) of
        [Line, RemainBuffer] ->
            {binary:split(Line, [<<" ">>], [global, trim]), RemainBuffer};
        [_UncompletedLine] ->
            {error, uncomplete}
    end.

parse_command([<<"ERROR">> | Reason]) ->
    {error, Reason};
parse_command([<<"CLIENT_ERROR">> | Reason]) ->
    {error, Reason};
parse_command([<<"SERVER_ERROR">> | Reason]) ->
    {error, Reason};
parse_command([<<"EXISTS">>]) ->
    {error, already_exists};
parse_command([<<"NOT_FOUND">>]) ->
    {error, not_found};
parse_command([<<"NOT_STORED">>]) ->
    {error, not_stored};
parse_command([<<"END">>]) ->
    {ok, finished};
parse_command([<<"STORED">>]) ->
    {ok, stored};
parse_command([<<"DELETED">>]) ->
    {ok, deleted};
parse_command([<<"VALUE">>, Key, _Flag, Bytes]) ->
    {ok, {value, Key, list_to_integer(binary_to_list(Bytes)), undefined}};
parse_command([<<"VALUE">>, Key, _Flag, Bytes, CAS]) ->
    {ok, {value, Key, list_to_integer(binary_to_list(Bytes)), binary_to_integer(CAS)}};
parse_command([<<"OK">>]) ->
    {ok, ok};
parse_command([<<"VERSION">> | Version]) ->
    {ok, Version};
parse_command([Value]) ->
    {ok, Value};
parse_command(Line) ->
    {error, {unknown, Line}}.

gen_tcp_recv(Client, Bytes, TimeLimit) ->
    Timeout = mero_conf:millis_to(TimeLimit),
    case Timeout of
        0 ->
            {error, timeout};
        Timeout ->
            gen_tcp:recv(Client#client.socket, Bytes, Timeout)
    end.

to_integer(Value) when is_integer(Value) ->
    Value;
to_integer(Value) when is_binary(Value) ->
    binary_to_integer(Value).

async_mget(Client, Keys) ->
    try
        Data = pack({?MEMCACHE_GET, {Keys}}),
        {ok, send(Client, Data)}
    catch
        {failed, Reason} ->
            {error, Reason}
    end.

async_delete(Client, Keys) ->
    try
        {ok, lists:foreach(fun(K) -> send(Client, pack({?MEMCACHE_DELETEQ, {K}})) end, Keys)}
    catch
        {failed, Reason} ->
            {error, Reason}
    end.

async_increment(_Client, _Keys) ->
    {error, not_supportable}. %% txt incr doesn't support initail etc

async_blank_response(_Client, _Keys, _TimeLimit) ->
    {ok, [ok]}.

async_mget_response(Client, Keys, TimeLimit) ->
    try
        {ok, receive_response(Client, {?MEMCACHE_GET, {Keys}}, TimeLimit, <<>>, [])}
    catch
        {failed, Reason} ->
            {error, Reason}
    end.
