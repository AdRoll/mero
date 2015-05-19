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


%%% Start/stop functions
-export([connect/3,
         controlling_process/2,
         transaction/3,
         close/1]).

-record(client, {socket, pool, event_callback}).

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

transaction(Client, delete, [Key,  TimeLimit]) ->
    case send_receive(Client, {?MEMCACHE_DELETE, {Key}}, TimeLimit) of
        {ok, deleted} ->
            {Client, ok};
        {error, not_found} ->
            {Client, {error, not_found}};
        {error, Reason} ->
            {error, Reason}
    end;

transaction(Client, add, [Key, Value, ExpTime, TimeLimit]) ->
    case send_receive(Client, {?MEMCACHE_ADD, {Key, Value, ExpTime}}, TimeLimit) of
        {ok, stored} ->
            {Client, ok};
        {error, not_stored} ->
            {Client, {error, not_stored}};
        {error, Reason} ->
            {error, Reason}
    end;

transaction(Client, set, [Key, Value, ExpTime, TimeLimit]) ->
    case send_receive(Client, {?MEMCACHE_SET, {Key, Value, ExpTime}}, TimeLimit) of
        {ok, stored} ->
            {Client, ok};
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

transaction(Client, get, [Keys, TimeLimit]) ->
    case send_receive(Client, {?MEMCACHE_GET, {Keys}}, TimeLimit) of
        {error, Reason} ->
            {error, Reason};
        {ok, FoundKeyValues} ->
            FoundKeys = [Key || {Key, _Value} <- FoundKeyValues],
            NotFoundKeys = lists:subtract(Keys, FoundKeys),
            Result = [{Key, undefined} || Key <- NotFoundKeys] ++ FoundKeyValues,
            {Client, Result}
    end.


close(Client) ->
    gen_tcp:close(Client#client.socket).


%%%=============================================================================
%%% Internal functions
%%%=============================================================================


send_receive(Client, {Op, _Args} = Cmd, TimeLimit) ->
    try
        Data = pack(Cmd),
        ok = send(Client, Data),
        receive_response(Client, Op, TimeLimit, <<>>, [])
    catch
        throw:{failed, Reason} ->
           {error, Reason}
    end.


pack({?MEMCACHE_DELETE, {Key}}) when is_binary(Key) ->
    [<<"delete ">>, Key, <<"\r\n">>];
pack({?MEMCACHE_ADD, {Key, Initial, ExpTime}}) ->
    NBytes = integer_to_list(size(Initial)),
    [<<"add ">>, Key, <<" ">>, <<"00">>, <<" ">>, ExpTime,
        <<" ">>, NBytes, <<"\r\n">>, Initial, <<"\r\n">>];
pack({?MEMCACHE_SET, {Key, Initial, ExpTime}}) ->
    NBytes = integer_to_list(size(Initial)),
    [<<"set ">>, Key, <<" ">>, <<"00">>, <<" ">>, ExpTime,
     <<" ">>, NBytes, <<"\r\n">>, Initial, <<"\r\n">>];
pack({?MEMCACHE_INCREMENT, {Key, Value}}) ->
    [<<"incr ">>, Key, <<" ">>, Value, <<"\r\n">>];
pack({?MEMCACHE_FLUSH_ALL, {}}) ->
    [<<"flush_all\r\n">>];
pack({?MEMCACHE_GET, {Keys}}) when is_list(Keys) ->
    Query = lists:foldr(fun(Key, Acc) -> [Key, <<" ">> | Acc] end, [], Keys),
    [<<"get ">>, Query, <<"\r\n">>].



send(Client, Data) ->
    case gen_tcp:send(Client#client.socket, Data) of
        ok -> ok;
        {error, Reason} ->
            ?LOG_EVENT(Client#client.event_callback, [socket, send, error, {reason, Reason}]),
           throw({failed, Reason})
    end.


receive_response(Client, Op, TimeLimit, AccBinary, AccResult) ->
    case gen_tcp_recv(Client, 0, TimeLimit) of
        {ok, Data} ->
            NAcc = <<AccBinary/binary, Data/binary>>,
            case parse_reply(NAcc, AccResult) of
                {ok, Atom, []} when (Atom == stored) or
                                    (Atom == deleted) or
                                    (Atom == ok) ->
                    {ok, Atom};
                {ok, Binary} when is_binary(Binary) ->
                    {ok, Binary};
                {ok, finished, Result} ->
                    {ok, Result};
                {ok, NBuffer, NAccResult} ->
                    receive_response(Client, Op, TimeLimit, NBuffer, NAccResult);
                {error, Reason} ->
                    ?LOG_EVENT(Client#client.event_callback, [socket, rcv, error, {reason, Reason}]),
                    throw({failed, Reason})
            end;
        {error, Reason} ->
            ?LOG_EVENT(Client#client.event_callback, [socket, rcv, error, {reason, Reason}]),
            throw({failed, Reason})
    end.

%% This is so extremely shitty that I will do the binary prototol no matter what :(
parse_reply(Buffer, AccResult) ->
    case split_command(Buffer) of
        {error, uncomplete} ->
            {ok, Buffer, AccResult};
        {Command, BinaryRest} ->
            case parse_command(Command) of
                {ok, Status} when is_atom(Status) ->
                    {ok, Status, AccResult};
                {ok, {value, Key, Bytes}} ->
                    case split_command(BinaryRest) of
                        {[Value], BinaryRest2} when (size(Value) == Bytes) ->
                            parse_reply(BinaryRest2, [{Key, Value} | AccResult]);
                        {error, uncomplete} ->
                            {ok, Buffer, AccResult}
                    end;
                {ok, Binary} when is_binary(Binary) ->
                    {ok, Binary};
                {error, Reason} ->
                    {error, Reason}
            end
    end.


split_command(Buffer) ->
    case binary:split(Buffer,  [<<"\r\n">>], []) of
        [Line, RemainBuffer] ->
            {binary:split(Line,  [<<" ">>], [global, trim]), RemainBuffer};
        [_UncompletedLine] ->
            {error, uncomplete}
    end.


parse_command([<<"ERROR">> | Reason] ) ->
    {error, Reason};
parse_command([<<"CLIENT_ERROR">> | Reason]) ->
    {error, Reason};
parse_command([<<"SERVER_ERROR">> | Reason]) ->
    {error, Reason};

parse_command([<<"EXISTS">>]) ->
    {error, exists};
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
    {ok, {value, Key, list_to_integer(binary_to_list(Bytes))}};
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
