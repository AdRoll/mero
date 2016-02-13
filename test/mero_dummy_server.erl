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
-module(mero_dummy_server).

-include_lib("mero/include/mero.hrl").

-author('Miriam Pena <miriam.pena@adroll.com>').

-behaviour(gen_server).

%%% Macros
-export([reset_all_keys/0,
         start_link/1,
         stop/1,
         reset/1,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         code_change/3,
         terminate/2]).

-export([accept/4]).

-define(TCP_SEND_TIMEOUT, 15000).
-define(FULLSWEEP_AFTER_OPT, {fullsweep_after, 10}).
-define(OP_Increment, 16#05).
-define(OP_Get, 16#00).

-define(ETS, ?MODULE).

-record(state, {listen_socket,
                num_acceptors,
                opts
               }).

%%%-----------------------------------------------------------------------------
%%% START/STOP EXPORTS
%%%-----------------------------------------------------------------------------
reset_all_keys() ->
    application:set_env(mero, dummy_server_keys, []).

name(Port) ->
    list_to_atom(lists:flatten(io_lib:format("~p_~p", [?MODULE, Port]))).

start_link(Port) ->
    gen_server:start_link({local, name(Port)}, ?MODULE, [Port, []], []).

stop(Pid) when is_pid(Pid) ->
    MRef = erlang:monitor(process, Pid),
    gen_server:call(Pid, stop),
    receive
        {'DOWN', MRef, _, _, _} ->
            io:format("server stopped ~p~n", [whereis(?MODULE)]),
            ok
    end;
stop(Port) when is_integer(Port) ->
    Name = name(Port),
    Pid = whereis(Name),
    stop(Pid).


reset(Port) ->
    gen_server:call(name(Port), reset).


handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.


handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, State) ->
    gen_tcp:close(State#state.listen_socket).


code_change(_, _, State) ->
    {ok, State}.


%%%-----------------------------------------------------------------------------
%%% INTERNAL EXPORTS
%%%-----------------------------------------------------------------------------

init([Port, Opts]) ->
    process_flag(trap_exit, true),
    case listen(Port, Opts) of
        {ok, ListenSocket} ->
            io:format("memcached mocked server started on port ~p~n", [Port]),
            start_acceptor([self(), Port, ListenSocket, Opts]),
            {ok, #state{listen_socket = ListenSocket,
                        opts = Opts}};
        {error, Reason} ->
            io:format("memcached dummy server error: ~p~n", [Reason]),
            {stop,Reason}
    end.

start_acceptor(Args) ->
    proc_lib:spawn_opt(?MODULE, accept, Args, [?FULLSWEEP_AFTER_OPT]).

listen(Port, SockOpts) ->
    gen_tcp:listen(Port, [binary,
                          {packet, 0},
                          {active, false},
                          {reuseaddr, true},
                          {nodelay, true},
                          {send_timeout, ?TCP_SEND_TIMEOUT},
                          {send_timeout_close, true},
                          {keepalive, true} |
                          SockOpts]).

accept(Parent, Port, ListenSocket, Opts) ->
    try
        link(Parent)
    catch
        error:noproc -> exit(normal)
    end,
    put('$ancestors', tl(get('$ancestors'))),
    start_accept(Parent, Port, ListenSocket, Opts).


start_accept(Parent, Port, ListenSocket, Opts) ->
    case gen_tcp:accept(ListenSocket) of
        {ok, Socket} ->
            unlink(Parent),
            start_acceptor([Parent, Port, ListenSocket, Opts]),
            loop(Socket, Port, Opts);
        {error, closed} ->
            unlink(Parent),
            exit(normal);
        {error, _Reason} ->
            start_accept(Parent, Port, ListenSocket, Opts)
    end.



loop(Sock, Port, Opts) ->
    loop(Sock, Port, Opts, <<>>).

loop(Sock, Port, Opts, Buf) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, Data} ->
            handle_data(Sock, Port, <<Buf/binary, Data/binary>>),
            loop(Sock, Port, Opts, Buf);
        {error, _Reason} = Error ->
            Error
    end.

%%%-----------------------------------------------------------------------------
%%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------

handle_data(Sock, Port, Data) ->
    Response = response(Port, Data),
    send(Sock, iolist_to_binary(Response)),
    ok.

%% We send one byte at a time to test that we are handling package split correctly
send(_Sock, <<>>) -> ok;
send(Sock, <<Byte:1/binary, Rest/binary>>) ->
    gen_tcp:send(Sock, Byte),
    timer:sleep(1),
    send(Sock, Rest).

get_current_keys() ->
    application:get_env(mero, dummy_server_keys, []).

get_key(Port, Key) ->
    proplists:get_value({Port, Key}, get_current_keys(), undefined).

put_key(Port, Key, Value) ->
    NList = lists:keystore({Port, Key}, 1, get_current_keys(), {{Port, Key}, Value}),
    ct:log("Current Keys: ~p~n", [get_current_keys()]),
    application:set_env(mero, dummy_server_keys, NList).

parse(<<16#80:8, _Rest/binary>> = Request) ->
    Resp = parse_binary(Request),
    {binary, Resp};
parse(Request) ->
    Resp = parse_text(Request),
    {text, Resp}.

%%%===================================================================
%%% Text Protocol
%%%===================================================================

%%%% Response

canned_responses(text, _Key, _Op, not_found)  -> ["NOT_FOUND", <<"\r\n">>];
canned_responses(text, _Key, _Op, not_stored) -> ["NOT_STORED", <<"\r\n">>];
canned_responses(text, _Key, _Op, stored)     -> [<<"STORED">>, <<"\r\n">>];
canned_responses(text, _Key, _Op, deleted)    -> [<<"DELETED">>, <<"\r\n">>];
canned_responses(text, _Key, _Op, {incr, I})  -> [mero_util:to_bin(I), <<"\r\n">>];

canned_responses(binary, _Key, Op, not_found) ->
    ExtrasOut = <<>>,
    ExtrasSizeOut = size(ExtrasOut),
    Status = 1,
    BodyOut = <<>>,
    BodySizeOut = size(BodyOut),
    KeySize = 0,

    <<16#81:8, Op:8, KeySize:16, ExtrasSizeOut:8, 0, Status:16,
      BodySizeOut:32, 0:32, 0:64, BodyOut/binary>>;

canned_responses(binary, _Key, Op, not_stored) ->
    ExtrasOut = <<>>,
    ExtrasSizeOut = size(ExtrasOut),
    Status = 5,
    BodyOut = <<>>,
    BodySizeOut = size(BodyOut),
    KeySize = 0,

    <<16#81:8, Op:8, KeySize:16, ExtrasSizeOut:8, 0, Status:16,
      BodySizeOut:32, 0:32, 0:64, BodyOut/binary>>;

canned_responses(binary, _Key, Op, stored) ->
    ExtrasOut = <<>>,
    ExtrasSizeOut = size(ExtrasOut),
    Status = 0,
    BodyOut = <<>>,
    BodySizeOut = size(BodyOut),
    KeySize = 0,

    <<16#81:8, Op:8, KeySize:16, ExtrasSizeOut:8, 0, Status:16,
      BodySizeOut:32, 0:32, 0:64, BodyOut/binary>>;

canned_responses(binary, _Key, Op, deleted) -> %% same as stored, intentionally
    ExtrasOut = <<>>,
    ExtrasSizeOut = size(ExtrasOut),
    Status = 0,
    BodyOut = <<>>,
    BodySizeOut = size(BodyOut),
    KeySize = 0,

    <<16#81:8, Op:8, KeySize:16, ExtrasSizeOut:8, 0, Status:16,
      BodySizeOut:32, 0:32, 0:64, BodyOut/binary>>;

canned_responses(binary, _Key, ?MEMCACHE_INCREMENT, {incr, I}) ->
    ExtrasOut = <<>>,
    ExtrasSizeOut = size(ExtrasOut),
    Status = 0,
    BodyOut = <<ExtrasOut/binary, I:64/integer>>,
    BodySizeOut = size(BodyOut),
    KeySize = 0,

    <<16#81:8, ?MEMCACHE_INCREMENT:8, KeySize:16, ExtrasSizeOut:8, 0, Status:16,
      BodySizeOut:32, 0:32, 0:64, BodyOut/binary>>.

text_response_get_keys(_Port, [], Acc) ->
    [Acc,  "END\r\n"];
text_response_get_keys(Port, [Key | Keys], Acc) ->
    case get_key(Port, Key) of
        undefined ->
            text_response_get_keys(Port, Keys, Acc);
        Value ->
            LValue = mero_util:to_bin(Value),
            NBytes = size(LValue),
            text_response_get_keys(Port, Keys, [Acc, "VALUE", " ", mero_util:to_bin(Key), " 00 ",
                                                mero_util:to_bin(NBytes), "\r\n", mero_util:to_bin(LValue),"\r\n"])
    end.

%% NOTE: This is not correct. Right now we don't distinguish between multiple
%% kinds of GETs, quiet and not. We must.
binary_response_get_keys(_Port, [], Acc) ->
    Acc;
binary_response_get_keys(Port, [{Op, Key} | Keys], Acc) ->
    {Status, Value} =  case get_key(Port, Key) of
                           undefined -> {1, <<>>};
                           Val -> {0, Val}
                       end,
    LValue = mero_util:to_bin(Value),
    ExtrasOut = <<>>,
    ExtrasSizeOut = size(ExtrasOut),
    BodyOut = <<ExtrasOut/binary, Key/binary, LValue/binary>>,
    BodySizeOut = size(BodyOut),
    KeySize = size(Key),

    binary_response_get_keys(Port, Keys, [<<16#81:8, Op:8, KeySize:16, ExtrasSizeOut:8, 0,
                                            Status:16, BodySizeOut:32, 0:32, 0:64, BodyOut/binary>> | Acc]).

%% TODO add stored / not stored responses here

response(Port, Request) ->
    case parse(Request) of
        {Kind, {get, Keys}} ->
            case Kind of
                text ->
                    text_response_get_keys(Port, Keys, []);
                binary ->
                    binary_response_get_keys(Port, Keys, [])
            end;
        {Kind, {set, Key, Bytes}} ->
            put_key(Port, Key, Bytes),
            canned_responses(Kind, Key, ?MEMCACHE_SET, stored);
        {Kind, {delete, Key}} ->
            case get_key(Port, Key) of
                undefined ->
                    canned_responses(Kind, Key, ?MEMCACHE_DELETE, not_found);
                _Value ->
                    put_key(Port, Key, undefined),
                    canned_responses(Kind, Key, ?MEMCACHE_DELETE, deleted)
            end;
        {Kind, {add, Key, Bytes}} ->
            case get_key(Port, Key) of
                undefined ->
                    put_key(Port, Key, Bytes),
                    canned_responses(Kind, Key, ?MEMCACHE_ADD, stored);
                _Value ->
                    canned_responses(Kind, Key, ?MEMCACHE_ADD, not_stored)
            end;
        {Kind, {incr, Key, ExpTime, Initial, Bytes}} ->
            case get_key(Port, Key) of
                undefined ->
                    %% Return error
                    case ExpTime of
                        4294967295 -> %% 32 bits, all 1
                            canned_responses(Kind, Key, ?MEMCACHE_INCREMENT, not_found);
                        _ ->
                            put_key(Port, Key, Initial),
                            canned_responses(Kind, Key, ?MEMCACHE_INCREMENT, {incr, Initial})
                    end;
                Value ->
                    Result = mero_util:to_int(Value) + mero_util:to_int(Bytes),
                    put_key(Port, Key, Result),
                    canned_responses(Kind, Key, ?MEMCACHE_INCREMENT, {incr, Result})
            end
    end.


%%% Parse

%% Only supporting requests that come in its own tcp package
parse_text(Request) ->
    case split(Request) of
        [<<"get">> | Keys] -> {get, Keys};
        [<<"set">>, Key, _Flag, _ExpTime, _NBytes, Bytes] -> {set, Key, Bytes};
        [<<"add">>, Key, _Flag, _ExpTime, _NBytes, Bytes] -> {add, Key, Bytes};
        [<<"delete">>, Key] -> {delete, Key};
        [<<"incr">>, Key, Value] -> {incr, Key, 100, Value, Value}
    end.

split(Binary) ->
    binary:split(Binary, [<<"\r\n">>, <<" ">>], [global, trim]).

%%%===================================================================
%%% Binary Protocol
%%%===================================================================

%%% Parse

parse_binary(<<16#80:8, ?MEMCACHE_GET:8, _/binary>> = Bin) ->
    {get, parse_get([], Bin)};
parse_binary(<<16#80:8, ?MEMCACHE_GETQ:8, _/binary>> = Bin) ->
    {get, parse_get([], Bin)};
parse_binary(<<16#80:8, ?MEMCACHE_GETK:8, _/binary>> = Bin) ->
    {get, parse_get([], Bin)};
parse_binary(<<16#80:8, ?MEMCACHE_GETKQ:8, _/binary>> = Bin) ->
    {get, parse_get([], Bin)};
parse_binary(<<16#80:8, ?MEMCACHE_SET:8, KeySize:16,
               ExtrasSize:8, 16#00:8, 16#00:16,
               _BodySize:32, 16#00:32, 16#00:64,
               _Extras:ExtrasSize/binary,
               Key:KeySize/binary, Value/binary>>) ->
    {set, Key, Value};
parse_binary(<<16#80:8, ?MEMCACHE_ADD:8, KeySize:16,
               ExtrasSize:8, 16#00:8, 16#00:16,
               _BodySize:32, 16#00:32, 16#00:64,
               _Extras:ExtrasSize/binary,
               Key:KeySize/binary, Value/binary>>) ->
    {add, Key, Value};
parse_binary(<<16#80:8, ?MEMCACHE_DELETE:8, KeySize:16,
               ExtrasSize:8, 16#00:8, 16#00:16,
               _BodySize:32, 16#00:32, 16#00:64,
               _Extras:ExtrasSize/binary,
               Key:KeySize/binary>>) ->
    {delete, Key};
parse_binary(<<16#80:8, ?MEMCACHE_INCREMENT:8, KeySize:16,
               _ExtrasSize:8, 16#00:8, 16#00:16,
               _BodySize:32, 16#00:32, 16#00:64,
               Value:64, Initial:64, ExpTime:32,
               Key:KeySize/binary>>) ->
    {incr, Key, ExpTime, Initial, Value}.


parse_get(Acc, <<>>) ->
    Acc;
parse_get(Acc, <<16#80:8, ?MEMCACHE_GET:8, KeySize:16,
                 _ExtrasSize:8, 16#00:8, 16#00:16,
                 _BodySize:32, 16#00:32, 16#00:64,
                 Key:KeySize/binary, Rest/binary>>) ->
    parse_get([{?MEMCACHE_GET, Key} | Acc], Rest);
parse_get(Acc, <<16#80:8, ?MEMCACHE_GETQ:8, KeySize:16,
                 _ExtrasSize:8, 16#00:8, 16#00:16,
                 _BodySize:32, 16#00:32, 16#00:64,
                 Key:KeySize/binary, Rest/binary>>) ->
    parse_get([{?MEMCACHE_GETQ, Key} | Acc], Rest);
parse_get(Acc, <<16#80:8, ?MEMCACHE_GETK:8, KeySize:16,
                 _ExtrasSize:8, 16#00:8, 16#00:16,
                 _BodySize:32, 16#00:32, 16#00:64,
                 Key:KeySize/binary, Rest/binary>>) ->
    parse_get([{?MEMCACHE_GETK, Key} | Acc], Rest);
parse_get(Acc, <<16#80:8, ?MEMCACHE_GETKQ:8, KeySize:16,
                 _ExtrasSize:8, 16#00:8, 16#00:16,
                 _BodySize:32, 16#00:32, 16#00:64,
                 Key:KeySize/binary, Rest/binary>>) ->
    parse_get([{?MEMCACHE_GETKQ, Key} | Acc], Rest).
