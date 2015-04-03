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
    %io:format("~p~n", [[{request, Data}, {response, Response}]]).
    ok.

%% We send one byte at a time to test that we are handling package split correctly
send(_Sock, <<>>) -> ok;
send(Sock, <<Byte:1/binary, Rest/binary>>) ->
    %io:format("Send byte ~p",[Byte]),
    gen_tcp:send(Sock, Byte),
    timer:sleep(1),
    send(Sock, Rest).

%% Probably the worst code ever..
%% response(<<16#80:8, ?OP_Increment:8, KeySize:16,
%%            _ExtrasSize:8, 16#00:8, 16#00:16,
%%            _BodySize:32, 16#00:32, 16#00:64,
%%            Value:64, Initial:64, _ExpTime:32,
%%            Key/binary>>) ->
%%     Result = increment(Key, Initial, Value),
%%
%%     ValueOut = list_to_binary(integer_to_list(Result)),
%%     ExtrasOut = <<>>,
%%     ExtrasSizeOut = size(ExtrasOut),
%%     Status = 0,
%%     BodyOut = <<ExtrasOut/binary, Key/binary,ValueOut/binary>>,
%%     BodySizeOut = size(BodyOut),
%%
%%     <<16#81:8, ?OP_Increment:8, KeySize:16, ExtrasSizeOut:8, 0, Status:16,
%%       BodySizeOut:32, 0:32, 0:64, BodyOut/binary>>;
%%
%%
%%
%% response(<<16#80:8, ?OP_Get:8, KeySize:16,
%%            _ExtrasSize:8, 16#00:8, 16#00:16,
%%            _BodySize:32, 16#00:32, 16#00:64,
%%            Key/binary>>) ->
%%
%%     ExtrasOut = <<>>,
%%     ExtrasSizeOut = size(ExtrasOut),
%%
%%     {ValueOut, Status} =
%%         case get_counter(Key) of
%%             undefined -> {<<>>, 1}; % Status 1
%%             ValueOutt -> {list_to_binary(integer_to_list(ValueOutt)), 0}
%%         end,
%%     BodyOut = <<ExtrasOut/binary, Key/binary, ValueOut/binary>>,
%%     BodySizeOut = size(BodyOut),
%%
%%     <<16#81:8, ?OP_Get:8, KeySize:16, ExtrasSizeOut:8, 0, Status:16,
%%       BodySizeOut:32, 0:32, 0:64, BodyOut/binary>>;

response(Port, Request) ->
    case parse(Request) of
        {get, Keys} ->
            response_get_keys(Port, Keys, []);
        {set, Key, Bytes} ->
            %io:format("[~p] Put ~p ~p ~n", [self(), Key, Bytes]),
            lput(Port, Key, Bytes),
            [<<"STORED">>, <<"\r\n">>];
        {delete, Key} ->
            case lget(Port, Key) of
                undefined ->
                    [<<"NOT_FOUND">>, <<"\r\n">>];
                _Value ->
                    lput(Port, Key, undefined),
                    [<<"DELETED">>, <<"\r\n">>]
            end;
        {add, Key, Bytes} ->
            case lget(Port, Key) of
                undefined ->
                    lput(Port, Key, Bytes),
                    io:format("[~p] Add ~p ~p ~n", [self(), Key, Bytes]),
                    [<<"STORED">>, <<"\r\n">>];
                Value ->
                    io:format("[~p] Add already exists ~p not added to ~p ~p~n", [self(), Key, Value, lget(Port, Key)]),
                    [<<"NOT_STORED">>, <<"\r\n">>]
            end;
        {incr, Key, Bytes} ->
            case lget(Port, Key) of
                undefined ->
                    %% Return error
                    %io:format("[~p] Key not found ~p ~n",[self(), Key]),
                    ["NOT_FOUND", <<"\r\n">>];
                Value ->
                    Result = to_int(Value) + to_int(Bytes),
                    lput(Port, Key, Result),
                    %io:format("[~p] Incr ~p from ~p to ~p~n", [self(), Key, Value, Result]),
                    [to_bin(Result), <<"\r\n">>]
            end
    end.


response_get_keys(_Port, [], Acc) ->
    [Acc,  "END\r\n"];

response_get_keys(Port, [Key | Keys], Acc) ->
    case lget(Port, Key) of
        undefined ->
            io:format("[~p] Get ~p not found ~n",[self(), Key]),
            response_get_keys(Port, Keys, Acc);
        Value ->
            LValue = to_bin(Value),
            NBytes = size(LValue),
            io:format("[~p] Get ~p -~p- ~n", [self(), Key, LValue]),
            response_get_keys(Port, Keys, [Acc, "VALUE", " ", to_bin(Key), " 00 ", to_bin(NBytes),  "\r\n", to_bin(LValue),"\r\n"])
    end.

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

get_current_keys() ->
    Keys = application:get_env(mero, dummy_server_keys, []),
    io:format("Current Keys ~p", [Keys]),
    Keys.

get_key(Port, Key) ->
    proplists:get_value({Port, Key}, get_current_keys(), undefined).

put_key(Port, Key, Value) ->
    NList = lists:keystore({Port, Key}, 1, get_current_keys(), {{Port, Key}, Value}),
    application:set_env(mero, dummy_server_keys, NList).

lget(Port, Key) ->
    Result = get_key(Port, Key),
    io:format("~p GET ~p = ~p ~n", [self(), Key, Result]),
    Result.

lput(Port, Key, Value) ->
    Result = put_key(Port, Key, Value),
    NewValue = get_key(Port, Key),
    io:format("~p PUT ~p = ~p  ~p => ~p ~n", [self(), Key, Value, Result, NewValue]),
    Result.

%% Only supporting requests that come in its own tcp package
parse(Request) ->
    case split(Request) of
        [<<"get">> | Keys] -> {get, Keys};
        [<<"set">>, Key, _Flag, _ExpTime, _NBytes, Bytes] -> {set, Key, Bytes};
        [<<"add">>, Key, _Flag, _ExpTime, _NBytes, Bytes] -> {add, Key, Bytes};
        [<<"delete">>, Key] -> {delete, Key};
        [<<"incr">>, Key, Bytes] -> {incr, Key, Bytes}
    end.



split(Binary) ->
    binary:split(Binary, [<<"\r\n">>, <<" ">>], [global, trim]).
