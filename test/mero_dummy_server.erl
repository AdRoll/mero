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
-include_lib("eunit/include/eunit.hrl").

-author('Miriam Pena <miriam.pena@adroll.com>').

-behaviour(gen_server).

-export([start_link/1, stop/1, reset/1, init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2]).
-export([accept/3]).

-define(TCP_SEND_TIMEOUT, 15000).
-define(FULLSWEEP_AFTER_OPT, {fullsweep_after, 10}).

-record(state, {listen_socket, keys = []}).

-type state() :: #state{}.

%%%-----------------------------------------------------------------------------
%%% START/STOP EXPORTS
%%%-----------------------------------------------------------------------------
name(Port) ->
    list_to_atom(lists:flatten(
                     io_lib:format("~p_~p", [?MODULE, Port]))).

start_link(Port) ->
    gen_server:start_link({local, name(Port)}, ?MODULE, Port, []).

stop(Pid) when is_pid(Pid) ->
    MRef = erlang:monitor(process, Pid),
    gen_server:call(Pid, stop),
    receive
        {'DOWN', MRef, _, Object, Info} ->
            ct:pal("server ~p stopped ~p: ~p", [Object, whereis(?MODULE), Info]),
            ok
    end;
stop(Port) when is_integer(Port) ->
    Name = name(Port),
    Pid = whereis(Name),
    stop(Pid).

reset(Port) ->
    gen_server:call(name(Port), reset).

-spec handle_call(stop |
                  flush_all |
                  {get_key, pos_integer(), mero:mero_key()} |
                  {put_key,
                   pos_integer(),
                   mero:mero_key(),
                   undefined | term(),
                   string() | integer()},
                  _From,
                  state()) ->
                     {reply, ok | {ok, term()}, state()} | {stop, normal, ok, state()}.
handle_call({put_key, Port, Key, undefined, _}, _From, #state{keys = Keys} = State) ->
    ct:log("~p deleting key ~p", [Port, Key]),
    NKeys = lists:keydelete({Port, Key}, 1, Keys),
    ct:log("new keys: ~p", [NKeys]),
    {reply, ok, State#state{keys = NKeys}};
handle_call({put_key, Port, Key, Value, CAS}, _From, #state{keys = Keys} = State) ->
    ct:log("~p setting key ~p (cas: ~p)", [Port, Key, CAS]),
    ActualCAS =
        case CAS of
            undefined ->
                {Mega, Sec, Micro} = os:timestamp(),
                Mega + Sec + Micro;
            _ ->
                CAS
        end,
    NKeys = lists:keystore({Port, Key}, 1, Keys, {{Port, Key}, {Value, ActualCAS}}),
    ct:log("new keys: ~p", [NKeys]),
    {reply, ok, State#state{keys = NKeys}};
handle_call({get_key, Port, Key}, _From, #state{keys = Keys} = State) ->
    {reply, {ok, proplists:get_value({Port, Key}, Keys)}, State};
handle_call({flush_all, Port}, _From, #state{keys = Keys} = State) ->
    ct:log("~p flushing all keys", [Port]),
    NKeys =
        lists:filter(fun ({{KeyPort, _}, _}) when KeyPort == Port ->
                             false;
                         (_) ->
                             true
                     end,
                     Keys),
    ct:log("new keys: ~p", [NKeys]),
    {reply, ok, State#state{keys = NKeys}};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(term(), state()) -> _.
terminate(_Reason, State) ->
    gen_tcp:close(State#state.listen_socket).

%%%-----------------------------------------------------------------------------
%%% INTERNAL EXPORTS
%%%-----------------------------------------------------------------------------
-spec init(pos_integer()) -> {ok, state()} | {stop, term()}.
init(Port) ->
    process_flag(trap_exit, true),
    case listen(Port) of
        {ok, ListenSocket} ->
            ct:pal("memcached mocked server started on port ~p", [Port]),
            start_acceptor(self(), Port, ListenSocket),
            {ok, #state{listen_socket = ListenSocket}};
        {error, Reason} ->
            ct:pal("memcached dummy server error: ~p", [Reason]),
            {stop, Reason}
    end.

start_acceptor(Parent, Port, ListenSocket) ->
    proc_lib:spawn_opt(?MODULE, accept, [Parent, Port, ListenSocket], [?FULLSWEEP_AFTER_OPT]).

listen(Port) ->
    gen_tcp:listen(Port,
                   [binary,
                    {packet, 0},
                    {active, false},
                    {reuseaddr, true},
                    {nodelay, true},
                    {send_timeout, ?TCP_SEND_TIMEOUT},
                    {send_timeout_close, true},
                    {keepalive, true}]).

accept(Parent, Port, ListenSocket) ->
    try
        link(Parent)
    catch
        error:noproc ->
            exit(normal)
    end,
    put('$ancestors', tl(get('$ancestors'))),
    start_accept(Parent, Port, ListenSocket).

start_accept(Parent, Port, ListenSocket) ->
    case gen_tcp:accept(ListenSocket) of
        {ok, Socket} ->
            unlink(Parent),
            start_acceptor(Parent, Port, ListenSocket),
            loop(Parent, Socket, Port);
        {error, closed} ->
            unlink(Parent),
            exit(normal);
        {error, _Reason} ->
            start_accept(Parent, Port, ListenSocket)
    end.

loop(Parent, Sock, Port) ->
    loop(Parent, Sock, Port, <<>>).

loop(Parent, Sock, Port, Buf) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, Data} ->
            handle_data(Parent, Sock, Port, <<Buf/binary, Data/binary>>),
            loop(Parent, Sock, Port, Buf);
        {error, _Reason} = Error ->
            Error
    end.

%%%-----------------------------------------------------------------------------
%%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------

handle_data(Parent, Sock, Port, Data) ->
    {Response, Unparsed} = response(Parent, Port, Data),
    ct:log("~p sending response ~p~nremaining data ~p",
           [Port, iolist_to_binary(Response), iolist_to_binary(Unparsed)]),
    send(Sock, iolist_to_binary(Response)),
    %% this assumes any remaining buffered data includes only complete requests.
    case Unparsed of
        <<>> ->
            ok;
        _ ->
            handle_data(Parent, Sock, Port, Unparsed)
    end.

%% We send one byte at a time to test that we are handling package split correctly
send(_Sock, <<>>) ->
    ok;
send(Sock, <<Byte:1/binary, Rest/binary>>) ->
    gen_tcp:send(Sock, Byte),
    timer:sleep(1),
    send(Sock, Rest).

flush_all(Parent, Port) ->
    gen_server:call(Parent, {flush_all, Port}).

get_key(Parent, Port, Key) ->
    {ok, Result} = gen_server:call(Parent, {get_key, Port, Key}),
    Result.

put_key(Parent, Port, Key, Value, CAS) ->
    gen_server:call(Parent, {put_key, Port, Key, Value, CAS}).

put_key(Parent, Port, Key, Value) ->
    put_key(Parent, Port, Key, Value, undefined).

parse(<<16#80:8, _Rest/binary>> = Request) ->
    ct:log("About to parse request: ~p", [Request]),
    {Resp, Unparsed} = parse_binary(Request),
    {binary, Resp, Unparsed};
parse(Request) ->
    ct:log("About to parse text request: ~p", [Request]),
    Resp = parse_text(split(Request)),
    ct:log("Parsed command: ~p", [Resp]),
    {text, Resp, <<>>}.

%%%===================================================================
%%% Text Protocol
%%%===================================================================

%%%% Response

canned_responses(text, _Index, _Key, _Op, not_found) ->
    ["NOT_FOUND", <<"\r\n">>];
canned_responses(text, _Index, _Key, _Op, not_stored) ->
    ["NOT_STORED", <<"\r\n">>];
canned_responses(text, _Index, _Key, _Op, stored) ->
    [<<"STORED">>, <<"\r\n">>];
canned_responses(text, _Index, _Key, _Op, already_exists) ->
    [<<"EXISTS">>, <<"\r\n">>];
canned_responses(text, _Index, _Key, _Op, deleted) ->
    [<<"DELETED">>, <<"\r\n">>];
canned_responses(text, _Index, _Key, _Op, flushed) ->
    [<<"FLUSHED">>, <<"\r\n">>];
canned_responses(text, _Index, _Key, _Op, {incr, I}) ->
    [mero_util:to_bin(I), <<"\r\n">>];
canned_responses(text, _Index, _Key, _Op, noop) ->
    [];
canned_responses(binary, Index, _Key, Op, not_found) ->
    ExtrasOut = <<>>,
    ExtrasSizeOut = size(ExtrasOut),
    Status = ?NOT_FOUND,
    BodyOut = <<>>,
    BodySizeOut = size(BodyOut),
    KeySize = 0,
    <<16#81:8,
      Op:8,
      KeySize:16,
      ExtrasSizeOut:8,
      0,
      Status:16,
      BodySizeOut:32,
      (opaque(Index)):32,
      0:64,
      BodyOut/binary>>;
canned_responses(binary, Index, _Key, Op, not_stored) ->
    ExtrasOut = <<>>,
    ExtrasSizeOut = size(ExtrasOut),
    Status = ?NOT_STORED,
    BodyOut = <<>>,
    BodySizeOut = size(BodyOut),
    KeySize = 0,

    <<16#81:8,
      Op:8,
      KeySize:16,
      ExtrasSizeOut:8,
      0,
      Status:16,
      BodySizeOut:32,
      (opaque(Index)):32,
      0:64,
      BodyOut/binary>>;
canned_responses(binary, Index, _Key, Op, stored) ->
    ExtrasOut = <<>>,
    ExtrasSizeOut = size(ExtrasOut),
    Status = ?NO_ERROR,
    BodyOut = <<>>,
    BodySizeOut = size(BodyOut),
    KeySize = 0,

    <<16#81:8,
      Op:8,
      KeySize:16,
      ExtrasSizeOut:8,
      0,
      Status:16,
      BodySizeOut:32,
      (opaque(Index)):32,
      0:64,
      BodyOut/binary>>;
canned_responses(binary,
                 Index,
                 _Key,
                 Op,
                 deleted) -> %% same as stored, intentionally
    ExtrasOut = <<>>,
    ExtrasSizeOut = size(ExtrasOut),
    Status = ?NO_ERROR,
    BodyOut = <<>>,
    BodySizeOut = size(BodyOut),
    KeySize = 0,

    <<16#81:8,
      Op:8,
      KeySize:16,
      ExtrasSizeOut:8,
      0,
      Status:16,
      BodySizeOut:32,
      (opaque(Index)):32,
      0:64,
      BodyOut/binary>>;
canned_responses(binary, Index, _Key, ?MEMCACHE_INCREMENT, {incr, I}) ->
    ExtrasOut = <<>>,
    ExtrasSizeOut = size(ExtrasOut),
    Status = ?NO_ERROR,
    BodyOut = <<ExtrasOut/binary, I:64/integer>>,
    BodySizeOut = size(BodyOut),
    KeySize = 0,

    <<16#81:8,
      ?MEMCACHE_INCREMENT:8,
      KeySize:16,
      ExtrasSizeOut:8,
      0,
      Status:16,
      BodySizeOut:32,
      (opaque(Index)):32,
      0:64,
      BodyOut/binary>>;
canned_responses(binary, Index, _Key, Op, already_exists) ->
    ExtrasOut = <<>>,
    ExtrasSizeOut = size(ExtrasOut),
    Status = ?KEY_EXISTS,
    BodyOut = <<>>,
    BodySizeOut = size(BodyOut),
    KeySize = 0,

    <<16#81:8,
      Op:8,
      KeySize:16,
      ExtrasSizeOut:8,
      0,
      Status:16,
      BodySizeOut:32,
      (opaque(Index)):32,
      0:64,
      BodyOut/binary>>;
canned_responses(binary, _Index, _Key, Op, flushed) ->
    ExtrasOut = <<>>,
    ExtrasSizeOut = size(ExtrasOut),
    Status = ?NO_ERROR,
    BodyOut = <<>>,
    BodySizeOut = size(BodyOut),
    KeySize = 0,

    <<16#81:8,
      Op:8,
      KeySize:16,
      ExtrasSizeOut:8,
      0,
      Status:16,
      BodySizeOut:32,
      16#00:32,
      0:64,
      BodyOut/binary>>;
canned_responses(binary, _Index, _Key, _Op, noop) ->
    [].

opaque(undefined) ->
    16#00;
opaque(Index) when is_integer(Index) ->
    Index.

text_response_get_keys(_Parent, _Port, [], Acc, _WithCas) ->
    [Acc, "END\r\n"];
text_response_get_keys(Parent, Port, [Key | Keys], Acc, WithCas) ->
    case get_key(Parent, Port, Key) of
        undefined ->
            text_response_get_keys(Parent, Port, Keys, Acc, WithCas);
        {Value, CAS} ->
            LValue = mero_util:to_bin(Value),
            NBytes = size(LValue),
            NAcc =
                [Acc,
                 "VALUE",
                 " ",
                 mero_util:to_bin(Key),
                 " 00 ",
                 mero_util:to_bin(NBytes),
                 case WithCas of
                     true ->
                         [" ", mero_util:to_bin(CAS)];
                     _ ->
                         ""
                 end,
                 "\r\n",
                 mero_util:to_bin(LValue),
                 "\r\n"],
            text_response_get_keys(Parent, Port, Keys, NAcc, WithCas)
    end.

%% NOTE: This is not correct. Right now we don't distinguish between multiple
%% kinds of GETs, quiet and not. We must.
binary_response_get_keys(_Parent, _Port, [], Acc, _WithCas) ->
    Acc;
binary_response_get_keys(Parent, Port, [{Op, Key} | Keys], Acc, WithCas) ->
    {Status, Value, CAS} =
        case get_key(Parent, Port, Key) of
            undefined ->
                {?NOT_FOUND, <<>>, undefined};
            {Val, StoredCAS} ->
                {?NO_ERROR, Val, StoredCAS}
        end,
    LValue = mero_util:to_bin(Value),
    ExtrasOut = <<>>,
    ExtrasSizeOut = size(ExtrasOut),
    BodyOut = <<ExtrasOut/binary, Key/binary, LValue/binary>>,
    BodySizeOut = size(BodyOut),
    KeySize = size(Key),
    CASValue =
        case CAS of
            undefined ->
                0;
            _ ->
                CAS
        end,
    binary_response_get_keys(Parent,
                             Port,
                             Keys,
                             [<<16#81:8,
                                Op:8,
                                KeySize:16,
                                ExtrasSizeOut:8,
                                0,
                                Status:16,
                                BodySizeOut:32,
                                0:32,
                                CASValue:64,
                                BodyOut/binary>>
                              | Acc],
                             WithCas).

%% TODO add stored / not stored responses here

response(Parent, Port, Request) ->
    {Kind, {DeleteKeys, Cmd}, Unparsed} = parse(Request),
    lists:foreach(fun(K) -> put_key(Parent, Port, K, undefined) end, DeleteKeys),
    Response =
        case {Kind, Cmd} of
            {Kind, flush_all} ->
                flush_all(Parent, Port),
                canned_responses(Kind, undefined, undefined, ?MEMCACHE_FLUSH_ALL, flushed);
            {Kind, {get, Keys}} ->
                case Kind of
                    text ->
                        text_response_get_keys(Parent, Port, Keys, [], false);
                    binary ->
                        binary_response_get_keys(Parent, Port, Keys, [], false)
                end;
            {Kind, {gets, Keys}} ->
                case Kind of
                    text ->
                        R = text_response_get_keys(Parent, Port, Keys, [], true),
                        ct:log("gets result: ~p", [iolist_to_binary(R)]),
                        R;
                    binary ->
                        binary_response_get_keys(Parent, Port, Keys, [], true)
                end;
            {_Kind, {set, Key, Bytes, _Index, true = _Quiet}} ->
                put_key(Parent, Port, Key, Bytes),
                <<>>;
            {Kind, {set, Key, Bytes, Index, false}} ->
                put_key(Parent, Port, Key, Bytes),
                canned_responses(Kind, Index, Key, ?MEMCACHE_SET, stored);
            {Kind, {cas, Key, Bytes, CAS, Index, Quiet}} ->
                Op = case Quiet of
                         true ->
                             ?MEMCACHE_SETQ;
                         false ->
                             ?MEMCACHE_SET
                     end,
                case get_key(Parent, Port, Key) of
                    undefined ->
                        ct:log("cas of non-existent key ~p", [Key]),
                        canned_responses(Kind, Index, Key, Op, not_found);
                    {_, CAS} ->
                        ct:log("cas of existing key ~p with correct token ~p", [Key, CAS]),
                        put_key(Parent, Port, Key, Bytes, CAS + 1),
                        case Quiet of
                            true ->
                                <<>>;
                            false ->
                                canned_responses(Kind, Index, Key, Op, stored)
                        end;
                    {_, ExpectedCAS} ->
                        ct:log("cas of existing key ~p with incorrect token ~p (wanted ~p)",
                               [Key, CAS, ExpectedCAS]),
                        canned_responses(Kind, Index, Key, Op, already_exists)
                end;
            {Kind, {delete, Key}} ->
                ct:log("deleting ~p", [Key]),
                case get_key(Parent, Port, Key) of
                    undefined ->
                        ct:log("was not present"),
                        canned_responses(Kind, undefined, Key, ?MEMCACHE_DELETE, not_found);
                    {_Value, _} ->
                        ct:log("key was present"),
                        put_key(Parent, Port, Key, undefined, undefined),
                        canned_responses(Kind, undefined, Key, ?MEMCACHE_DELETE, deleted)
                end;
            {Kind, {add, Key, Bytes, Index, Quiet}} ->
                Op = case Quiet of
                         true ->
                             ?MEMCACHE_ADDQ;
                         false ->
                             ?MEMCACHE_ADD
                     end,
                case get_key(Parent, Port, Key) of
                    undefined ->
                        put_key(Parent, Port, Key, Bytes, undefined),
                        case Quiet of
                            true ->
                                <<>>;
                            false ->
                                canned_responses(Kind, Index, Key, Op, stored)
                        end;
                    {_Value, _} ->
                        canned_responses(Kind, Index, Key, Op, already_exists)
                end;
            {Kind, {incr, Key, ExpTime, Initial, Bytes}} ->
                case get_key(Parent, Port, Key) of
                    undefined ->
                        %% Return error
                        case ExpTime of
                            4294967295 -> %% 32 bits, all 1
                                canned_responses(Kind,
                                                 undefined,
                                                 Key,
                                                 ?MEMCACHE_INCREMENT,
                                                 not_found);
                            _ ->
                                put_key(Parent, Port, Key, Initial),
                                canned_responses(Kind,
                                                 undefined,
                                                 Key,
                                                 ?MEMCACHE_INCREMENT,
                                                 {incr, Initial})
                        end;
                    {Value, _} ->
                        Result = mero_util:to_int(Value) + mero_util:to_int(Bytes),
                        put_key(Parent, Port, Key, Result),
                        canned_responses(Kind, undefined, Key, ?MEMCACHE_INCREMENT, {incr, Result})
                end
        end,
    {Response, Unparsed}.

%%% Parse

parse_text([<<"get">> | Keys]) ->
    {[], {get, Keys}};
parse_text([<<"gets">> | Keys]) ->
    {[], {gets, Keys}};
parse_text([<<"set">>, Key, _Flag, _ExpTime, _NBytes, Bytes]) ->
    {[], {set, Key, Bytes, undefined, false}};
parse_text([<<"cas">>, Key, _Flag, _ExpTime, _NBytes, CAS, Bytes]) ->
    {[], {cas, Key, Bytes, binary_to_integer(CAS), undefined, false}};
parse_text([<<"add">>, Key, _Flag, _ExpTime, _NBytes, Bytes]) ->
    {[], {add, Key, Bytes, undefined, false}};
parse_text([<<"delete">>, Key]) ->
    {[], {delete, Key}};
parse_text([<<"delete">>, Key, <<"noreply">>, <<>> | Rest]) ->
    parse_multi_delete_text([Key], Rest);
parse_text([<<"incr">>, Key, Value]) ->
    {[], {incr, Key, 100, Value, Value}};
parse_text([<<"flush_all">>]) ->
    {[], flush_all}.

parse_multi_delete_text(Acc, []) ->
    {Acc, undefined};
parse_multi_delete_text(Acc, [<<"delete">>, Key, <<"noreply">>, <<>> | Rest]) ->
    parse_multi_delete_text([Key | Acc], Rest);
parse_multi_delete_text(Acc, Other) ->
    {[], Cmd} = parse_text(Other),
    {Acc, Cmd}.

split(Binary) ->
    binary:split(Binary, [<<"\r\n">>, <<" ">>], [global, trim]).

%%%===================================================================
%%% Binary Protocol
%%%===================================================================

%%% Parse

parse_binary(<<16#80:8, ?MEMCACHE_FLUSH_ALL:8, _/binary>>) ->
    {{[], flush_all}, <<>>};
parse_binary(<<16#80:8, ?MEMCACHE_GET:8, _/binary>> = Bin) ->
    {{[], {get, parse_get([], Bin)}}, <<>>};
parse_binary(<<16#80:8, ?MEMCACHE_GETQ:8, _/binary>> = Bin) ->
    {{[], {get, parse_get([], Bin)}}, <<>>};
parse_binary(<<16#80:8, ?MEMCACHE_GETK:8, _/binary>> = Bin) ->
    {{[], {get, parse_get([], Bin)}}, <<>>};
parse_binary(<<16#80:8, ?MEMCACHE_GETKQ:8, _/binary>> = Bin) ->
    {{[], {get, parse_get([], Bin)}}, <<>>};
parse_binary(<<16#80:8,
               Op:8,
               KeySize:16,
               ExtrasSize:8,
               16#00:8,
               16#00:16,
               BodySize:32,
               Index:32,
               CAS:64,
               _Extras:ExtrasSize/binary,
               Key:KeySize/binary,
               Rest/binary>>)
    when Op == ?MEMCACHE_SET; Op == ?MEMCACHE_SETQ ->
    Quiet = Op == ?MEMCACHE_SETQ,
    ValueSize = BodySize - ExtrasSize - KeySize,
    <<Value:ValueSize/binary, Remaining/binary>> = Rest,
    case CAS of
        16#00 ->
            {{[], {set, Key, Value, Index, Quiet}}, Remaining};
        _ ->
            {{[], {cas, Key, Value, CAS, Index, Quiet}}, Remaining}
    end;
parse_binary(<<16#80:8,
               Op:8,
               KeySize:16,
               ExtrasSize:8,
               16#00:8,
               16#00:16,
               BodySize:32,
               Index:32,
               16#00:64,
               _Extras:ExtrasSize/binary,
               Key:KeySize/binary,
               Rest/binary>>)
    when Op == ?MEMCACHE_ADD; Op == ?MEMCACHE_ADDQ ->
    Quiet = Op == ?MEMCACHE_ADDQ,
    ValueSize = BodySize - ExtrasSize - KeySize,
    <<Value:ValueSize/binary, Remaining/binary>> = Rest,
    {{[], {add, Key, Value, Index, Quiet}}, Remaining};
parse_binary(<<16#80:8,
               ?MEMCACHE_DELETE:8,
               KeySize:16,
               ExtrasSize:8,
               16#00:8,
               16#00:16,
               _BodySize:32,
               16#00:32,
               16#00:64,
               _Extras:ExtrasSize/binary,
               Key:KeySize/binary>>) ->
    {{[], {delete, Key}}, <<>>};
parse_binary(<<16#80:8, ?MEMCACHE_DELETEQ:8, _/binary>> = Inp) ->
    parse_multi_delete_binary([], Inp);
parse_binary(<<16#80:8,
               ?MEMCACHE_INCREMENT:8,
               KeySize:16,
               _ExtrasSize:8,
               16#00:8,
               16#00:16,
               _BodySize:32,
               16#00:32,
               16#00:64,
               Value:64,
               Initial:64,
               ExpTime:32,
               Key:KeySize/binary>>) ->
    {{[], {incr, Key, ExpTime, Initial, Value}}, <<>>}.

parse_multi_delete_binary(Acc, []) ->
    {{Acc, undefined}, <<>>};
parse_multi_delete_binary(Acc,
                          <<16#80:8,
                            ?MEMCACHE_DELETEQ:8,
                            KeySize:16,
                            ExtrasSize:8,
                            16#00:8,
                            16#00:16,
                            _BodySize:32,
                            16#00:32,
                            16#00:64,
                            _Extras:ExtrasSize/binary,
                            Key:KeySize/binary,
                            Rest/binary>>) ->
    parse_multi_delete_binary([Key | Acc], Rest);
parse_multi_delete_binary(Acc, Other) ->
    {{[], Cmd}, Remaining} = parse_binary(Other),
    {{Acc, Cmd}, Remaining}.

parse_get(Acc, <<>>) ->
    Acc;
parse_get(Acc,
          <<16#80:8,
            ?MEMCACHE_GET:8,
            KeySize:16,
            _ExtrasSize:8,
            16#00:8,
            16#00:16,
            _BodySize:32,
            16#00:32,
            16#00:64,
            Key:KeySize/binary,
            Rest/binary>>) ->
    parse_get([{?MEMCACHE_GET, Key} | Acc], Rest);
parse_get(Acc,
          <<16#80:8,
            ?MEMCACHE_GETQ:8,
            KeySize:16,
            _ExtrasSize:8,
            16#00:8,
            16#00:16,
            _BodySize:32,
            16#00:32,
            16#00:64,
            Key:KeySize/binary,
            Rest/binary>>) ->
    parse_get([{?MEMCACHE_GETQ, Key} | Acc], Rest);
parse_get(Acc,
          <<16#80:8,
            ?MEMCACHE_GETK:8,
            KeySize:16,
            _ExtrasSize:8,
            16#00:8,
            16#00:16,
            _BodySize:32,
            16#00:32,
            16#00:64,
            Key:KeySize/binary,
            Rest/binary>>) ->
    parse_get([{?MEMCACHE_GETK, Key} | Acc], Rest);
parse_get(Acc,
          <<16#80:8,
            ?MEMCACHE_GETKQ:8,
            KeySize:16,
            _ExtrasSize:8,
            16#00:8,
            16#00:16,
            _BodySize:32,
            16#00:32,
            16#00:64,
            Key:KeySize/binary,
            Rest/binary>>) ->
    parse_get([{?MEMCACHE_GETKQ, Key} | Acc], Rest).
