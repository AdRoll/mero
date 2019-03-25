%% Copyright (c) 2018, AdRoll
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
-module(mero_wrk_tcp_txt_SUITE).

-include_lib("mero/include/mero.hrl").

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    all/0,
    init_per_testcase/2,
    end_per_testcase/2,
    mero_get_not_found/1,
    mero_get_found/1,
    mero_set/1,
    mero_set_cas/1,
    mero_set_cas_exists/1,
    mero_set_cas_not_found/1,
    mero_add/1,
    mero_add_not_stored/1,
    mero_delete/1,
    mero_delete_not_found/1,
    mero_flush_all/1,
    mero_mget/1
]).

-export([stats/1]).

all() -> [
    mero_get_not_found,
    mero_get_found,
    mero_set,
    mero_set_cas,
    mero_set_cas_exists,
    mero_set_cas_not_found,
    mero_add,
    mero_add_not_stored,
    mero_delete,
    mero_delete_not_found,
    mero_flush_all,
    mero_mget
].

init_per_testcase(_, Conf) ->
    meck:new(gen_tcp, [unstick]),
    meck:expect(gen_tcp, connect, fun(_,_,_) -> {ok, socket} end),
    meck:expect(gen_tcp, controlling_process, fun(_,_) -> ok end),
    meck:expect(gen_tcp, close, fun(_) -> ok end),
    meck:expect(gen_tcp, send, fun(_,_) -> ok end),
    Conf.

end_per_testcase(_, _Conf) ->
    meck:unload(gen_tcp),
    ok.


%%%=============================================================================
%%% Tests
%%%=============================================================================

-define(GET_NOT_FOUND_RESPONSE, <<"END\r\n">>).

-define(GET_FOUND_RESPONSE, <<"VALUE aa 0 19\r\nsome cached\r\n value\r\nEND\r\n">>).

-define(STORED_RESPONSE, <<"STORED\r\n">>).

-define(EXISTS_RESPONSE, <<"EXISTS\r\n">>).

-define(NOT_STORED_RESPONSE, <<"NOT_STORED\r\n">>).

-define(NOT_FOUND_RESPONSE, <<"NOT_FOUND\r\n">>).

-define(DELETED_RESPONSE, <<"DELETED\r\n">>).

-define(OK_RESPONSE, <<"OK\r\n">>).

-define(MGET_RESPONSE, <<"VALUE aa 0 19 31198\r\nsome cached\r\n value\r\n"
                         "VALUE b 0 9\r\nsomething\r\nEND\r\n">>).

%% Return up to L of the remaining bytes in buffer in each call.
fake_network_recv(Buffer, L) ->
    receive
        {read, Pid} ->
            case Buffer of
                <<B:L/binary, Rest/binary>> ->
                    Pid ! {ok, B},
                    fake_network_recv(Rest, L);
                _ ->
                    Pid ! {ok, Buffer},
                    fake_network_recv(<<>>, L)
            end
    end.

network_read(Pid) ->
    Pid ! {read, self()},
    receive
        {ok, B} ->
            {ok, B}
    end.

stats(Metric) ->
    ct:log("~p", [Metric]).


test_response_parsing(Buffer, ExpectedResult, {MemcachedOp, MemcachedOpArgs}) ->
    %% Reads from the buffer in different chunk sizes, to exercise the buffering done
    %% on mero_wrk_tcp_binary.  Check that the parsed result is the expected one.
    lists:foreach(fun(ReadSize) ->
        FakeNetwork = spawn_link(fun() -> fake_network_recv(Buffer, ReadSize) end),
        meck:expect(gen_tcp, recv, fun(_, 0, _Timeout) -> network_read(FakeNetwork) end),
        {ok, Client} = mero_wrk_tcp_txt:connect("localhost", 5000, {?MODULE, stats, []}),
        %dbg:tracer(),
        dbg:p(all, [c]),
        dbg:tpl(mero_wrk_tcp_txt, x),
        dbg:tpl(gen_tcp, recv, x),
        ?assertMatch({Client, ExpectedResult},
                      mero_wrk_tcp_txt:transaction(Client, MemcachedOp, MemcachedOpArgs))
      end, [10, 2, 1024]).

mero_get_not_found(_Conf) ->
    test_response_parsing(
        ?GET_NOT_FOUND_RESPONSE,
        #mero_item{key =  <<"aa">>, value = undefined},
        {get, [<<"aa">>, mero_conf:add_now(100)]}
    ).

mero_get_found(_Conf) ->
    test_response_parsing(
        ?GET_FOUND_RESPONSE,
        #mero_item{key = <<"aa">>, value = <<"some cached\r\n value">>, cas = undefined},
        {get, [<<"aa">>, mero_conf:add_now(100)]}
    ).

mero_set(_Conf) ->
    test_response_parsing(
        ?STORED_RESPONSE,
        ok,
        {set, [<<"aa">>, <<"some cached value">>, <<"1000">>, mero_conf:add_now(100), undefined]}
    ).

mero_set_cas(_Conf) ->
    test_response_parsing(
        ?STORED_RESPONSE,
        ok,
        {set, [<<"aa">>, <<"some cached value">>, <<"1000">>, mero_conf:add_now(100), 31198]}
    ).

mero_set_cas_exists(_Conf) ->
    test_response_parsing(
        ?EXISTS_RESPONSE,
        {error, already_exists},
        {set, [<<"aa">>, <<"some cached value">>, <<"1000">>, mero_conf:add_now(100), 31198]}
    ).

mero_set_cas_not_found(_Conf) ->
    test_response_parsing(
        ?NOT_FOUND_RESPONSE,
        {error, not_found},
        {set, [<<"aa">>, <<"some cached value">>, <<"1000">>, mero_conf:add_now(100), 31198]}
    ).

mero_add(_Conf) ->
    test_response_parsing(
        ?STORED_RESPONSE,
        ok,
        {add, [<<"aa">>, <<"some cached value">>, <<"1000">>, mero_conf:add_now(100)]}
    ).

mero_add_not_stored(_Conf) ->
    test_response_parsing(
        ?NOT_STORED_RESPONSE,
        {error, not_stored},
        {add, [<<"aa">>, <<"some cached value">>, <<"1000">>, mero_conf:add_now(100)]}
    ).

mero_delete(_Conf) ->
    test_response_parsing(
        ?DELETED_RESPONSE,
        ok,
        {delete, [<<"aa">>, mero_conf:add_now(100)]}
    ).

mero_delete_not_found(_Conf) ->
    test_response_parsing(
        ?NOT_FOUND_RESPONSE,
        {error, not_found},
        {delete, [<<"aa">>, mero_conf:add_now(100)]}
    ).

mero_flush_all(_Conf) ->
    test_response_parsing(
        ?OK_RESPONSE,
        ok,
        {flush_all, [mero_conf:add_now(100)]}
    ).

mero_mget(_Conf) ->
    test_response_parsing(
        ?MGET_RESPONSE,
        [
            #mero_item{key = <<"b">>, value = <<"something">>},
            #mero_item{key = <<"aa">>, value = <<"some cached\r\n value">>, cas = 31198},
            #mero_item{key = <<"c">>, value = undefined}
        ],
        {async_mget_response, [[<<"b">>, <<"aa">>,<<"c">>], mero_conf:add_now(100)]}
    ).
