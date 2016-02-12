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
-module(mero_SUITE).

-author('Miriam Pena <miriam.pena@adroll.com>').

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-compile(export_all).

-define(HOST, "127.0.0.1").
-define(PORT, 11911).

%%%=============================================================================
%%% common_test callbacks
%%%=============================================================================

all() -> [
          {group, text_protocol},
          {group, binary_protocol}
         ].

groups() ->
    [
     {text_protocol, [shuffle, {repeat_until_any_fail, 5}],
      [
       add,
       delete,
       get_undefineds,
       increase_counter,
       increment,
       mdelete,
       multiget_defineds,
       multiget_undefineds,
       set,
       undefined_counter
      ]
     },
     {binary_protocol, [shuffle, {repeat_until_any_fail, 5}],
      [
       add,
       delete,
       get_undefineds,
       increase_counter,
       increment,
       mdelete,
       multiget_defineds,
       multiget_undefineds,
       set,
       undefined_counter
      ]
     }
    ].

suite() ->
    [{timetrap, {seconds, 15}}].

init_per_group(text_protocol, Config) ->
    ClusterConfig = [{cluster,
                      [{servers, [{"localhost", 11298}, {"localhost", 11299}]},
                       {sharding_algorithm, {mero, shard_phash2}},
                       {workers_per_shard, 1},
                       {pool_worker_module, mero_wrk_tcp_txt}]
                     },
                     {cluster2,
                      [{servers, [{"localhost", 11300}]},
                       {sharding_algorithm, {mero, shard_crc32}},
                       {workers_per_shard, 1},
                       {pool_worker_module, mero_wrk_tcp_txt}]
                     }],
    [{cluster_config, ClusterConfig} | Config];
init_per_group(binary_protocol, Config) ->
    ClusterConfig = [{cluster,
                      [{servers, [{"localhost", 11298}, {"localhost", 11299}]},
                       {sharding_algorithm, {mero, shard_phash2}},
                       {workers_per_shard, 1},
                       {pool_worker_module, mero_wrk_tcp_binary}]
                     },
                     {cluster2,
                      [{servers, [{"localhost", 11300}]},
                       {sharding_algorithm, {mero, shard_crc32}},
                       {workers_per_shard, 1},
                       {pool_worker_module, mero_wrk_tcp_binary}]
                     }],
    [{cluster_config, ClusterConfig} | Config].

end_per_group(_GroupName, _Config) ->
    ok.

init_per_suite(Conf) ->
    ok = application:start(inets),
    Conf.

end_per_suite(_Conf) ->
    ok = application:stop(inets),
    ok.

init_per_testcase(_Module, Conf) ->
    application:load(mero),
    ClusterConfig = ?config(cluster_config, Conf),
    Pids = mero_test_util:start_server(ClusterConfig, 1, 1, 30000, 90000),
    mero_conf:timeout_write(1000),
    mero_conf:timeout_read(1000),
    [{pids, Pids} | Conf].

end_per_testcase(_Module, Conf) ->
    {ok, Pids} = proplists:get_value(pids, Conf),
    mero_test_util:stop_servers(Pids),
    mero_dummy_server:reset_all_keys(),
    ok = application:stop(mero),
    ok.


%%%=============================================================================
%%% Tests
%%%=============================================================================
undefined_counter(_Conf) ->
    Key = key(),
    ct:log("state ~p", [mero:state()]),
    ?assertMatch({Key, undefined}, mero:get(cluster, Key, 1000)),
    ?assertMatch({Key, undefined}, mero:get(cluster, Key, 1000)),
    ?assertMatch({Key, undefined}, mero:get(cluster2, Key, 1000)),
    ?assertMatch({Key, undefined}, mero:get(cluster2, Key, 1000)),
    ok.


increase_counter(_Conf) ->
    Key = key(),
    ct:log("state ~p", [mero:state()]),
    ct:log("READ ~p", [mero_conf:timeout_read()]),
    ct:log("WRITE ~p", [mero_conf:timeout_write()]),
    ?assertMatch({ok, 1}, mero:increment_counter(cluster, Key)),
    ?assertMatch({ok, 2}, mero:increment_counter(cluster, Key)),
    ?assertMatch({ok, 1}, mero:increment_counter(cluster2, Key)),
    ?assertMatch({ok, 2}, mero:increment_counter(cluster2, Key)),
    ok.


delete(_Conf) ->
    ?assertMatch({<<"11">>, undefined}, mero:get(cluster, <<"11">>)),
    ?assertMatch(ok, mero:set(cluster, <<"11">>, <<"Adroll">>, 11111, 1000)),
    ?assertMatch({<<"11">>, <<"Adroll">>}, mero:get(cluster, <<"11">>)),
    ?assertMatch(ok, mero:delete(cluster, <<"11">>, 1000)),
    ?assertMatch({<<"11">>, undefined}, mero:get(cluster, <<"11">>)),
    ?assertMatch({error, not_found},  mero:delete(cluster, <<"11">>, 1000)).


mdelete(_Conf) ->
    ?assertMatch({<<"11">>, undefined}, mero:get(cluster, <<"11">>)),
    ?assertMatch({<<"22">>, undefined}, mero:get(cluster, <<"22">>)),

    ?assertMatch(ok, mero:set(cluster, <<"11">>, <<"Adroll">>, 11111, 1000)),
    ?assertMatch(ok, mero:set(cluster, <<"22">>, <<"Adroll">>, 22222, 1000)),

    ?assertMatch({<<"11">>, <<"Adroll">>}, mero:get(cluster, <<"11">>)),
    ?assertMatch({<<"22">>, <<"Adroll">>}, mero:get(cluster, <<"22">>)),

    ?assertMatch(ok, mero:mdelete(cluster, [<<"11">>, <<"22">>], 1000)),

    ?assertMatch({<<"11">>, undefined}, mero:get(cluster, <<"11">>)),
    ?assertMatch({<<"22">>, undefined}, mero:get(cluster, <<"22">>)),

    %% mdelete is fire and forget. If this is undesirable an alternate approach
    %% can be taken but it's Good Enough for the motivating problem.
    ?assertMatch(ok,  mero:mdelete(cluster, [<<"11">>, <<"22">>], 1000)).


set(_Conf) ->
    ct:log("state ~p", [mero:state()]),
    ?assertMatch(ok, mero:set(cluster, <<"11">>, <<"Adroll">>, 11111, 1000)),
    ?assertMatch({<<"11">>, <<"Adroll">>}, mero:get(cluster, <<"11">>)),

    ?assertMatch(ok, mero:set(cluster, <<"12">>, <<"Adroll2">>, 11111, 1000)),
    ?assertMatch({<<"12">>, <<"Adroll2">>}, mero:get(cluster, <<"12">>)),

    Resp0 = mero:mget(cluster, [<<"11">>, <<"12">>], 5000),
    [{<<"11">>, <<"Adroll">>},
     {<<"12">>, <<"Adroll2">>}] = lists:sort(Resp0),

    Resp1 = mero:mget(cluster2, [<<"11">>, <<"12">>], 5000),
    [{<<"11">>, undefined},
     {<<"12">>, undefined}] = lists:sort(Resp1),

    ok.

get_undefineds(_Conf) ->
    Key = key(),
    Key2 = key(),
    Key3 = key(),

    {Key, undefined} = mero:get(cluster, Key, 1000),
    {Key2, undefined} = mero:get(cluster, Key2, 1000),
    {Key3, undefined} = mero:get(cluster, Key3, 1000).

multiget_undefineds(_Conf) ->
   [] = mero:mget(cluster, [], 1000),

    %% 13, 14 and 15 will go to the same server
    %% 11, 12 and 16 to a different one
    Resp = mero:mget(cluster, [<<"11">>,<<"12">>,<<"13">>,<<"14">>,<<"15">>,<<"16">>], 1000),

    [{<<"11">>, undefined},
     {<<"12">>, undefined},
     {<<"13">>, undefined},
     {<<"14">>, undefined},
     {<<"15">>, undefined},
     {<<"16">>, undefined}] = lists:sort(Resp).


multiget_defineds(_Conf) ->
    ?assertMatch({ok, 1}, mero:increment_counter(cluster, <<"11">>)),
    ?assertMatch({ok, 1}, mero:increment_counter(cluster, <<"12">>)),
    ?assertMatch({ok, 1}, mero:increment_counter(cluster, <<"13">>)),
    ?assertMatch({ok, 1}, mero:increment_counter(cluster, <<"14">>)),
    ?assertMatch({ok, 1}, mero:increment_counter(cluster, <<"15">>)),
    ?assertMatch({ok, 1}, mero:increment_counter(cluster, <<"16">>)),
    ?assertMatch({ok, 2}, mero:increment_counter(cluster, <<"14">>)),
    ?assertMatch({ok, 2}, mero:increment_counter(cluster, <<"15">>)),
    ?assertMatch({ok, 2}, mero:increment_counter(cluster, <<"16">>)),
    ?assertMatch({ok, 3}, mero:increment_counter(cluster, <<"16">>)),
    %% 13, 14 and 15 will go to the same server
    %% 11, 12 and 16 to a different one
    [{<<"17">>, undefined},
     {<<"15">>, <<"2">>},
     {<<"14">>, <<"2">>},
     {<<"13">>, <<"1">>},
     {<<"16">>, <<"3">>},
     {<<"12">>, <<"1">>},
     {<<"11">>, <<"1">>}] = mero:mget(cluster,
        [<<"11">>,<<"12">>,<<"13">>,<<"14">>,<<"15">>,<<"16">>,<<"17">>], 1000).


increment(_Conf) ->
    ?assertMatch({<<"11">>, undefined}, mero:get(cluster, <<"11">>)),
    ?assertMatch({<<"12">>, undefined}, mero:get(cluster, <<"12">>)),
    ?assertMatch({<<"13">>, undefined}, mero:get(cluster, <<"13">>)),
    ?assertMatch({<<"14">>, undefined}, mero:get(cluster, <<"14">>)),

    ?assertMatch({ok, 1}, mero:increment_counter(cluster, <<"11">>)),
    ?assertMatch({<<"11">>, <<"1">>}, mero:get(cluster, <<"11">>)),
    ?assertMatch({<<"12">>, undefined}, mero:get(cluster, <<"12">>)),
    ?assertMatch({<<"13">>, undefined}, mero:get(cluster, <<"13">>)),
    ?assertMatch({<<"14">>, undefined}, mero:get(cluster, <<"14">>)),

    ?assertMatch({ok, 1}, mero:increment_counter(cluster, <<"12">>)),
    ?assertMatch({<<"11">>, <<"1">>}, mero:get(cluster, <<"11">>)),
    ?assertMatch({<<"12">>, <<"1">>}, mero:get(cluster, <<"12">>)),
    ?assertMatch({<<"13">>, undefined}, mero:get(cluster, <<"13">>)),
    ?assertMatch({<<"14">>, undefined}, mero:get(cluster, <<"14">>)),

    ?assertMatch({ok, 1}, mero:increment_counter(cluster, <<"13">>)),
    ?assertMatch({ok, 2}, mero:increment_counter(cluster, <<"13">>)),
    ?assertMatch({ok, 3}, mero:increment_counter(cluster, <<"13">>)),
    ?assertMatch({ok, 1}, mero:increment_counter(cluster, <<"14">>)),
    ?assertMatch({ok, 2}, mero:increment_counter(cluster, <<"14">>)),

    ?assertMatch({<<"11">>, <<"1">>}, mero:get(cluster, <<"11">>)),
    ?assertMatch({<<"12">>, <<"1">>}, mero:get(cluster, <<"12">>)),
    ?assertMatch({<<"13">>, <<"3">>}, mero:get(cluster, <<"13">>)),
    ?assertMatch({<<"14">>, <<"2">>}, mero:get(cluster, <<"14">>)),
    ?assertMatch({<<"15">>, undefined}, mero:get(cluster, <<"15">>)),
    ?assertMatch({<<"16">>, undefined}, mero:get(cluster, <<"16">>)),

    ?assertMatch({<<"11">>, undefined}, mero:get(cluster2, <<"11">>)),
    ?assertMatch({<<"12">>, undefined}, mero:get(cluster2, <<"12">>)),
    ?assertMatch({<<"13">>, undefined}, mero:get(cluster2, <<"13">>)),
    ?assertMatch({<<"14">>, undefined}, mero:get(cluster2, <<"14">>)),
    ok.


add(_Conf) ->
    ?assertMatch(ok, mero:add(cluster, <<"11">>, <<"Adroll">>, 11111, 1000)),
    ct:log("First not stored"),
    ?assertMatch({error, not_stored}, mero:add(cluster, <<"11">>, <<"Adroll2">>, 111111, 1000)),
    ct:log("Second not stored"),
    ?assertMatch({error, not_stored}, mero:add(cluster, <<"11">>, <<"Adroll2">>, 111111, 1000)),
    ?assertMatch({<<"11">>, <<"Adroll">>}, mero:get(cluster, <<"11">>)),

    % ?assertMatch({<<"11">>, <<"Adroll">>}, mero:get(cluster, <<"11">>)),
    ok.
m() ->
    ?assertMatch(ok,  mero:delete(cluster, <<"11">>, 1000)),
    ?assertMatch({<<"11">>, undefined}, mero:get(cluster, <<"11">>)),

    ?assertMatch(ok, mero:add(cluster, <<"11">>, <<"Adroll3">>, 11111, 1000)),
    ?assertMatch({<<"11">>, <<"Adroll3">>}, mero:get(cluster, <<"11">>)).


%%%=============================================================================
%%% Internal functions
%%%=============================================================================

key() ->
    base64:encode(crypto:strong_rand_bytes(20)).
