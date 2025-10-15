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

-behaviour(ct_suite).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, groups/0, init_per_group/2, end_per_group/2, init_per_testcase/2,
         end_per_testcase/2, add/1, delete/1, get_undefineds/1, increase_counter/1,
         increase_counter_clustered_key/1, increment/1, mdelete/1, multiget_defineds/1,
         multiget_defineds_clustered_keys/1, multiget_undefineds/1, set/1, undefined_counter/1,
         mincrease_counter/1, cas/1, madd/1, mset/1, mcas/1, state_ok/1, state_error/1,
         state_timeout/1, get_clustered_key/1]).

%%%=============================================================================
%%% common_test callbacks
%%%=============================================================================

all() ->
    [{group, text_protocol}, {group, binary_protocol}].

groups() ->
    [{text_protocol,
      [shuffle, {repeat_until_any_fail, 1}],
      [add,
       delete,
       get_undefineds,
       get_clustered_key,
       increase_counter,
       increase_counter_clustered_key,
       increment,
       mdelete,
       multiget_defineds,
       multiget_defineds_clustered_keys,
       multiget_undefineds,
       set,
       undefined_counter,
       cas,
       state_ok,
       state_error,
       state_timeout]},
     {binary_protocol,
      [shuffle, {repeat_until_any_fail, 1}],
      [add,
       delete,
       get_undefineds,
       get_clustered_key,
       increase_counter,
       increase_counter_clustered_key,
       %% mincrease_counter,
       increment,
       mdelete,
       multiget_defineds,
       multiget_defineds_clustered_keys,
       multiget_undefineds,
       set,
       undefined_counter,
       cas,
       madd,
       mset,
       mcas,
       state_ok,
       state_error,
       state_timeout]}].

init_per_group(text_protocol, Config) ->
    ClusterConfig =
        [{cluster,
          [{servers, [{"localhost", 11298}, {"localhost", 11299}]},
           {sharding_algorithm, {mero, shard_phash2}},
           {workers_per_shard, 1},
           {pool_worker_module, mero_wrk_tcp_txt}]},
         {cluster2,
          [{servers, [{"localhost", 11300}]},
           {sharding_algorithm, {mero, shard_crc32}},
           {workers_per_shard, 1},
           {pool_worker_module, mero_wrk_tcp_txt}]}],
    [{cluster_config, ClusterConfig} | Config];
init_per_group(binary_protocol, Config) ->
    ClusterConfig =
        [{cluster,
          [{servers, [{"localhost", 11298}, {"localhost", 11299}]},
           {sharding_algorithm, {mero, shard_phash2}},
           {workers_per_shard, 1},
           {pool_worker_module, mero_wrk_tcp_binary}]},
         {cluster2,
          [{servers, [{"localhost", 11300}]},
           {sharding_algorithm, {mero, shard_crc32}},
           {workers_per_shard, 1},
           {pool_worker_module, mero_wrk_tcp_binary}]}],
    [{cluster_config, ClusterConfig} | Config].

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(TestCase, Conf)
    when TestCase == state_error; TestCase == state_timeout ->
    meck:new(mero_pool, [passthrough]),
    init_per_testcase(default, Conf);
init_per_testcase(_TestCase, Conf) ->
    application:load(mero),
    ClusterConfig = ?config(cluster_config, Conf),
    Pids = mero_test_util:start_server(ClusterConfig, 1, 1, 30000, 90000),
    mero_conf:timeout_write(1000),
    mero_conf:timeout_read(1000),
    [{pids, Pids} | Conf].

end_per_testcase(TestCase, Conf)
    when TestCase == state_error; TestCase == state_timeout ->
    meck:unload(mero_pool),
    end_per_testcase(default, Conf);
end_per_testcase(_TestCase, Conf) ->
    {ok, Pids} = proplists:get_value(pids, Conf),
    mero_test_util:stop_servers(Pids),
    ok = application:stop(mero),
    ok.

%%%=============================================================================
%%% Tests
%%%=============================================================================
cas(_Conf) ->
    Key = key(),
    ct:log("state ~p", [mero:state()]),

    %% CAS with a token which is not undefined should result in a "not found" error:
    ?assertEqual({error, not_found}, mero:cas(cluster, Key, <<"asdf">>, 1000, 1000, 12345)),
    %% connection may have been closed due to error.  wait for free connections:
    await_connected(cluster),

    ?assertEqual({Key, undefined, undefined}, mero:gets(cluster, Key, 1000)),
    ?assertEqual(ok, mero:add(cluster, Key, <<"x">>, 1000, 1000)),
    {Key, <<"x">>, CAS} = mero:gets(cluster, Key, 1000),

    %% CAS with a token other than the token which was returned from
    %% `gets` should result in an "exists" error:
    ?assertEqual({error, already_exists},
                 mero:cas(cluster, Key, <<"y">>, 1000, 1000, CAS + 123)),
    await_connected(cluster),

    ?assertEqual({Key, <<"x">>}, mero:get(cluster, Key, 1000)),
    {Key, <<"x">>, CAS} = mero:gets(cluster, Key, 1000),

    %% CAS of an existing key using the expected token should succeed:
    ?assertEqual(ok, mero:cas(cluster, Key, <<"z">>, 1000, 1000, CAS)),
    {Key, <<"z">>, CAS1} = mero:gets(cluster, Key, 1000),

    %% overwriting the key without CAS should succeed, and the token should change:
    ?assertEqual(ok, mero:set(cluster, Key, <<"q">>, 1000, 1000)),
    {Key, <<"q">>, CAS2} = mero:gets(cluster, Key, 1000),
    ?assertEqual({Key, <<"q">>}, mero:get(cluster, Key, 1000)),
    ?assertNotEqual(CAS1, CAS2),
    ok.

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
    ?assertMatch({ok, 1}, mero:increment_counter(cluster, Key)),
    ?assertMatch({ok, 2}, mero:increment_counter(cluster, Key)),
    ?assertMatch({ok, 1}, mero:increment_counter(cluster2, Key)),
    ?assertMatch({ok, 2}, mero:increment_counter(cluster2, Key)),
    ok.

increase_counter_clustered_key(_Conf) ->
    Key = {<<"22">>, key()},
    ct:log("state ~p", [mero:state()]),
    ?assertMatch({ok, 1}, mero:increment_counter(cluster, Key)),
    ?assertMatch({ok, 2}, mero:increment_counter(cluster, Key)),
    ?assertMatch({ok, 1}, mero:increment_counter(cluster2, Key)),
    ?assertMatch({ok, 2}, mero:increment_counter(cluster2, Key)),
    ok.

mincrease_counter(_Conf) ->
    Key0 = key(),
    Key1 = key(),
    ok = mero:mincrement_counter(cluster, [Key0, Key1]),
    Expected = [{Key0, 1}, {Key1, 1}],
    Ret = mero:mget(cluster, [Key0, Key1]),
    ?assertMatch(Expected, Ret).

delete(_Conf) ->
    ?assertMatch({<<"11">>, undefined}, mero:get(cluster, <<"11">>)),
    ?assertMatch(ok, mero:set(cluster, <<"11">>, <<"Adroll">>, 11111, 1000)),
    ?assertMatch({<<"11">>, <<"Adroll">>}, mero:get(cluster, <<"11">>)),
    ?assertMatch(ok, mero:delete(cluster, <<"11">>, 1000)),
    ?assertMatch({<<"11">>, undefined}, mero:get(cluster, <<"11">>)),
    ?assertMatch({error, not_found}, mero:delete(cluster, <<"11">>, 1000)).

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
    ?assertMatch(ok, mero:mdelete(cluster, [<<"11">>, <<"22">>], 1000)).

set(_Conf) ->
    ct:log("state ~p", [mero:state()]),
    ?assertMatch(ok, mero:set(cluster, <<"11">>, <<"Adroll">>, 11111, 1000)),
    ?assertMatch({<<"11">>, <<"Adroll">>}, mero:get(cluster, <<"11">>)),

    ?assertMatch(ok, mero:set(cluster, <<"12">>, <<"Adroll2">>, 11111, 1000)),
    ?assertMatch({<<"12">>, <<"Adroll2">>}, mero:get(cluster, <<"12">>)),

    Resp0 = mero:mget(cluster, [<<"11">>, <<"12">>], 5000),
    [{<<"11">>, <<"Adroll">>}, {<<"12">>, <<"Adroll2">>}] = lists:sort(Resp0),

    Resp1 = mero:mget(cluster2, [<<"11">>, <<"12">>], 5000),
    [{<<"11">>, undefined}, {<<"12">>, undefined}] = lists:sort(Resp1),

    ok.

get_undefineds(_Conf) ->
    Key = key(),
    Key2 = key(),
    Key3 = key(),

    {Key, undefined} = mero:get(cluster, Key, 1000),
    {Key2, undefined} = mero:get(cluster, Key2, 1000),
    {Key3, undefined} = mero:get(cluster, Key3, 1000).

get_clustered_key(_) ->
    Key = key(),
    ClusteredKey = {<<"1">>, Key},
    ?assertEqual(ok, mero:set(cluster, ClusteredKey, <<"Adroll">>, 11111, 1000)),
    ?assertEqual({Key, <<"Adroll">>}, mero:get(cluster, ClusteredKey, 1000)).

multiget_undefineds(_Conf) ->
    [] = mero:mget(cluster, [], 1000),

    %% 13, 14 and 15 will go to the same server
    %% 11, 12 and 16 to a different one
    Resp =
        mero:mget(cluster, [<<"11">>, <<"12">>, <<"13">>, <<"14">>, <<"15">>, <<"16">>], 1000),
    [{<<"11">>, undefined},
     {<<"12">>, undefined},
     {<<"13">>, undefined},
     {<<"14">>, undefined},
     {<<"15">>, undefined},
     {<<"16">>, undefined}] =
        lists:sort(Resp).

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
    Expected =
        [{<<"11">>, <<"1">>},
         {<<"12">>, <<"1">>},
         {<<"13">>, <<"1">>},
         {<<"14">>, <<"2">>},
         {<<"15">>, <<"2">>},
         {<<"16">>, <<"3">>},
         {<<"17">>, undefined}],
    ?assertEqual(Expected,
                 lists:sort(
                     mero:mget(cluster,
                               [<<"11">>,
                                <<"12">>,
                                <<"13">>,
                                <<"14">>,
                                <<"15">>,
                                <<"16">>,
                                <<"17">>],
                               1000))).

multiget_defineds_clustered_keys(_Conf) ->
    ?assertMatch({ok, 1}, mero:increment_counter(cluster, {<<"1">>, <<"11">>})),
    ?assertMatch({ok, 1}, mero:increment_counter(cluster, {<<"2">>, <<"12">>})),
    ?assertMatch({ok, 1}, mero:increment_counter(cluster, {<<"3">>, <<"13">>})),
    ?assertMatch({ok, 1}, mero:increment_counter(cluster, {<<"3">>, <<"16">>})),
    ?assertMatch({ok, 2}, mero:increment_counter(cluster, {<<"3">>, <<"16">>})),
    ?assertMatch({ok, 3}, mero:increment_counter(cluster, {<<"3">>, <<"16">>})),
    %% 13, 14 and 15 will go to the same server
    %% 11, 12 and 16 to a different one
    Expected =
        [{<<"11">>, <<"1">>},
         {<<"12">>, <<"1">>},
         {<<"13">>, <<"1">>},
         {<<"16">>, <<"3">>},
         {<<"17">>, undefined}],
    ?assertEqual(Expected,
                 lists:sort(
                     mero:mget(cluster,
                               [{<<"1">>, <<"11">>},
                                {<<"2">>, <<"12">>},
                                {<<"3">>, <<"13">>},
                                {<<"3">>, <<"16">>},
                                {<<"3">>, <<"17">>}],
                               1000))).

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
    ?assertEqual(ok, mero:add(cluster, <<"11">>, <<"Adroll">>, 11111, 1000)),
    ct:log("First not stored"),
    ?assertEqual({error, already_exists},
                 mero:add(cluster, <<"11">>, <<"Adroll2">>, 111111, 1000)),
    await_connected(cluster),
    ct:log("Second not stored"),
    ?assertEqual({error, already_exists},
                 mero:add(cluster, <<"11">>, <<"Adroll2">>, 111111, 1000)),
    await_connected(cluster),
    ?assertEqual({<<"11">>, <<"Adroll">>}, mero:get(cluster, <<"11">>)),

    ?assertEqual(ok, mero:delete(cluster, <<"11">>, 1000)),
    ?assertEqual({<<"11">>, undefined}, mero:get(cluster, <<"11">>)),

    ?assertEqual(ok, mero:add(cluster, <<"11">>, <<"Adroll3">>, 11111, 1000)),
    ?assertEqual({<<"11">>, <<"Adroll3">>}, mero:get(cluster, <<"11">>)).

madd(_) ->
    %% with one existing key, add new keys repeatedly, moving the
    %% position of the existing key each time:
    ExistingKey = key(),
    MakeKeys =
        fun(Start, Count) ->
           [<<"key", (integer_to_binary(I))/binary>> || I <- lists:seq(Start, Start + Count - 1)]
        end,
    Total = 10,
    lists:foreach(fun({Start, N}) ->
                     mero:flush_all(cluster),
                     ok = mero:add(cluster, ExistingKey, ExistingKey, 10000, 1000),
                     Keys =
                         MakeKeys(Start, N)
                         ++ [ExistingKey]
                         ++ MakeKeys(Start + N + 1, Total - N - 1),
                     Expected =
                         [case Key of
                              ExistingKey ->
                                  {error, already_exists};
                              _ ->
                                  ok
                          end
                          || Key <- Keys],
                     ?assertEqual(Expected,
                                  mero:madd(cluster, [{Key, Key, 10000} || Key <- Keys], 5000)),
                     ?assertEqual(lists:keysort(1, [{Key, Key} || Key <- Keys]),
                                  lists:keysort(1, mero:mget(cluster, Keys, 5000)))
                  end,
                  [{1, N} || N <- lists:seq(1, Total - 1)]).

mset(_) ->
    Keys = [key() || _ <- lists:seq(1, 10)],
    Updates = [{Key, Key, 10000} || Key <- Keys],
    Expected = lists:duplicate(length(Updates), ok),
    ?assertEqual(Expected, mero:mset(cluster, Updates, 5000)).

mcas(_) ->
    Keys = [key() || _ <- lists:seq(1, 10)],
    Updates = [{Key, Key, 10000} || Key <- Keys],
    ?assertEqual(lists:duplicate(length(Updates), ok), mero:mset(cluster, Updates, 5000)),
    await_connected(cluster),
    KVCs = mero:mgets(cluster, Keys, 5000),
    FailingKeys = [hd(Keys), lists:nth(length(Keys), Keys)],
    {NUpdates, Expected} =
        lists:unzip([case lists:member(Key, FailingKeys) of
                         true ->
                             {{Key, <<"should not update">>, 10000, CAS + 1},
                              {error, already_exists}};
                         false ->
                             {{Key, <<Key/binary, Key/binary>>, 10000, CAS}, ok}
                     end
                     || {Key, _, CAS} <- KVCs]),
    ?assertEqual(Expected, mero:mcas(cluster, NUpdates, 5000)).

%% OTP25 introduced a modernization on the `timer` module,
%% which refactors the amount of linked processes said module
%% will produce. Reference: https://github.com/erlang/otp/pull/4811
state_ok(_) ->
    State = mero:state(),
    ?assertMatch([{connected, 1},
                  {connecting, 0},
                  {failed, 0},
                  {free, 1},
                  %% @todo: restore to value '2' when we require OTP >= 25
                  {links, _},
                  {message_queue_len, 0},
                  {monitors, 0}],
                 lists:sort(
                     proplists:get_value(cluster2, State))),
    ?assertMatch([{connected, 2},
                  {connecting, 0},
                  {failed, 0},
                  {free, 2},
                  %% @todo: restore to value '4' when we require OTP >= 25
                  {links, _},
                  {message_queue_len, 0},
                  {monitors, 0}],
                 lists:sort(
                     proplists:get_value(cluster, State))).

state_error(_) ->
    meck:expect(mero_pool, state, 1, {error, down}),
    State = mero:state(),
    ?assertEqual([{connected, 0},
                  {connecting, 0},
                  {failed, 0},
                  {free, 0},
                  {links, 0},
                  {message_queue_len, 0},
                  {monitors, 0}],
                 lists:sort(
                     proplists:get_value(cluster2, State))),
    ?assertEqual([{connected, 0},
                  {connecting, 0},
                  {failed, 0},
                  {free, 0},
                  {links, 0},
                  {message_queue_len, 0},
                  {monitors, 0}],
                 lists:sort(
                     proplists:get_value(cluster, State))).

state_timeout(_) ->
    meck:expect(mero_pool, state, 1, {error, timeout}),
    State = mero:state(),
    ?assertEqual([{connected, 0},
                  {connecting, 0},
                  {failed, 0},
                  {free, 0},
                  {links, 0},
                  {message_queue_len, 0},
                  {monitors, 0}],
                 lists:sort(
                     proplists:get_value(cluster2, State))),
    ?assertEqual([{connected, 0},
                  {connecting, 0},
                  {failed, 0},
                  {free, 0},
                  {links, 0},
                  {message_queue_len, 0},
                  {monitors, 0}],
                 lists:sort(
                     proplists:get_value(cluster, State))).

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

key() ->
    base64:encode(
        crypto:strong_rand_bytes(20)).

await_connected(Cluster) ->
    ct:log("waiting for free connections"),
    Wait =
        fun W() ->
                State = mero:state(),
                case proplists:get_value(connected, proplists:get_value(Cluster, State)) of
                    N when is_integer(N) andalso N > 1 ->
                        ok;
                    _ ->
                        timer:sleep(100),
                        W()
                end
        end,
    Wait().
