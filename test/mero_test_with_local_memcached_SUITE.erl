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
-module(mero_test_with_local_memcached_SUITE).

-author('Miriam Pena <miriam.pena@adroll.com>').

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
         end_per_testcase/2, get_undefined_binary/1, get_undefined_txt/1, get_set_binary/1,
         get_set_txt/1, flush_binary/1, flush_txt/1, delete_binary/1, mdelete_binary/1,
         mdelete_txt/1, delete_txt/1, mget_binary/1, mget_txt/1, add_binary/1, add_txt/1,
         increment_binary/1, increment_txt/1, increment_binary_with_initial/1,
         increment_txt_with_initial/1, mincrement_binary/1, mincrement_txt/1, cas_binary/1,
         cas_txt/1, mgets_binary/1, madd_binary/1, mset_binary/1, mcas_binary/1]).

-define(HOST, "127.0.0.1").
-define(PORT, 11911).

%%%=============================================================================
%%% common_test callbacks
%%%=============================================================================

%% TODO: Uncomment these if you want to test agains a specific memcache server
all() ->
    [].    %% get_undefined_binary,
           %% get_undefined_txt,
           %% get_set_binary,
           %% get_set_txt,
           %% flush_binary,
           %% flush_txt,
           %% delete_binary,
           %% mdelete_binary,
           %% mdelete_txt,
           %% delete_txt,
           %% mget_binary,
           %% mget_txt,
           %% add_binary,
           %% add_txt,
           %% increment_binary,
           %% increment_txt,
           %% increment_binary_with_initial,
           %% increment_txt_with_initial,
           %% mincrement_binary,
           %% mincrement_txt,
           %% cas_binary,
           %% cas_txt,
           %% mgets_binary,
           %% madd_binary,
           %% mset_binary,
           %% mcas_binary

init_per_suite(Conf) ->
    application:load(mero),
    ok = mero_conf:cluster_config([{cluster_binary,
                                    [{servers, [{"localhost", 11211}]},
                                     {sharding_algorithm, {mero, shard_crc32}},
                                     {workers_per_shard, 1},
                                     {pool_worker_module, mero_wrk_tcp_binary}]},
                                   {cluster_txt,
                                    [{servers, [{"localhost", 11211}]},
                                     {sharding_algorithm, {mero, shard_crc32}},
                                     {workers_per_shard, 1},
                                     {pool_worker_module, mero_wrk_tcp_txt}]}]),
    ok = mero_conf:initial_connections_per_pool(4),
    ok = mero_conf:min_free_connections_per_pool(1),
    ok = mero_conf:expiration_interval(3000),
    ok = mero_conf:max_connections_per_pool(10),
    ok = mero_conf:connection_unused_max_time(10000),
    ok = mero_conf:timeout_read(1000),
    ok = mero_conf:timeout_write(1000),
    {ok, _} = application:ensure_all_started(mero),
    timer:sleep(5000),
    Conf.

end_per_suite(_Conf) ->
    ok = application:stop(mero),
    ok.

init_per_testcase(_Module, Conf) ->
    ct:log("state ~p", [mero:state()]),
    Keys = [key() || _ <- lists:seq(1, 4)],
    [{keys, Keys} | Conf].

end_per_testcase(_Module, _Conf) ->
    mero:flush_all(cluster_txt).

%%%=============================================================================
%%% Tests
%%%=============================================================================

get_undefined_binary(Conf) ->
    get_undefined(cluster_binary, keys(Conf)).

get_undefined_txt(Conf) ->
    get_undefined(cluster_txt, keys(Conf)).

get_set_binary(Conf) ->
    Keys = keys(Conf),
    get_set(cluster_binary, cluster_txt, Keys).

get_set_txt(Conf) ->
    Keys = keys(Conf),
    get_set(cluster_txt, cluster_binary, Keys).

flush_binary(Conf) ->
    Keys = keys(Conf),
    flush(cluster_binary, Keys).

flush_txt(Conf) ->
    Keys = keys(Conf),
    flush(cluster_txt, Keys).

delete_binary(Conf) ->
    Keys = keys(Conf),
    delete(cluster_binary, Keys).

mdelete_binary(Conf) ->
    Keys = keys(Conf),
    mdelete(cluster_binary, Keys).

delete_txt(Conf) ->
    Keys = keys(Conf),
    delete(cluster_txt, Keys).

mdelete_txt(Conf) ->
    Keys = keys(Conf),
    mdelete(cluster_txt, Keys).

mget_binary(Conf) ->
    Keys = keys(Conf),
    mget(cluster_binary, cluster_txt, Keys).

mget_txt(Conf) ->
    Keys = keys(Conf),
    mget(cluster_txt, cluster_binary, Keys).

add_binary(Conf) ->
    Keys = keys(Conf),
    add(cluster_binary, cluster_txt, Keys).

add_txt(Conf) ->
    Keys = keys(Conf),
    add(cluster_txt, cluster_binary, Keys).

increment_binary(Conf) ->
    Keys = keys(Conf),
    increment(cluster_binary, cluster_txt, Keys).

mincrement_binary(Conf) ->
    Keys = keys(Conf),
    mincrement(cluster_binary, cluster_txt, Keys).

mincrement_txt(Conf) ->
    Keys = keys(Conf),
    mincrement(cluster_txt, cluster_binary, Keys).

increment_txt(Conf) ->
    Keys = keys(Conf),
    increment(cluster_txt, cluster_binary, Keys).

increment_binary_with_initial(Conf) ->
    Keys = keys(Conf),
    increment_with_initial(cluster_binary, cluster_txt, Keys, 10, 2),
    mero:flush_all(cluster_binary),
    increment_with_initial(cluster_binary, cluster_txt, Keys, 0, 100).

increment_txt_with_initial(Conf) ->
    Keys = keys(Conf),
    increment_with_initial(cluster_binary, cluster_txt, Keys, 10, 2),
    mero:flush_all(cluster_binary),
    increment_with_initial(cluster_binary, cluster_txt, Keys, 800, 100).

cas_txt(Conf) ->
    Keys = keys(Conf),
    cas(cluster_txt, cluster_binary, Keys).

cas_binary(Conf) ->
    Keys = keys(Conf),
    cas(cluster_binary, cluster_txt, Keys).

mgets_binary(Conf) ->
    Keys = keys(Conf),
    mgets(cluster_binary, cluster_txt, Keys).

madd_binary(Conf) ->
    Keys = keys(Conf),
    madd(cluster_binary, cluster_txt, Keys),
    madd_moving(cluster_binary, cluster_txt, Keys).

mset_binary(Conf) ->
    Keys = keys(Conf),
    mset(cluster_binary, cluster_txt, Keys).

mcas_binary(Conf) ->
    Keys = keys(Conf),
    mcas(cluster_binary, cluster_txt, Keys).

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

keys(Conf) ->
    proplists:get_value(keys, Conf).

get_undefined(Cluster, Keys) ->
    ct:log("Checking empty keys with ~p~n", [Cluster]),
    ct:log("state ~p", [mero:state()]),
    [{Key, undefined} = mero:get(Cluster, Key) || Key <- Keys],
    ct:log("Checking empty keys ok~n").

get_set(Cluster, ClusterAlt, Keys) ->
    ct:log("Check set to adroll ~n"),
    ct:log("state ~p", [mero:state()]),
    [ok = mero:set(Cluster, Key, <<"Adroll">>, 11111, 1000) || Key <- Keys],
    ct:log("Checking keys ~n"),
    [{Key, <<"Adroll">>} = mero:get(Cluster, Key) || Key <- Keys],
    [{Key, <<"Adroll">>} = mero:get(ClusterAlt, Key) || Key <- Keys].

flush(Cluster, Keys) ->
    ct:log("Check set to adroll ~n"),
    ct:log("state ~p", [mero:state()]),
    [ok = mero:set(Cluster, Key, <<"Adroll">>, 11111, 1000) || Key <- Keys],
    ct:log("Flushing local memcache ! ~p ~n", [mero:flush_all(Cluster)]),

    ct:log("Checking empty keys ~n"),
    [{Key, undefined} = mero:get(Cluster, Key) || Key <- Keys].

delete(Cluster, Keys) ->
    ct:log("Check set to adroll ~n"),
    ct:log("state ~p", [mero:state()]),
    [ok = mero:set(Cluster, Key, <<"Adroll">>, 11111, 1000) || Key <- Keys],
    ct:log("Delete ! ~n", []),
    [ok = mero:delete(Cluster, Key, 1000) || Key <- Keys],

    ct:log("Checking empty keys ~n"),
    [{Key, undefined} = mero:get(Cluster, Key) || Key <- Keys].

mdelete(Cluster, Keys) ->
    ct:log("Check set to adroll ~n"),
    ct:log("state ~p", [mero:state()]),
    [ok = mero:set(Cluster, Key, <<"Adroll">>, 11111, 1000) || Key <- Keys],
    ct:log("Delete ! ~n", []),
    ok = mero:mdelete(Cluster, Keys, 1000),

    ct:log("Checking empty keys ~n"),
    [{Key, undefined} = mero:get(Cluster, Key) || Key <- Keys].

mget(Cluster, ClusterAlt, Keys) ->
    [?assertMatch(ok, mero:set(Cluster, Key, Key, 11111, 1000)) || Key <- Keys],
    io:format("Checking get itself ~n"),
    [?assertMatch({Key, Key}, mero:get(Cluster, Key)) || Key <- Keys],

    Results = mero:mget(Cluster, Keys, 10000),
    ResultsAlt = mero:mget(ClusterAlt, Keys, 10000),
    io:format("Checking mget ~p ~n", [Results]),
    [begin
       ?assertEqual({value, {Key, Key}}, lists:keysearch(Key, 1, Results)),
       ?assertEqual({value, {Key, Key}}, lists:keysearch(Key, 1, ResultsAlt))
     end
     || Key <- Keys].

mincrement(Cluster = cluster_txt, _ClusterAlt, Keys) ->
    {error, not_supportable} = mero:mincrement_counter(Cluster, Keys);
mincrement(Cluster = cluster_binary, _ClusterAlt, Keys) ->
    ok = mero:mincrement_counter(Cluster, Keys),
    MGetRet = lists:sort(mero:mget(Cluster, Keys)),
    Expected = lists:sort([{K, <<"1">>} || K <- Keys]),
    ?assertMatch(Expected, MGetRet).

increment(Cluster, ClusterAlt, Keys) ->
    io:format("Increment +1 +1 +1 ~n"),

    F = fun (Key, Expected) ->
                IncrementReturn = element(2, mero:increment_counter(Cluster, Key)),
                io:format("Increment return Expected ~p Received ~p~n",
                          [Expected, IncrementReturn]),
                {Key, Value2} = mero:get(Cluster, Key),
                io:format("Checking get ~p ~p ~n", [Cluster, Value2]),
                ?assertMatch(Expected, IncrementReturn),
                ?assertMatch(IncrementReturn, binary_to_integer(Value2)),
                {Key, Value3} = mero:get(ClusterAlt, Key),
                io:format("Checking get ~p ~p ~n", [ClusterAlt, Value3]),
                ?assertMatch(IncrementReturn, binary_to_integer(Value3))
        end,
    [F(Key, 1) || Key <- Keys],
    [F(Key, 2) || Key <- Keys],
    [F(Key, 3) || Key <- Keys].

increment_with_initial(Cluster, ClusterAlt, Keys, Initial, Steps) ->
    io:format("Increment +~p ~p ~n", [Initial, Steps]),

    F = fun (Key, Expected) ->
                IncrementReturn = element(2,
                                          mero:increment_counter(Cluster,
                                                                 Key,
                                                                 Steps,
                                                                 Initial,
                                                                 22222,
                                                                 3,
                                                                 1000)),
                io:format("Increment return Expected ~p Received ~p~n",
                          [Expected, IncrementReturn]),
                {Key, Value2} = mero:get(Cluster, Key),
                io:format("Checking get ~p ~p ~n", [Cluster, Value2]),
                {Key, Value3} = mero:get(ClusterAlt, Key),
                io:format("Checking get ~p ~p ~n", [ClusterAlt, Value3]),
                ?assertMatch(Expected, IncrementReturn),
                ?assertMatch(IncrementReturn, binary_to_integer(Value2)),
                ?assertMatch(IncrementReturn, binary_to_integer(Value3))
        end,
    [F(Key, Initial) || Key <- Keys],
    [F(Key, Initial + Steps) || Key <- Keys],
    [F(Key, Initial + 2 * Steps) || Key <- Keys].

add(Cluster, ClusterAlt, Keys) ->
    io:format("Attempt to add sucess to 5000 ~n"),
    Expected = <<"5000">>,
    Expected2 = <<"asdf">>,
    [begin
       ?assertEqual(ok, mero:add(Cluster, Key, Expected, 10000, 10000)),
       ?assertEqual({error, not_stored}, mero:add(cluster_txt, Key, Expected2, 10000, 10000)),
       ?assertEqual({error, already_exists},
                    mero:add(cluster_binary, Key, Expected2, 10000, 10000)),
       {Key, Value} = mero:get(Cluster, Key),
       {Key, Value2} = mero:get(ClusterAlt, Key),
       io:format("Checking get ~p ~p ~n", [Value, Value2]),
       ?assertEqual(Expected, Value),
       ?assertEqual(Expected, Value2)
     end
     || Key <- Keys].

cas(Cluster, ClusterAlt, Keys) ->
    Value1 = <<"asdf">>,
    Value2 = <<"foo">>,
    Value3 = <<"bar">>,
    [begin
       ?assertEqual({error, not_found}, mero:cas(Cluster, Key, Value1, 10000, 10000, 12345)),
       await_connected(Cluster),
       ?assertEqual(ok, mero:set(Cluster, Key, Value1, 10000, 10000)),
       ?assertEqual({Key, Value1}, mero:get(ClusterAlt, Key)),
       {Key, Value1, CAS} = mero:gets(Cluster, Key),
       {Key, Value1, CAS} = mero:gets(ClusterAlt, Key),
       ?assertEqual({error, already_exists},
                    mero:cas(Cluster, Key, Value2, 10000, 10000, CAS + 1)),
       await_connected(Cluster),
       ?assertEqual(ok, mero:cas(Cluster, Key, Value2, 10000, 10000, CAS)),
       ?assertEqual({error, already_exists},
                    mero:cas(ClusterAlt, Key, Value2, 10000, 10000, CAS)),
       await_connected(ClusterAlt),
       ?assertEqual({Key, Value2}, mero:get(ClusterAlt, Key)),
       ?assertEqual(ok, mero:set(Cluster, Key, Value3, 10000, 10000)),
       {Key, Value3, NCAS} = mero:gets(Cluster, Key),
       ?assertNotEqual(CAS, NCAS)
     end
     || Key <- Keys].

%% this is needed b/c our test server doesn't emulate a real memcached server with 100%
%% accuracy.
mgets(Cluster, _ClusterAlt, Keys) ->
    Expected = lists:keysort(1, [{Key, undefined, undefined} || Key <- Keys]),
    ?assertEqual(Expected, lists:keysort(1, mero:mgets(Cluster, Keys, 1000))).

madd(Cluster, _ClusterAlt, Keys) ->
    Expected = lists:duplicate(length(Keys), ok) ++ [{error, already_exists}],
    KVs = [{Key, <<"xyzzy">>, 1000} || Key <- Keys] ++ [{hd(Keys), <<"flub">>, 1000}],
    ?assertEqual(Expected, mero:madd(Cluster, KVs, 1000)),
    ?assertEqual({hd(Keys), <<"xyzzy">>}, mero:get(Cluster, hd(Keys), 1000)).

madd_moving(Cluster, _ClusterAlt, _Keys) ->
    %% with one existing key, add new keys repeatedly, moving the
    %% position of the existing key each time:
    ExistingKey = key(),
    MakeKeys = fun (Start, Count) ->
                       [<<"key", (integer_to_binary(I))/binary>>
                        || I <- lists:seq(Start, Start + Count - 1)]
               end,
    Total = 100,
    lists:foreach(fun ({Start, N}) ->
                          mero:flush_all(Cluster),
                          ok = mero:add(Cluster, ExistingKey, ExistingKey, 1000, 1000),
                          CurKeys = MakeKeys(Start, N) ++
                                      [ExistingKey] ++ MakeKeys(Start + N + 1, Total - N - 1),
                          ExpectedResult = [case Key of
                                              ExistingKey ->
                                                  {error, already_exists};
                                              _ ->
                                                  ok
                                            end
                                            || Key <- CurKeys],
                          ?assertEqual(ExpectedResult,
                                       mero:madd(Cluster,
                                                 [{Key, Key, 1000} || Key <- CurKeys],
                                                 1000)),
                          ?assertEqual(lists:keysort(1, [{Key, Key} || Key <- CurKeys]),
                                       lists:keysort(1, mero:mget(Cluster, CurKeys, 1000)))
                  end,
                  [{1, N} || N <- lists:seq(1, Total - 1)]).

mset(Cluster, _ClusterAlt, Keys) ->
    KVs = [{Key, Key, 1000} || Key <- Keys],
    Expected = lists:duplicate(length(Keys), ok),
    ?assertEqual(Expected, mero:mset(Cluster, KVs, 1000)),
    ?assertEqual(lists:keysort(1, [{Key, Key} || Key <- Keys]),
                 lists:keysort(1, mero:mget(Cluster, Keys, 1000))).

mcas(Cluster, _ClusterAlt, Keys) ->
    mero:mset(Cluster, [{Key, Key, 1000} || Key <- Keys], 1000),
    Stored = mero:mgets(Cluster, Keys, 1000),
    FailedUpdate = {element(1, hd(Stored)), <<"xyzzy">>, 1000, 12345},
    Updates = [FailedUpdate | [{Key, <<Key/binary, Key/binary>>, 1000, CAS}
                               || {Key, _, CAS} <- tl(Stored)]]
                ++ [FailedUpdate],
    Expected = [{error, already_exists} | lists:duplicate(length(Stored) - 1, ok)] ++
                 [{error, already_exists}],
    ?assertEqual(Expected, mero:mcas(Cluster, Updates, 1000)),
    ?assertEqual(lists:keysort(1,
                               [{element(1, hd(Stored)), element(1, hd(Stored))} | [{Key,
                                                                                     <<Key/binary,
                                                                                       Key/binary>>}
                                                                                    || {Key, _, _}
                                                                                           <- tl(Stored)]]),
                 lists:keysort(1, mero:mget(Cluster, Keys, 1000))).

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

key() ->
    base64:encode(crypto:strong_rand_bytes(20)).

await_connected(Cluster) ->
    ct:log("waiting for free connections"),
    Wait = fun W() ->
                   State = mero:state(),
                   case proplists:get_value(connected, proplists:get_value(Cluster, State)) of
                     N when is_integer(N) andalso N > 0 ->
                         ok;
                     _ ->
                         timer:sleep(100),
                         W()
                   end
           end,
    Wait().

