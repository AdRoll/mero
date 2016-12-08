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

-compile(export_all).

-define(HOST, "127.0.0.1").
-define(PORT, 11911).

%%%=============================================================================
%%% common_test callbacks
%%%=============================================================================

%% TODO: Uncomment these if you want to test agains a specific memcache server
all() -> [
         %% get_undefined_binary,
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
         %% cas_txt
         %% mgets_binary
    ].


suite() ->
    [{timetrap, {seconds, 15}}].

init_per_suite(Conf) ->
    ok = application:start(inets),

    application:load(mero),
    ok = mero_conf:cluster_config(
        [{cluster_binary,
            [{servers, [{"localhost", 11211}]},
             {sharding_algorithm, {mero, shard_crc32}},
             {workers_per_shard, 1},
             {pool_worker_module,mero_wrk_tcp_binary}]},
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
    ok = application:start(mero),
    timer:sleep(5000),
    Conf.


end_per_suite(_Conf) ->

    ok = application:stop(mero),
    ok = application:stop(inets),
    ok.


init_per_testcase(_Module, Conf) ->
    ct:log("state ~p", [mero:state()]),
    Keys = [key() || _  <- lists:seq(1, 4)],
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
    [ok = mero:set(Cluster, Key, <<"Adroll">>, 11111, 1000)
        || Key <- Keys],
    ct:log("Checking keys ~n"),
    [{Key, <<"Adroll">>} = mero:get(Cluster, Key) || Key <- Keys],
    [{Key, <<"Adroll">>} = mero:get(ClusterAlt, Key) || Key <- Keys].


flush(Cluster, Keys) ->
    ct:log("Check set to adroll ~n"),
    ct:log("state ~p", [mero:state()]),
    [ok = mero:set(Cluster, Key, <<"Adroll">>, 11111, 1000)
        || Key <- Keys],
    ct:log("Flushing local memcache ! ~p ~n", [mero:flush_all(Cluster)]),

    ct:log("Checking empty keys ~n"),
    [{Key, undefined} = mero:get(Cluster, Key) || Key <- Keys].



delete(Cluster, Keys) ->
    ct:log("Check set to adroll ~n"),
    ct:log("state ~p", [mero:state()]),
    [ok = mero:set(Cluster, Key, <<"Adroll">>, 11111, 1000)
        || Key <- Keys],

    ct:log("Delete ! ~n", []),
    [ok = mero:delete(Cluster, Key, 1000) || Key <- Keys],

    ct:log("Checking empty keys ~n"),
    [{Key, undefined} = mero:get(Cluster, Key) || Key <- Keys].


mdelete(Cluster, Keys) ->
    ct:log("Check set to adroll ~n"),
    ct:log("state ~p", [mero:state()]),
    [ok = mero:set(Cluster, Key, <<"Adroll">>, 11111, 1000)
        || Key <- Keys],

    ct:log("Delete ! ~n", []),
    ok = mero:mdelete(Cluster, Keys, 1000),

    ct:log("Checking empty keys ~n"),
    [{Key, undefined} = mero:get(Cluster, Key) || Key <- Keys].


mget(Cluster, ClusterAlt, Keys) ->
    [?assertMatch(ok, mero:set(Cluster, Key, Key, 11111, 1000))
        || Key <- Keys],

    io:format("Checking get itself ~n"),
    [?assertMatch({Key, Key}, mero:get(Cluster, Key)) || Key <- Keys],

    Results = mero:mget(Cluster, Keys, 10000),
    ResultsAlt = mero:mget(ClusterAlt, Keys, 10000),
    io:format("Checking mget ~p ~n", [Results]),
    [begin
         ?assertEqual({value, {Key, Key}}, lists:keysearch(Key, 1, Results)),
         ?assertEqual({value, {Key, Key}}, lists:keysearch(Key, 1, ResultsAlt))
     end || Key <- Keys].

mincrement(Cluster = cluster_txt, _ClusterAlt, Keys) ->
    {error, not_supportable} = mero:mincrement_counter(Cluster, Keys);
mincrement(Cluster = cluster_binary, _ClusterAlt, Keys) ->
    ok = mero:mincrement_counter(Cluster, Keys),
    MGetRet = lists:sort(mero:mget(Cluster, Keys)),
    Expected = lists:sort([{K, <<"1">>} || K <- Keys]),
    ?assertMatch(Expected, MGetRet).

increment(Cluster, ClusterAlt, Keys) ->
    io:format("Increment +1 +1 +1 ~n"),

    F = fun(Key, Expected) ->
            IncrementReturn = element(2, mero:increment_counter(Cluster, Key)),
            io:format("Increment return Expected ~p Received ~p~n", [Expected, IncrementReturn]),
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
    io:format("Increment +~p ~p ~n",[Initial, Steps]),

    F = fun(Key, Expected) ->
        IncrementReturn = element(2,
            mero:increment_counter(Cluster, Key, Steps, Initial, 22222, 3, 1000)),
        io:format("Increment return Expected ~p Received ~p~n", [Expected, IncrementReturn]),
        {Key, Value2} = mero:get(Cluster, Key),
        io:format("Checking get ~p ~p ~n", [Cluster, Value2]),
        {Key, Value3} = mero:get(ClusterAlt, Key),
        io:format("Checking get ~p ~p ~n", [ClusterAlt, Value3]),
        ?assertMatch(Expected, IncrementReturn),
        ?assertMatch(IncrementReturn, binary_to_integer(Value2)),
        ?assertMatch(IncrementReturn, binary_to_integer(Value3))
    end,

    [F(Key, Initial) || Key <- Keys],
    [F(Key, (Initial + Steps)) || Key <- Keys],
    [F(Key, (Initial + 2*Steps)) || Key <- Keys].


add(Cluster, ClusterAlt, Keys) ->
    io:format("Attempt to add sucess to 5000 ~n"),
    Expected = <<"5000">>,
    Expected2 = <<"asdf">>,
    [begin
        ?assertMatch(ok, mero:add(Cluster, Key, Expected, 10000, 10000)),
        ?assertMatch({error, not_stored}, mero:add(cluster_txt, Key, Expected2, 10000, 10000)),
        ?assertMatch({error, already_exists}, mero:add(cluster_binary, Key, Expected2, 10000, 10000)),
        {Key, Value} = mero:get(Cluster, Key),
        {Key, Value2} = mero:get(ClusterAlt, Key),
        io:format("Checking get ~p ~p ~n", [Value, Value2]),
        ?assertMatch(Expected, Value),
        ?assertMatch(Expected, Value2)
     end || Key <- Keys].


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
         ?assertEqual({error, already_exists}, mero:cas(Cluster, Key, Value2, 10000, 10000, CAS + 1)),
         await_connected(Cluster),
         ?assertEqual(ok, mero:cas(Cluster, Key, Value2, 10000, 10000, CAS)),
         ?assertEqual({error, already_exists}, mero:cas(ClusterAlt, Key, Value2, 10000, 10000, CAS)),
         await_connected(ClusterAlt),
         ?assertEqual({Key, Value2}, mero:get(ClusterAlt, Key)),
         ?assertEqual(ok, mero:set(Cluster, Key, Value3, 10000, 10000)),
         {Key, Value3, NCAS} = mero:gets(Cluster, Key),
         ?assertNotEqual(CAS, NCAS)
     end
     || Key <- Keys].


%% our test server doesn't emulate a real memcached server with 100% accuracy.
mgets(Cluster, _ClusterAlt, Keys) ->
    Expected = lists:keysort(1, [{Key, undefined, undefined}
                                 || Key <- Keys]),
    ?assertEqual(Expected, lists:keysort(1, mero:mgets(Cluster, Keys, 1000))).



%%%=============================================================================
%%% Internal functions
%%%=============================================================================


key() ->
    base64:encode(crypto:strong_rand_bytes(20)).


await_connected(Cluster) ->
    ct:log("waiting for free connections"),
    Wait = fun W () ->
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
