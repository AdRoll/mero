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
-module(mero_cluster_SUITE).

-author('Miriam Pena <miriam.pena@adroll.com>').

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    all/0,
    load_cluster/1,
    shard_phash2/1,
    shard_crc32/1,
    select_pool/1,
    group_by_shards/1,
    group_by_shards_clustered_key/1
]).

all() -> [
    load_cluster,
    shard_phash2,
    shard_crc32,
    select_pool,
    group_by_shards,
    group_by_shards_clustered_key
].

%% Just tests if the application can be started and when it does that
%% the mero_cluster module is generated correctly.
load_cluster(_Conf) ->
    Config = [
        {cluster, [
            {servers, [
                {"localhost", 11996},
                {"localhost", 11997},
                {"localhost", 11998},
                {"localhost", 11999}
            ]},
            {sharding_algorithm, {mero, shard_phash2}},
            {workers_per_shard, 3},
            {pool_worker_module, mero_wrk_tcp_txt}
        ]},
        {cluster2, [
            {servers, [{"localhost", 11995}]},
            {sharding_algorithm, {mero, shard_crc32}},
            {workers_per_shard, 2},
            {pool_worker_module, mero_wrk_tcp_txt}
        ]}
    ],
    mero_cluster:load_clusters(Config),
    ?assertMatch(4, mero_cluster:cluster_shards(cluster)),
    ?assertMatch(1, mero_cluster:cluster_shards(cluster2)),

    ?assertMatch(3, mero_cluster:workers_per_shard(cluster)),
    ?assertMatch(2, mero_cluster:workers_per_shard(cluster2)),

    ?assertMatch({mero, shard_phash2}, mero_cluster:sharding_algorithm(cluster)),
    ?assertMatch({mero, shard_crc32}, mero_cluster:sharding_algorithm(cluster2)),

    ?assertMatch(12, mero_cluster:total_workers(cluster)),
    ?assertMatch(2, mero_cluster:total_workers(cluster2)),

    ?assertEqual((4 * 3) + 2, length(mero_cluster:child_definitions())),

    ct:log("~p", [mero_cluster:child_definitions()]),
    ?assertMatch(
        [{cluster, "localhost",11996,mero_cluster_localhost_0_0,mero_wrk_tcp_txt},
         {cluster, "localhost",11996,mero_cluster_localhost_0_1,mero_wrk_tcp_txt},
         {cluster, "localhost",11996,mero_cluster_localhost_0_2,mero_wrk_tcp_txt},
         {cluster, "localhost",11997,mero_cluster_localhost_1_0,mero_wrk_tcp_txt},
         {cluster, "localhost",11997,mero_cluster_localhost_1_1,mero_wrk_tcp_txt},
         {cluster, "localhost",11997,mero_cluster_localhost_1_2,mero_wrk_tcp_txt},
         {cluster, "localhost",11998,mero_cluster_localhost_2_0,mero_wrk_tcp_txt},
         {cluster, "localhost",11998,mero_cluster_localhost_2_1,mero_wrk_tcp_txt},
         {cluster, "localhost",11998,mero_cluster_localhost_2_2,mero_wrk_tcp_txt},
         {cluster, "localhost",11999,mero_cluster_localhost_3_0,mero_wrk_tcp_txt},
         {cluster, "localhost",11999,mero_cluster_localhost_3_1,mero_wrk_tcp_txt},
         {cluster, "localhost",11999,mero_cluster_localhost_3_2,mero_wrk_tcp_txt},
         {cluster2, "localhost",11995,mero_cluster2_localhost_0_0,mero_wrk_tcp_txt},
         {cluster2, "localhost",11995,mero_cluster2_localhost_0_1,mero_wrk_tcp_txt}],
        mero_cluster:child_definitions()),
    ok.



shard_phash2(_Conf) ->
    [[begin
            Result = mero:shard_phash2(Key, Shards),
            ?assertEqual(Result, mero:shard_phash2(Key, Shards)),
            ?assert(Result =< Shards)
        end || Shards <- lists:seq(1, 10)]
     || Key <- [<<"Adroll">>, <<"retargetting">>, <<"platform">>]].


shard_crc32(_Conf) ->
    [[begin
          Result = mero:shard_crc32(Key, Shards),
          ?assertEqual(Result, mero:shard_crc32(Key, Shards)),
          ?assert(Result =< Shards)
      end || Shards <- lists:seq(1, 10)]
        || Key <- [<<"Adroll">>, <<"retargetting">>, <<"platform">>]].

select_pool(_Conf) ->
    Config = [
        {cluster,
         [{servers, [{"localhost", 11996}, {"localhost", 11997}]},
          {sharding_algorithm, {mero, shard_phash2}},
          {workers_per_shard, 1},
          {pool_worker_module, mero_wrk_tcp_txt}]},
        {cluster2,
         [{servers, [{"localhost", 11995}, {"localhost", 11998}]},
          {sharding_algorithm, {mero, shard_phash2}},
          {workers_per_shard, 1},
          {pool_worker_module, mero_wrk_tcp_txt}]
        }],
    mero_cluster:load_clusters(Config),
    ct:log("~p", [mero_cluster:child_definitions()]),
    ?assertMatch(
        [{cluster, "localhost", 11996, mero_cluster_localhost_0_0, mero_wrk_tcp_txt},
         {cluster, "localhost", 11997, mero_cluster_localhost_1_0, mero_wrk_tcp_txt},
         {cluster2, "localhost", 11995, mero_cluster2_localhost_0_0, mero_wrk_tcp_txt},
         {cluster2, "localhost", 11998, mero_cluster2_localhost_1_0, mero_wrk_tcp_txt}],
            mero_cluster:child_definitions()),
    ?assertMatch(mero_cluster_localhost_0_0, mero_cluster:server(cluster, <<"Adroll">>)),
    ?assertMatch(mero_cluster2_localhost_0_0, mero_cluster:server(cluster2, <<"Adroll">>)),
    ?assertMatch(mero_cluster_localhost_1_0, mero_cluster:server(cluster, <<"Adroll2">>)),
    ?assertMatch(mero_cluster2_localhost_1_0, mero_cluster:server(cluster2, <<"Adroll2">>)),
    ok.


group_by_shards(_Conf) ->
    Config = [
        {cluster,
            [{servers, [{"localhost", 11996}, {"localhost", 11997}]},
                {sharding_algorithm, {mero, shard_phash2}},
                {workers_per_shard, 1},
                {pool_worker_module, mero_wrk_tcp_txt}]}],
    mero_cluster:load_clusters(Config),
    ?assertEqual([], mero_cluster:group_by_shards(cluster, [])),
    ?assertEqual([
        {0, [<<"6">>, <<"13">>, <<"14">>, <<"15">>, <<"17">>]},
        {1, [<<"1">>,<<"2">>,<<"3">>,<<"4">>,<<"5">>,<<"7">>,
             <<"8">>,<<"9">>, <<"11">>,<<"12">>,<<"16">>,<<"18">>,<<"19">>
        ]}],
        mero_cluster:group_by_shards(cluster,
            [<<"1">>, <<"2">>, <<"3">>,
             <<"4">>, <<"5">>, <<"6">>,
             <<"7">>, <<"8">>, <<"9">>,
             <<"11">>, <<"12">>, <<"13">>,
             <<"14">>, <<"15">>, <<"16">>,
             <<"17">>, <<"18">>, <<"19">>])),
    ?assertEqual([{0, [{x, <<"6">>}, {y, <<"13">>}]},
                  {1, [{a, <<"1">>}, {b, <<"2">>}]}],
                 mero_cluster:group_by_shards(cluster,
                                              [{a, <<"1">>}, {b, <<"2">>},
                                               {x, <<"6">>}, {y, <<"13">>}],
                                              2)),
    ok.

group_by_shards_clustered_key(_Conf) ->
    Config = [
        {cluster,
            [{servers, [{"localhost", 11996}, {"localhost", 11997}]},
                {sharding_algorithm, {mero, shard_phash2}},
                {workers_per_shard, 1},
                {pool_worker_module, mero_wrk_tcp_txt}]}],
    mero_cluster:load_clusters(Config),
    ?assertEqual(
        [
            {0, [<<"K6">>]},
            {1, [<<"K1">>,<<"K2">>,<<"K3">>,<<"K4">>,<<"K5">>,<<"K7">>,<<"K8">>,<<"K9">>]}
        ],
        mero_cluster:group_by_shards(cluster,
            [{<<"1">>, <<"K1">>}, {<<"2">>, <<"K2">>}, {<<"3">>, <<"K3">>},
             {<<"4">>, <<"K4">>}, {<<"5">>, <<"K5">>}, {<<"6">>, <<"K6">>},
             {<<"7">>, <<"K7">>}, {<<"8">>, <<"K8">>}, {<<"9">>, <<"K9">>}])
    ),
    ?assertEqual(
        [{0, [{x, <<"K6">>}, {y, <<"K13">>}]}, {1, [{a, <<"K1">>}, {b, <<"K2">>}]}],
        mero_cluster:group_by_shards(
            cluster,
            [
                {a, {<<"1">>, <<"K1">>}},
                {b, {<<"2">>, <<"K2">>}},
                {x, {<<"6">>, <<"K6">>}},
                {y, {<<"13">>, <<"K13">>}}
            ],
            2
        )
    ),
    ok.
