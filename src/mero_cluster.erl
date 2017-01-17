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

%% @doc: Dynamically compiles a module containing the startup information for
%% each worker pool.
%%
%% Cofiguration example:
%%
%% [{cluster_a,
%%     [{servers, [{" server1", 11211}, {"server2", 11211}]},
%%      {sharding_algorithm, {mero, shard_phash2}},
%%      {workers_per_shard, 3},
%%      {pool_worker_module, mero_wrk_tcp_txt}]
%%  },
%%  {cluster_b,
%%     [{servers, [{"server3", 11211}]},
%%      {sharding_algorithm, {mero, shard_crc32}},
%%      {workers_per_shard, 1},
%%      {pool_worker_module, mero_wrk_tcp_binary}]
%%  }
%% ]
%%
%% Module generated:
%%
%% -module(mero_cluster_util).
%% -export([child_definitions/0,
%%     worker_by_index/3,
%%     cluster_shards/1,
%%     workers_per_shard/1,
%%     clusters/0,
%%     sharding_algorithm/1]).
%%
%%
%% child_definitions() ->
%%
%%     [{cluster_a,"server1",11211,mero_cluster_a_server1_0_0,
%%             mero_wrk_tcp_txt},
%%      {cluster_a,"server1",11211,mero_cluster_a_server1_0_1,
%%             mero_wrk_tcp_txt},
%%      {cluster_a,"server1",11211,mero_cluster_a_server1_0_2,
%%             mero_wrk_tcp_txt},
%%
%%      {cluster_a,"server2",11211,mero_cluster_a_server2_1_0,
%%             mero_wrk_tcp_txt},
%%      {cluster_a,"server2",11211,mero_cluster_a_server2_1_1,
%%             mero_wrk_tcp_txt},
%%      {cluster_a,"server2",11211,mero_cluster_a_server2_1_2,
%%             mero_wrk_tcp_txt},
%%
%%      {cluster_b,"server3",11211,mero_cluster_b_server3_0_0,
%%             mero_wrk_tcp_binary}].
%%
%% worker_by_index(cluster_a, 0, 0) -> mero_cluster_a_server1_0_0;
%% worker_by_index(cluster_a, 0, 1) -> mero_cluster_a_server1_0_1;
%% worker_by_index(cluster_a, 0, 2) -> mero_cluster_a_server1_0_2;
%% worker_by_index(cluster_a, 1, 0) -> mero_cluster_a_server2_1_0;
%% worker_by_index(cluster_a, 1, 1) -> mero_cluster_a_server2_1_1;
%% worker_by_index(cluster_a, 1, 2) -> mero_cluster_a_server2_1_2;
%% worker_by_index(cluster_b, 0, 0) -> mero_cluster_b_server3_0_0.
%%
%% cluster_shards(cluster_a) -> 2;
%% cluster_shards(cluster_b) -> 1.
%%
%% workers_per_shard(cluster_a) -> 3;
%% workers_per_shard(cluster_b) -> 1.
%%
%% pool_worker_module(cluster_a) -> mero_wrk_tcp_txt;
%% pool_worker_module(cluster_b) -> mero_wrk_tcp_binary.
%%
%% sharding_algorithm(cluster_a) -> {mero, shard_phash2};
%% sharding_algorithm(cluster_b) -> {mero, shard_crc32}.
%%
%% clusters() ->
%%     [cluster_a, cluster_b]
%%
-module(mero_cluster).

-author('Miriam Pena <miriam.pena@adroll.com>').

-export([child_definitions/0,
    cluster_shards/1,
    workers_per_shard/1,
    sharding_algorithm/1,
    load_clusters/1,
    total_workers/1,
    shard_identifier/2,
    server/2,
    one_pool_of_each_shard_of_cluster/1,
    group_by_shards/2, group_by_shards/3,
    pool_worker_module/1,
    random_pool_of_shard/2,
    clusters/0]).


-ignore_xref([
    {mero_cluster_util, cluster_shards, 1},
    {mero_cluster_util, workers_per_shard, 1},
    {mero_cluster_util, child_definitions, 0},
    {mero_cluster_util, clusters, 0},
    {mero_cluster_util, sharding_algorithm, 1},
    {mero_cluster_util, pool_worker_module, 1},
    {mero_cluster_util, worker_by_index, 3}]).


%%%===================================================================
%%% API functions
%%%===================================================================

%% @doc: Loads a file with the pool configuration
-spec load_clusters(ClusterConfig :: list({ClusterName :: atom(),
    Config :: list()})) ->
    ok.
load_clusters(ClusterConfig) ->
    WorkerDefs = worker_defs(ClusterConfig),
    DynModuleBegin =
        "-module(mero_cluster_util). \n"
        "-export([child_definitions/0,\n"
        "         worker_by_index/3,\n"
        "         cluster_shards/1,\n"
        "         workers_per_shard/1,\n"
        "         pool_worker_module/1,\n"
        "         clusters/0,\n"
        "         sharding_algorithm/1]).\n\n",
    ModuleStringTotal = lists:flatten(
        [DynModuleBegin,
            child_definitions_function(WorkerDefs),
            worker_by_index_function(WorkerDefs),
            cluster_shards_function(ClusterConfig),
            workers_per_shard_function(ClusterConfig),
            sharding_algorithm_function(ClusterConfig),
            pool_worker_module_function(ClusterConfig),
            clusters_function(ClusterConfig)
        ]),
    {M, B} = dynamic_compile:from_string(ModuleStringTotal),
    {module, mero_cluster_util} = code:load_binary(M, "", B),
    ok.


-spec child_definitions() -> list({}).
child_definitions() ->
    mero_cluster_util:child_definitions().

cluster_shards(Name) ->
    mero_cluster_util:cluster_shards(Name).

workers_per_shard(Name) ->
    mero_cluster_util:workers_per_shard(Name).

pool_worker_module(Name) ->
    mero_cluster_util:pool_worker_module(Name).

clusters() ->
    mero_cluster_util:clusters().

sharding_algorithm(Name) ->
    mero_cluster_util:sharding_algorithm(Name).
%% Selects a worker based on the cluster identifier and the key.
-spec server(Name :: atom(), Key :: binary()) ->
    Server :: atom().
server(Name, Key) ->
    ShardIdentifier = shard_identifier(Name, Key),
    random_pool_of_shard(Name, ShardIdentifier).

-spec group_by_shards(ClusterName :: atom(), Keys :: list(binary())) ->
    [{PoolName ::atom(), Keys :: list(binary())}].
group_by_shards(ClusterName, Keys) ->
    group_by_shards_(ClusterName, Keys, undefined, []).

-spec group_by_shards(ClusterName :: atom(), Items :: list(tuple()), KeyPos :: pos_integer()) ->
    [{PoolName ::atom(), Items :: list(tuple())}].
group_by_shards(ClusterName, Items, KeyPos) ->
    group_by_shards_(ClusterName, Items, KeyPos, []).


one_pool_of_each_shard_of_cluster(ClusterName) ->
    Shards = cluster_shards(ClusterName),
    [mero_cluster_util:worker_by_index(ClusterName, Shard, 0) || Shard <-
        lists:seq(0, Shards -1)].


random_pool_of_shard(Name, ShardIdentifier) ->
    RandomWorker = random_integer(mero_cluster_util:workers_per_shard(Name)),
    mero_cluster_util:worker_by_index(Name, ShardIdentifier, RandomWorker).


total_workers(Name) ->
    mero_cluster_util:cluster_shards(Name) *
        mero_cluster_util:workers_per_shard(Name).

shard_identifier(Name, Key) ->
    {Module, Function} = mero_cluster_util:sharding_algorithm(Name),
    apply(Module, Function, [Key, mero_cluster_util:cluster_shards(Name)]).


%% @doc: Returns an integer between 0 and max -1
-spec random_integer(Max :: integer()) ->
    integer().
random_integer(Max) when Max > 0 ->
    rand:uniform(Max) - 1.



%%%===================================================================
%%% private functions
%%%===================================================================

group_by_shards_(_ClusterName, [], _, Acc) ->
    Acc;
group_by_shards_(ClusterName, [Item | Items], KeyPos, Acc) ->
    Key = case KeyPos of
              undefined ->
                  Item;
              N when is_integer(N), N > 0 ->
                  element(N, Item)
          end,
    Identifier = mero_cluster:shard_identifier(ClusterName, Key),
    case lists:keyfind(Identifier, 1, Acc) of
        false ->
            group_by_shards_(ClusterName, Items, KeyPos, [{Identifier, [Item]} | Acc]);
        {Identifier, List} ->
            group_by_shards_(ClusterName, Items, KeyPos,
                lists:keyreplace(Identifier, 1, Acc, {Identifier, List ++ [Item]}))
    end.

worker_defs(ClusterConfig) ->
    lists:foldl(fun(Cluster, Acc) ->
        Def = get_server_defs(Cluster),
        Acc ++ Def
    end, [], ClusterConfig).

get_server_defs({ClusterName, ClusterConfig}) ->

    Servers = get_config(servers, ClusterConfig),
    WorkerModule = get_config(pool_worker_module, ClusterConfig),
    Workers = get_config(workers_per_shard, ClusterConfig),
    SortedServers = lists:sort(Servers),

    {Elements, _} = lists:foldl(
        fun({Host, Port}, {Acc, ShardSizeAcc}) ->
            Elements =
                [begin
                     WorkerName = worker_name(ClusterName, Host, ReplicationNumber, ShardSizeAcc),
                     {ClusterName, ShardSizeAcc, ReplicationNumber,
                         {ClusterName, Host, Port, WorkerName, WorkerModule}}
                 end || ReplicationNumber <- lists:seq(0, (Workers - 1))],
            {Acc ++ Elements, ShardSizeAcc + 1}
        end, {[], 0}, SortedServers),
    Elements.

cluster_shards_function(ClusterConfig) ->
    lists:foldl(fun
        ({ClusterName, Config}, []) ->
            Servers = length(get_config(servers, Config)),
            [io_lib:format("cluster_shards(~p) -> ~p.\n\n",
                [ClusterName, Servers])];
        ({ClusterNameIn, Config}, Acc) ->
            Servers = length(get_config(servers, Config)),
            [io_lib:format("cluster_shards(~p) -> ~p;\n",
                [ClusterNameIn, Servers]) | Acc]
    end, [], lists:reverse(ClusterConfig)).


workers_per_shard_function(ClusterConfig) ->
    lists:foldl(fun
        ({ClusterName, Config}, []) ->
            WorkersPerServer = get_config(workers_per_shard, Config),
            [io_lib:format("workers_per_shard(~p) -> ~p.\n\n",
                [ClusterName, WorkersPerServer])];
        ({ClusterNameIn, Config}, Acc) ->
            WorkersPerServer = get_config(workers_per_shard, Config),
            [io_lib:format("workers_per_shard(~p) -> ~p;\n",
                [ClusterNameIn, WorkersPerServer]) | Acc]
    end, [], lists:reverse(ClusterConfig)).


sharding_algorithm_function(ClusterConfig) ->
    lists:foldl(fun
        ({ClusterName, Config}, []) ->
            {Module, Function} = get_config(sharding_algorithm, Config),
            [io_lib:format("sharding_algorithm(~p) -> {~p, ~p}.\n\n",
                [ClusterName, Module, Function])];
        ({ClusterNameIn, Config}, Acc) ->
            {Module, Function} = get_config(sharding_algorithm, Config),
            [io_lib:format("sharding_algorithm(~p) -> {~p, ~p};\n",
                [ClusterNameIn, Module, Function]) | Acc]
    end, [], lists:reverse(ClusterConfig)).


pool_worker_module_function(ClusterConfig) ->
    lists:foldl(fun
        ({ClusterName, Config}, []) ->
            Module = get_config(pool_worker_module, Config),
            [io_lib:format("pool_worker_module(~p) -> ~p.\n\n",
                [ClusterName, Module])];
        ({ClusterNameIn, Config}, Acc) ->
            Module = get_config(pool_worker_module, Config),
            [io_lib:format("pool_worker_module(~p) -> ~p;\n",
                [ClusterNameIn, Module]) | Acc]
    end, [], lists:reverse(ClusterConfig)).

clusters_function(ClusterConfig) ->
    Clusters = [ClusterName || {ClusterName, _} <- ClusterConfig],
    io_lib:format("clusters() -> \n ~p.\n\n", [Clusters]).


child_definitions_function(WorkerDefs) ->
    Result = [Args || {_Name, _, _, Args} <- WorkerDefs],
    io_lib:format("child_definitions() -> \n ~p.\n\n", [Result]).


worker_by_index_function(WorkerDefs) ->
    lists:flatten(
        lists:foldr(fun
            ({Name, ShardSizeAcc, ReplicationNumber, {_Name, _Host, _Port, WorkerName, _WorkerModule}}, []) ->
                io_lib:format("worker_by_index(~p, ~p, ~p) -> ~p.\n\n",
                    [Name, ShardSizeAcc, ReplicationNumber, WorkerName]);
            ({Name, ShardSizeAcc, ReplicationNumber, {_Name, _Host, _Port, WorkerName, _WorkerModule}}, Acc) ->
                Clause = io_lib:format("worker_by_index(~p, ~p, ~p) -> ~p;\n",
                    [Name, ShardSizeAcc, ReplicationNumber, WorkerName]),
                [Clause, Acc]
        end, [], WorkerDefs)
    ).

get_config(Type, ClusterConfig) ->
    case proplists:get_value(Type, ClusterConfig) of
        undefined ->
            error({undefined_config, Type, ClusterConfig});
        Value ->
            Value
    end.

%% PoolName :: mero_pool_127.0.0.1_0
worker_name(ClusterName, Host, ReplicationNumber, ShardSizeAcc) ->
    list_to_atom("mero_" ++ atom_to_list(ClusterName) ++ "_" ++ Host ++ "_" ++
        integer_to_list(ShardSizeAcc) ++ "_" ++ integer_to_list(ReplicationNumber)).
