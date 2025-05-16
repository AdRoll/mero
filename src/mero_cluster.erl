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
%% Configuration example:
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
%% -export([child_definitions/1,
%%     sup_by_cluster_name/1,
%%     worker_by_index/3,
%%     cluster_shards/1,
%%     workers_per_shard/1,
%%     clusters/0,
%%     sharding_algorithm/1]).
%%
%%
%% child_definitions(cluster_a) ->
%%     [{"server1",11211,mero_cluster_a_server1_0_0,
%%             mero_wrk_tcp_txt},
%%      {"server1",11211,mero_cluster_a_server1_0_1,
%%             mero_wrk_tcp_txt},
%%      {"server1",11211,mero_cluster_a_server1_0_2,
%%             mero_wrk_tcp_txt},
%%
%%      {"server2",11211,mero_cluster_a_server2_1_0,
%%             mero_wrk_tcp_txt},
%%      {"server2",11211,mero_cluster_a_server2_1_1,
%%             mero_wrk_tcp_txt},
%%      {"server2",11211,mero_cluster_a_server2_1_2,
%%             mero_wrk_tcp_txt}];
%% child_definitions(cluster_b) ->
%%     [{"server3",11211,mero_cluster_b_server3_0_0,
%%             mero_wrk_tcp_binary}].
%%
%% sup_by_cluster_name(cluster_a) -> mero_cluster_a_sup;
%% sup_by_cluster_name(cluster_b) -> mero_cluster_b_sup.
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

-export([child_definitions/1, sup_by_cluster_name/1, cluster_shards/1,
         workers_per_shard/1, sharding_algorithm/1, load_clusters/1, total_workers/1, server/2,
         one_pool_of_each_shard_of_cluster/1, group_by_shards/2, group_by_shards/3,
         pool_worker_module/1, random_pool_of_shard/2, clusters/0, version/0, purge/0]).

-ignore_xref([{mero_cluster_util, cluster_shards, 1},
              {mero_cluster_util, workers_per_shard, 1},
              {mero_cluster_util, child_definitions, 1},
              {mero_cluster_util, sup_by_cluster_name, 1},
              {mero_cluster_util, clusters, 0},
              {mero_cluster_util, sharding_algorithm, 1},
              {mero_cluster_util, pool_worker_module, 1},
              {mero_cluster_util, worker_by_index, 3},
              {mero_cluster_util, module_info, 1}]).

%% OTP 27 improved dialyzer and now it's complaining
%% about functions of a dynamic compiled module.
-if(?OTP_RELEASE >= 27).

-dialyzer({[no_unknown],
           [child_definitions/1,
            sup_by_cluster_name/1,
            cluster_shards/1,
            workers_per_shard/1,
            pool_worker_module/1,
            sharding_algorithm/1,
            one_pool_of_each_shard_of_cluster/1,
            random_pool_of_shard/2,
            total_workers/1,
            shard_identifier/2,
            version/0,
            clusters/0]}).

- else .

-endif.

-type child_definitions() ::
    [{Host :: string(),
      Port :: pos_integer(),
      WorkerName :: atom(),
      WorkerModule :: module()}].

-export_type([child_definitions/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

%% @doc: Loads a file with the pool configuration
-spec load_clusters(mero:cluster_config()) -> ok.
load_clusters(ClusterConfig) ->
    WorkerDefs = worker_defs(ClusterConfig),
    DynModuleBegin =
        "-module(mero_cluster_util). \n-export([child_definitions/1,\n "
        "        sup_by_cluster_name/1,\n         worker_by_index/3,\n "
        "        cluster_shards/1,\n         workers_per_shard/1,\n "
        "        pool_worker_module/1,\n         clusters/0,\n      "
        "   sharding_algorithm/1]).\n\n",
    ModuleStringTotal =
        lists:flatten([DynModuleBegin,
                       child_definitions_function(WorkerDefs),
                       sup_by_cluster_name_function(WorkerDefs),
                       worker_by_index_function(WorkerDefs),
                       cluster_shards_function(ClusterConfig),
                       workers_per_shard_function(ClusterConfig),
                       sharding_algorithm_function(ClusterConfig),
                       pool_worker_module_function(ClusterConfig),
                       clusters_function(ClusterConfig)]),
    {M, B} = dynamic_compile:from_string(ModuleStringTotal),
    {module, mero_cluster_util} = code:load_binary(M, "", B),
    ok.

%% @doc: Returns the current version of mero_cluster_util
-spec version() -> pos_integer().
version() ->
    [Version] = [Vsn || {vsn, [Vsn]} <- mero_cluster_util:module_info(attributes)],
    Version.

%% @doc: Purges old mero_cluster_util code from the system
-spec purge() -> ok.
purge() ->
    case code:purge(mero_cluster_util) of
        false ->
            ok;
        true ->
            error_logger:warning_msg("Some processes were killed while purging mero_cluster_util"),
            ok
    end.

-spec child_definitions(ClusterName :: atom()) -> child_definitions().
child_definitions(ClusterName) ->
    mero_cluster_util:child_definitions(ClusterName).

-spec sup_by_cluster_name(atom()) -> atom().
sup_by_cluster_name(ClusterName) ->
    mero_cluster_util:sup_by_cluster_name(ClusterName).

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
-spec server(Name :: atom(), Key :: mero:mero_key()) ->
                {ok, Server :: atom()} | {error, term()}.
server(Name, Key) ->
    try
        ShardIdentifier = shard_identifier(Name, Key),
        {ok, random_pool_of_shard(Name, ShardIdentifier)}
    catch
        Exception:Reason ->
            {error, {Exception, Reason}}
    end.

-spec group_by_shards(ClusterName :: atom(), Keys :: [mero:mero_key()]) ->
                         [{PoolName :: atom(), Keys :: [binary()]}].
group_by_shards(ClusterName, Keys) ->
    group_by_shards_(ClusterName, Keys, undefined, []).

-spec group_by_shards(ClusterName :: atom(),
                      Items :: [tuple()],
                      KeyPos :: pos_integer()) ->
                         [{PoolName :: atom(), Items :: [tuple()]}].
group_by_shards(ClusterName, Items, KeyPos) ->
    group_by_shards_(ClusterName, Items, KeyPos, []).

one_pool_of_each_shard_of_cluster(ClusterName) ->
    Shards = cluster_shards(ClusterName),
    [mero_cluster_util:worker_by_index(ClusterName, Shard, 0)
     || Shard <- lists:seq(0, Shards - 1)].

random_pool_of_shard(Name, ShardIdentifier) ->
    RandomWorker = random_integer(mero_cluster_util:workers_per_shard(Name)),
    mero_cluster_util:worker_by_index(Name, ShardIdentifier, RandomWorker).

total_workers(Name) ->
    mero_cluster_util:cluster_shards(Name) * mero_cluster_util:workers_per_shard(Name).

%% @doc: Returns an integer between 0 and max -1
-spec random_integer(Max :: integer()) -> integer().
random_integer(Max) when Max > 0 ->
    rand:uniform(Max) - 1.

%%%===================================================================
%%% private functions
%%%===================================================================

shard_identifier(Name, Key) ->
    {Module, Function} = mero_cluster_util:sharding_algorithm(Name),
    apply(Module,
          Function,
          [mero:clustering_key(Key), mero_cluster_util:cluster_shards(Name)]).

key_to_storage_key(undefined, Key, Key) ->
    mero:storage_key(Key);
key_to_storage_key(KeyPos, Item, Key) ->
    erlang:setelement(KeyPos, Item, mero:storage_key(Key)).

group_by_shards_(_ClusterName, [], _, Acc) ->
    Acc;
group_by_shards_(ClusterName, [Item | Items], KeyPos, Acc) ->
    Key = case KeyPos of
              undefined ->
                  Item;
              N when is_integer(N), N > 0 ->
                  element(N, Item)
          end,
    Identifier = shard_identifier(ClusterName, Key),
    Item2 = key_to_storage_key(KeyPos, Item, Key),
    case lists:keyfind(Identifier, 1, Acc) of
        false ->
            group_by_shards_(ClusterName, Items, KeyPos, [{Identifier, [Item2]} | Acc]);
        {Identifier, List} ->
            group_by_shards_(ClusterName,
                             Items,
                             KeyPos,
                             lists:keyreplace(Identifier, 1, Acc, {Identifier, List ++ [Item2]}))
    end.

worker_defs(ClusterConfig) ->
    lists:foldl(fun(Cluster, Acc) ->
                   Def = get_server_defs(Cluster),
                   Acc ++ Def
                end,
                [],
                ClusterConfig).

get_server_defs({ClusterName, ClusterConfig}) ->
    Servers = get_config(servers, ClusterConfig),
    WorkerModule = get_config(pool_worker_module, ClusterConfig),
    Workers = get_config(workers_per_shard, ClusterConfig),
    SortedServers = lists:sort(Servers),

    {Elements, _} =
        lists:foldl(fun({Host, Port}, {Acc, ShardSizeAcc}) ->
                       Elements =
                           lists:map(fun(ReplicationNumber) ->
                                        WorkerName =
                                            worker_name(ClusterName,
                                                        Host,
                                                        ReplicationNumber,
                                                        ShardSizeAcc),
                                        {ClusterName,
                                         ShardSizeAcc,
                                         ReplicationNumber,
                                         {ClusterName, Host, Port, WorkerName, WorkerModule}}
                                     end,
                                     lists:seq(0, Workers - 1)),
                       {Acc ++ Elements, ShardSizeAcc + 1}
                    end,
                    {[], 0},
                    SortedServers),
    Elements.

cluster_shards_function(ClusterConfig) ->
    lists:foldl(fun ({ClusterName, Config}, []) ->
                        Servers = length(get_config(servers, Config)),
                        [io_lib:format("cluster_shards(~p) -> ~p.\n\n", [ClusterName, Servers])];
                    ({ClusterNameIn, Config}, Acc) ->
                        Servers = length(get_config(servers, Config)),
                        [io_lib:format("cluster_shards(~p) -> ~p;\n", [ClusterNameIn, Servers])
                         | Acc]
                end,
                [],
                lists:reverse(ClusterConfig)).

workers_per_shard_function(ClusterConfig) ->
    lists:foldl(fun ({ClusterName, Config}, []) ->
                        WorkersPerServer = get_config(workers_per_shard, Config),
                        [io_lib:format("workers_per_shard(~p) -> ~p.\n\n",
                                       [ClusterName, WorkersPerServer])];
                    ({ClusterNameIn, Config}, Acc) ->
                        WorkersPerServer = get_config(workers_per_shard, Config),
                        [io_lib:format("workers_per_shard(~p) -> ~p;\n",
                                       [ClusterNameIn, WorkersPerServer])
                         | Acc]
                end,
                [],
                lists:reverse(ClusterConfig)).

sharding_algorithm_function(ClusterConfig) ->
    lists:foldl(fun ({ClusterName, Config}, []) ->
                        {Module, Function} = get_config(sharding_algorithm, Config),
                        [io_lib:format("sharding_algorithm(~p) -> {~p, ~p}.\n\n",
                                       [ClusterName, Module, Function])];
                    ({ClusterNameIn, Config}, Acc) ->
                        {Module, Function} = get_config(sharding_algorithm, Config),
                        [io_lib:format("sharding_algorithm(~p) -> {~p, ~p};\n",
                                       [ClusterNameIn, Module, Function])
                         | Acc]
                end,
                [],
                lists:reverse(ClusterConfig)).

pool_worker_module_function(ClusterConfig) ->
    lists:foldl(fun ({ClusterName, Config}, []) ->
                        Module = get_config(pool_worker_module, Config),
                        [io_lib:format("pool_worker_module(~p) -> ~p.\n\n", [ClusterName, Module])];
                    ({ClusterNameIn, Config}, Acc) ->
                        Module = get_config(pool_worker_module, Config),
                        [io_lib:format("pool_worker_module(~p) -> ~p;\n", [ClusterNameIn, Module])
                         | Acc]
                end,
                [],
                lists:reverse(ClusterConfig)).

clusters_function(ClusterConfig) ->
    Clusters = [ClusterName || {ClusterName, _} <- ClusterConfig],
    io_lib:format("clusters() -> \n ~p.\n\n", [Clusters]).

child_definitions_function(WorkerDefs) ->
    AllDefs = [Args || {_Name, _, _, Args} <- WorkerDefs],
    Clusters = lists:usort([Cluster || {Cluster, _Host, _Port, _Name, _Module} <- AllDefs]),
    [io_lib:format("child_definitions(~p) ->\n ~p;\n\n",
                   [Cluster, [{H, P, N, M} || {C, H, P, N, M} <- AllDefs, C == Cluster]])
     || Cluster <- Clusters]
    ++ "child_definitions(_) ->\n [].\n\n".

sup_by_cluster_name_function(WorkerDefs) ->
    AllDefs = [Args || {_Name, _, _, Args} <- WorkerDefs],
    Clusters = lists:usort([Cluster || {Cluster, _Host, _Port, _Name, _Module} <- AllDefs]),
    [io_lib:format("sup_by_cluster_name(~p) ->\n mero_~p_sup;\n\n", [Cluster, Cluster])
     || Cluster <- Clusters]
    ++ "sup_by_cluster_name(_) ->\n undefined.\n\n".

worker_by_index_function(WorkerDefs) ->
    lists:foldr(fun (WorkerDef, []) ->
                        worker_by_index_clause(WorkerDef, ".");
                    (WorkerDef, Acc) ->
                        [worker_by_index_clause(WorkerDef, ";"), Acc]
                end,
                [],
                WorkerDefs).

worker_by_index_clause({Name,
                        ShardSizeAcc,
                        ReplicationNumber,
                        {_Name, _Host, _Port, WorkerName, _WorkerModule}},
                       Separator) ->
    io_lib:format("worker_by_index(~p, ~p, ~p) -> ~p~s\n\n",
                  [Name, ShardSizeAcc, ReplicationNumber, WorkerName, Separator]).

get_config(Type, ClusterConfig) ->
    case proplists:get_value(Type, ClusterConfig) of
        undefined ->
            error({undefined_config, Type, ClusterConfig});
        Value ->
            Value
    end.

%% PoolName :: mero_pool_127.0.0.1_0
worker_name(ClusterName, Host, ReplicationNumber, ShardSizeAcc) ->
    list_to_atom("mero_"
                 ++ atom_to_list(ClusterName)
                 ++ "_"
                 ++ Host
                 ++ "_"
                 ++ integer_to_list(ShardSizeAcc)
                 ++ "_"
                 ++ integer_to_list(ReplicationNumber)).
