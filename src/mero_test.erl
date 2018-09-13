-module(mero_test).

-export([test/0]).


test() ->
    _ = application:start(inets),
    {ok,_} = application:ensure_all_started(dynamic_compile),
    application:load(mero),
    ok = mero_conf:cluster_config(
        [{cluster_udp,
            [{servers, [{"localhost", 11211}]},
             {sharding_algorithm, {mero, shard_phash2}},
             {workers_per_shard, 3},
             %{pool_worker_module,mero_wrk_tcp_binary}]}]),
             {pool_worker_module, mero_wrk_udp_binary}]},
        {cluster_tcp,
            [{servers, [{"localhost", 11211}]},
             {sharding_algorithm, {mero, shard_phash2}},
             {workers_per_shard, 3},
             {pool_worker_module,mero_wrk_tcp_binary}]}
        ]),
    ok = mero_conf:initial_connections_per_pool(10),
    ok = mero_conf:min_free_connections_per_pool(1),
    ok = mero_conf:expiration_interval(3000),
    ok = mero_conf:max_connections_per_pool(10 + 10),
    ok = mero_conf:connection_unused_max_time(10000),
    ok = mero_conf:timeout_read(1000),
    ok = mero_conf:timeout_write(1000),
    _ = application:ensure_all_started(mero),
    timer:sleep(1000),


    ok = mero:set(cluster_tcp, <<"key2">>, <<"This is the data">>, 0, 1000),
    ok = mero:set(cluster_tcp, <<"key3">>, iolist_to_binary([<<"This is the data">> || _ <- lists:seq(1,10000)]), 0, 1000),

    Keys = [<<"key1">>, <<"key2">>, <<"key3">>],


    %TCP = lists:map(fun(Key) ->
    %                    mero:get(cluster_tcp, Key, 1000)
    %              end, Keys),
    %UDP = lists:map(fun(Key) ->
    %                    mero:get(cluster_udp, Key, 1000)
    %              end, Keys),
    %TCP = UDP,

    io:format("mget: ~p\n", [mero:mget(cluster_udp, Keys, 1000)]),
    ok.



