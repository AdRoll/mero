Mero
========

Mero is a scalable and lightweight OTP Erlang client for memcache. Mero allows you to interact with
different clusters, specifying different number of pools per server and sharding algorithms per cluster.

Mero is lightweight because it doesnt use an Erlang process per socket like other typical
pooling libraries. Each pool worker adjusts the number of available sockets based on the demand.
There is no bottleneck selecting the pool because all the workers are created on startup and registered
with a local name which will be parametrized on a function on a dynamic-compile module.

If a connection to the memcached server fails there is a mechanism to delay
connection retries. All the connections are renewed every time interval.

The storage module is configurable so you can use different protocols or even use a storage different
than memcache.

It includes a callback that will be called to notify of specific error events. These events have the
form of:
  {Id :: list(atoms),
   Args :: list([{Key :: cluster_name | host | port | error_reason,
                  Value :: term()}])
  }
 Examples of Ids are:

 - [socket, connect, ok]
 - [socket, connect, error]
 - [socket, send, error]
 - [socket, rcv, error]
 - [socket, controlling_process, error]

Configuration
=============

Look at the mero.app.src to see all the available options.
The sharding algorithms available are shard_phash2 and shard_crc32.

```
  [{cluster_a,
     [{servers, [{"server1", 11211},
                 {"server2", 11211},
                 {"server3", 11211},
                 {"server4", 11211},
                 {"server5", 11211},
                 {"server6", 11211},
                 {"server7", 11211},
                 {"server8", 11211}]},
      {sharding_algorithm, {mero, shard_phash2}}, %% Module and function
      {workers_per_shard, 3},                         %% Number of pools that each server will have
      {pool_worker_module, mero_wrk_tcp_txt}]
  },

  {cluster_b,
     [{servers, [{"localhost", 11211}]},
      {sharding_algorithm, {mero, shard_crc32}},
      {workers_per_shard, 1},
      {pool_worker_module, mero_wrk_tcp_txt}]
  },
  ...
]

```


Using Mero:
===============

```
make

make test

```

There are three ways to start this application:

 - From an erlang shell:
```
erl -pa ebin/ -pa deps/*/ebin/ -s inets

> application:start(mero).
ok

> mero:increment_counter(default, <<"key">>).
{ok,1}

> mero:increment_counter(default, <<"key">>).
{ok,2}

> mero:get(default, <<"key">>).
{<<"key">>,<<"2">>}

> mero:set(default, <<"key">>, <<"5">>, 3600, 5000).
ok

> mero:increment_counter(default, <<"key">>).
{ok,6}

> mero:set(default, <<"key">>, <<"50">>, 3600, 5000).
ok

> mero:increment_counter(default, <<"key">>).
{ok,51}

> mero:increment_counter(default, <<"key2">>).
{ok,1}

> mero:increment_counter(default, <<"key3">>).
{ok,1}

> mero:increment_counter(default, <<"key4">>).
{ok,1}

> mero:mget(default, [<<"key">>, <<"key2">>, <<"key3">>, <<"key4">>], 5000).
[{<<"key">>,<<"51">>},
 {<<"key2">>,<<"1">>},
 {<<"key3">>,<<"1">>},
 {<<"key4">>,<<"1">>}]

> mero:set(default, <<"key">>, <<"key">>, 3600, 5000).
ok

> mero:set(default, <<"key2">>, <<"key2">>, 3600, 5000).
ok

> mero:increment_counter(default, <<"key">>).
{error,incr_decr_on_non_numeric_value}
=ERROR REPORT==== 3-Apr-2015::13:57:40 ===
    error: memcached_request_failed
    client: {client,#Port<0.3562>,undefined,
                    {mero,stat_event_callback,
                              [{cluster_name,default},
                               {host,"localhost"},
                               {port,11211}]}}
    cmd: {5,{<<"key">>,<<"1">>,<<"1">>,<<"86400">>}}
    reason: incr_decr_on_non_numeric_value

> mero:mget(default, [<<"key">>, <<"key2">>, <<"key3">>, <<"key4">>], 5000).
[{<<"key">>,<<"key">>},
 {<<"key2">>,<<"key2">>},
 {<<"key3">>,<<"1">>},
 {<<"key4">>,<<"1">>}]

> mero:flush_all(default).
[{default,ok}]


> mero:mget(default, [<<"key">>, <<"key2">>, <<"key3">>, <<"key4">>], 5000).
[{<<"key">>,undefined},
 {<<"key2">>,undefined},
 {<<"key3">>,undefined},
 {<<"key4">>,undefined}]

> mero:add(default, <<"key">>, <<"value">>, 1000, 5000).
ok

> mero:add(default, <<"key">>, <<"value">>, 1000, 5000).
{error,already_exists}

=ERROR REPORT==== 3-Apr-2015::14:03:25 ===
    error: memcached_request_failed
    client: {client,#Port<0.3558>,undefined,
                    {mero,stat_event_callback,
                              [{cluster_name,default},
                               {host,"localhost"},
                               {port,11211}]}}
    cmd: {2,{<<"key">>,<<"value">>,<<"1000">>}}
    reason: already_exists

```


 - Set the configuration in the mero.app.src file and start the application inside your
OTP node.


 - As an OTP included application, pass the ClusterConfiguration as a parameter to the
supervisor of the application.

```
mero_sup:start_link([{default,[{servers,[{"localhost",11211}]},
                                   {sharding_algorithm,{mero,shard_crc32}},
                                   {workers_per_shard,1},
                                   {pool_worker_module,mero_wrk_tcp_txt}]}]).

```

Testing the library against a local memcached server:
=====================================================

Warning: This will erase all the contents of the memcached server it connects to ("localhost" by default).
Uncomment the test cases at suite test/mero_test_with_local_memcached_SUITE.erl


