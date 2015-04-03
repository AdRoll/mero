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
-module(mero_pool).

-author('Miriam Pena <miriam.pena@adroll.com>').

-export([start_link/5,
         checkout/2,
         checkin/1,
         checkin_closed/1,
         transaction/3,
         close/1,
         pool_loop/3,
         system_continue/3,
         system_terminate/4]).

%%% Internal & introspection functions
-export([init/6,
         state/1]).

-include_lib("mero/include/mero.hrl").

-record(conn, {updated :: erlang:timestamp(),
               pool :: module(),
               worker_module :: module(),
               client :: term()}).

-record(pool_st, {cluster,
                  host,
                  port,
                  max_connections,
                  min_connections,

                  %% List of free connections
                  free :: list(term()),

                  %% Busy connections (pid -> #conn)
                  busy :: dict:dict(),

                  %% Number of connections established (busy + free)
                  num_connected :: non_neg_integer(),

                  %% Number of connection attempts in progress
                  num_connecting :: non_neg_integer(),

                  %% Number of failed connection attempts
                  %% (reset to zero when connect attempt succeds)
                  num_failed_connecting :: non_neg_integer(),

                  reconnect_wait_time :: non_neg_integer(),
                  worker_module :: atom(),
                  callback_info :: {module(), Function :: atom()},
                  pool :: term()}).

%%%=============================================================================
%%% External functions
%%%=============================================================================

start_link(ClusterName, Host, Port, PoolName, WorkerModule) ->
    proc_lib:start_link(?MODULE, init, [self(), ClusterName, Host, Port, PoolName, WorkerModule]).

%% @doc Checks out an element of the pool.
-spec checkout(atom(), TimeLimit :: tuple()) ->
                      {ok, #conn{}} | {error, Reason :: term()}.
checkout(PoolName, TimeLimit) ->
    Timeout = mero_conf:millis_to(TimeLimit),
    MRef = erlang:monitor(process, PoolName),
    safe_send(PoolName, {checkout, {self(), MRef}}),
    receive
         {'DOWN', MRef, _, _, _} ->
                {error, down};
         {MRef, {reject, _State}} ->
                erlang:demonitor(MRef),
                {error, reject};
         {MRef, Connection} ->
                erlang:demonitor(MRef),
                {ok, Connection}
         after Timeout ->
                erlang:demonitor(MRef),
                safe_send(PoolName, {checkout_cancel, self()}),
                {error, pool_timeout}
    end.


%% @doc Return a  connection to specfied pool updating its timestamp
-spec checkin(Connection :: #conn{}) -> ok.
checkin(#conn{pool = PoolName} = Connection) ->
    safe_send(PoolName, {checkin, self(),
                         Connection#conn{updated = os:timestamp()}}),
    ok.


%% @doc Return a connection that has been closed.
-spec checkin_closed(Connection :: #conn{}) -> ok.
checkin_closed(#conn{pool = PoolName}) ->
    safe_send(PoolName, {checkin_closed, self()}),
    ok.


%% @doc Executes an operation

-spec transaction(Connection :: #conn{}, atom(), list()) ->
                         {NewConnection :: #conn{}, {ok, any()}} | {error, any()}.
transaction(#conn{worker_module = WorkerModule,
                  client = Client} = Conn, Function, Args) ->
    case WorkerModule:transaction(Client, Function, Args) of
        {error, Reason} -> {error, Reason};
        {NClient, Res} ->
            {Conn#conn{client = NClient}, Res}
    end.


close(#conn{worker_module = WorkerModule,
            client = Client}) ->
    WorkerModule:close(Client).


system_continue(Parent, Deb, State) ->
    pool_loop(State, Parent, Deb).


system_terminate(Reason, _Parent, _Deb, _State) ->
    exit(Reason).


%%%=============================================================================
%%% Internal functions
%%%=============================================================================

init(Parent, ClusterName, Host, Port, PoolName, WrkModule) ->
    case is_config_valid() of
        false ->
            proc_lib:init_ack(Parent, {error, invalid_config});
        true ->
            register(PoolName, self()),
            process_flag(trap_exit, true),
            Deb = sys:debug_options([]),
            {Module, Function} = mero_conf:stat_event_callback(),
            CallBackInfo = ?CALLBACK_CONTEXT(Module, Function, ClusterName, Host, Port),
            Initial = mero_conf:initial_connections_per_pool(),
            spawn_connections(PoolName, WrkModule, Host, Port, CallBackInfo,
                              Initial),
            proc_lib:init_ack(Parent, {ok, self()}),
            State = #pool_st{
                       cluster = ClusterName,
                       free = [],
                       host = Host,
                       port = Port,
                       min_connections =
                           mero_conf:min_free_connections_per_pool(),
                       max_connections =
                           mero_conf:max_connections_per_pool(),
                       busy = dict:new(),
                       num_connected = 0,
                       num_connecting = Initial,
                       num_failed_connecting = 0,
                       reconnect_wait_time = ?RECONNECT_WAIT_TIME,
                       pool = PoolName,
                       callback_info = CallBackInfo,
                       worker_module = WrkModule},
            pool_loop(schedule_expiration(State), Parent, Deb)
    end.


%%% @doc Returns the specified PoolName state.
-spec state(PoolName :: atom()) -> term().
state(PoolName) ->
    MRef = erlang:monitor(process, PoolName),
    safe_send(PoolName, {state, {self(), MRef}}),
    receive
        {MRef, State} ->
            erlang:demonitor(MRef),
            [process_info(whereis(PoolName), message_queue_len),
             {free, length(State#pool_st.free)},
             {num_connected, State#pool_st.num_connected},
             {num_connecting, State#pool_st.num_connecting},
             {num_failed_connecting, State#pool_st.num_failed_connecting}];
        {'DOWN', MRef, _, _, _} ->
            {error, down}
    after ?DEFAULT_TIMEOUT ->
            erlang:demonitor(MRef),
            {error, timeout}
    end.


%%%=============================================================================
%%% Internal functions
%%%=============================================================================

pool_loop(State, Parent, Deb) ->
    receive
        {connect_success, Conn} ->
            ?MODULE:pool_loop(connect_success(State, Conn), Parent, Deb);
        connect_failed ->
            ?MODULE:pool_loop(connect_failed(State), Parent, Deb);
        connect ->
            spawn_connect(State#pool_st.pool,
                          State#pool_st.worker_module,
                          State#pool_st.host,
                          State#pool_st.port,
                          State#pool_st.callback_info),
            ?MODULE:pool_loop(State, Parent, Deb);
        {checkout, From} ->
            ?MODULE:pool_loop(get_connection(State, From), Parent, Deb);
        {checkin, Pid, Conn} ->
            ?MODULE:pool_loop(checkin(State, Pid, Conn), Parent, Deb);
        {checkin_closed, Pid} ->
            ?MODULE:pool_loop(checkin_closed_pid(State, Pid), Parent, Deb);
        {checkout_cancel, Pid} ->
            ?MODULE:pool_loop(checkout_cancel(State, Pid), Parent, Deb);
        expire ->
            ?MODULE:pool_loop(schedule_expiration(expire_connections(State)), Parent, Deb);
        {state, {Pid, Ref}} ->
            safe_send(Pid, {Ref, State}),
            ?MODULE:pool_loop(State, Parent, Deb);
        {'DOWN', _, _, Pid, _} ->
            ?MODULE:pool_loop(down(State, Pid), Parent, Deb);
        {'EXIT', Parent, Reason} ->
            exit(Reason);
        %% Assume exit signal from connecting process
        {'EXIT', _, Reason} when Reason /= normal ->
            ?MODULE:pool_loop(connect_failed(State), Parent, Deb);
        {system, From, Msg} ->
            sys:handle_system_msg(Msg, From, Parent, ?MODULE, Deb, State);
        _ ->
            ?MODULE:pool_loop(State, Parent, Deb)
    end.


get_connection(#pool_st{free = Free} = State, From) when Free /= [] ->
    maybe_spawn_connect(give(State, From));

get_connection(State, {Pid, Ref} = _From) ->
    safe_send(Pid, {Ref, {reject, State}}),
    State.


maybe_spawn_connect(#pool_st{
                       free = Free,
                       num_connected = Connected,
                       max_connections = MaxConn,
                       min_connections = MinConn,
                       num_connecting = Connecting,
                       num_failed_connecting = NumFailed,
                       reconnect_wait_time = WaitTime,
                       worker_module = WrkModule,
                       callback_info = CallbackInfo,
                       pool = Pool,
                       host = Host,
                       port = Port} = State) ->
    %% Length could be big.. better to not have more than a few dozens of sockets
    %% May be worth to keep track of the length of the free in a counter.
    MaxAllowed = MaxConn - (Connected + Connecting),
    Needed = case MinConn - (length(Free) + Connecting) of
                 MaxNeeded when MaxNeeded > MaxAllowed ->
                     MaxAllowed;
                 MaxNeeded ->
                     MaxNeeded
             end,
    if
        %% Need sockets and no failed connections are reported..
        %% we create new ones
        (Needed > 0), NumFailed =< 1 ->
            spawn_connections(Pool, WrkModule, Host, Port, CallbackInfo, Needed),
            State#pool_st{num_connecting = Connecting + Needed};

        %% Wait before reconnection if more than one successive
        %% connection attempt has failed. Don't open more than
        %% one connection until an attempt has succeeded again.
        (Needed > 0), Connecting == 0 ->
            reconnect_after_wait(WaitTime),
            State#pool_st{num_connecting = Connecting + 1};

        %% We dont need sockets or we have failed connections
        %% we wait before reconnecting.
        true ->
            State
    end.


connect_success(#pool_st{free = Free,
                         num_connected = Num,
                         num_connecting = NumConnecting,
                         num_failed_connecting = NumFailed} = State,
                Conn) ->
    NState = State#pool_st{free = [Conn|Free],
                           num_connected = Num + 1,
                           num_connecting = NumConnecting - 1,
                           num_failed_connecting = 0},
    case (NumFailed > 0) of
        true -> maybe_spawn_connect(NState);
        false -> NState
    end.


connect_failed(#pool_st{num_connecting = Num,
                        num_failed_connecting = NumFailed} = State) ->
    maybe_spawn_connect(State#pool_st{num_connecting = Num - 1,
                                      num_failed_connecting = NumFailed + 1}).


checkin(#pool_st{busy = Busy, free = Free} = State, Pid, Conn) ->
    case dict:find(Pid, Busy) of
        {ok, {MRef, _}} ->
            erlang:demonitor(MRef),
            State#pool_st{busy = dict:erase(Pid, Busy),
                          free = [Conn|Free]};
        error ->
            State
    end.


checkin_closed_pid(#pool_st{busy = Busy, num_connected = Num} = State, Pid) ->
    case dict:find(Pid, Busy) of
        {ok, {MRef, _}} ->
            erlang:demonitor(MRef),
            maybe_spawn_connect(State#pool_st{busy = dict:erase(Pid, Busy),
                                              num_connected = Num - 1
                                             });
        error ->
            State
    end.


down(#pool_st{busy = Busy, num_connected = Num} = State, Pid) ->
    case dict:find(Pid, Busy) of
        {ok, {_, Conn}} ->
            catch close(Conn),
            NewState = State#pool_st{busy = dict:erase(Pid, Busy),
                                     num_connected = Num - 1},
            maybe_spawn_connect(NewState);
        error ->
            State
    end.


give(#pool_st{free = [Conn|Free],
              busy = Busy} = State, {Pid, Ref}) ->
    MRef = erlang:monitor(process, Pid),
    safe_send(Pid, {Ref, Conn}),
    State#pool_st{busy = dict:store(Pid, {MRef, Conn}, Busy), free = Free}.


reconnect_after_wait(WaitTime) ->
    erlang:send_after(WaitTime, self(), connect).


%% connect inmediately
spawn_connect(Pool, WrkModule, Host, Port, CallbackInfo) ->
    spawn_connect(Pool, WrkModule, Host, Port, CallbackInfo, 0).

spawn_connect(Pool, WrkModule, Host, Port, CallbackInfo, SleepTime) ->
    spawn_link(fun() ->
                       case (SleepTime > 0) of
                           true ->
                               %% Wait before reconnect
                               random:seed(os:timestamp()),
                               timer:sleep(random:uniform(SleepTime));
                           false ->
                               ignore
                       end,
                       try_connect(Pool, WrkModule, Host, Port, CallbackInfo)
               end).


%% Connect with delay
spawn_connections(Pool, WrkModule, Host, Port, CallbackInfo, 1) ->
    spawn_connect(Pool, WrkModule, Host, Port, CallbackInfo);
spawn_connections(Pool, WrkModule, Host, Port, CallbackInfo, Number) when (Number > 0) ->
    SleepTime = mero_conf:max_connection_delay_time(),
    [ begin
          spawn_connect(Pool, WrkModule, Host, Port, CallbackInfo, SleepTime)
      end || _Number <- lists:seq(1, Number)].


try_connect(Pool, WrkModule, Host, Port, CallbackInfo) ->
    case connect(WrkModule, Host, Port, CallbackInfo) of
        {ok, Client} ->
            case controlling_process(WrkModule, Client, whereis(Pool)) of
                ok ->
                    safe_send(Pool, {connect_success, #conn{worker_module = WrkModule,
                                                            client = Client,
                                                            pool = Pool,
                                                            updated = os:timestamp()
                                                           }});
                {error, _Reason} ->
                    safe_send(Pool, connect_failed)
            end;
        {error, _Reason} ->
            safe_send(Pool, connect_failed)
    end.


connect(WrkModule, Host, Port, CallbackInfo) ->
    WrkModule:connect(Host, Port, CallbackInfo).

controlling_process(WrkModule, WrkState, Parent) ->
    WrkModule:controlling_process(WrkState, Parent).


conn_time_to_live(_Pool) ->
    case mero_conf:connection_unused_max_time() of
        infinity -> infinity;
        Milliseconds -> Milliseconds * 1000
    end.


schedule_expiration(State) ->
    erlang:send_after(mero_conf:expiration_interval(), self(), expire),
    State.


expire_connections(#pool_st{free = Conns, pool = Pool, num_connected = Num} = State) ->
    Now = os:timestamp(),
    try conn_time_to_live(Pool) of
        TTL ->
            case lists:foldl(fun filter_expired/2, {Now, TTL, [], []}, Conns) of
                {_, _, [], _} -> State;
                {_, _, ExpConns, ActConns} ->
                    spawn_link(fun() -> close_connections(ExpConns) end),
                    maybe_spawn_connect(
                      State#pool_st{free = ActConns,
                                    num_connected = Num - length(ExpConns)})
            end
    catch
        error:badarg -> ok
    end.


checkout_cancel(#pool_st{busy = Busy, free = Free} = State, Pid) ->
    case dict:find(Pid, Busy) of
        {ok, {MRef, Conn}} ->
            erlang:demonitor(MRef),
            State#pool_st{busy = dict:erase(Pid, Busy),
                          free = [Conn|Free]};
        error ->
            State
    end.


filter_expired(#conn{updated = Updated} = Conn, {Now, TTL, ExpConns, ActConns}) ->
    case timer:now_diff(Now, Updated) < TTL of
        true -> {Now, TTL, ExpConns, [Conn | ActConns]};
        false -> {Now, TTL, [Conn | ExpConns], ActConns}
    end.


safe_send(PoolName, Cmd) ->
    catch PoolName ! Cmd.

close_connections([]) -> ok;

close_connections([Conn | Conns]) ->
    catch close(Conn),
    close_connections(Conns).

is_config_valid() ->
    Initial = mero_conf:initial_connections_per_pool(),
    Max = mero_conf:max_connections_per_pool(),
    Min = mero_conf:min_free_connections_per_pool(),
    case (Min =< Initial) andalso (Initial =< Max) of
        true ->
            true;
        false ->
            error_logger:error_report(
              [{error, invalid_config},
               {min_connections, Min},
               {max_connections, Max},
               {initial_connections, Initial}]),
            false
    end.
