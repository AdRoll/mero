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

-export([start_link/5, checkout/2, checkin/1, checkin_closed/1, transaction/3, close/2,
         pool_loop/3, system_continue/3, system_terminate/4]).
%%% Internal & introspection functions
-export([init/6, state/1]).

-include_lib("mero/include/mero.hrl").

-type mfargs() :: {module(), Function :: atom(), Args :: [term()]}.
-type client() :: term().
-type host() :: inet:socket_address() | inet:hostname().

-record(conn,
        {updated :: erlang:timestamp(),
         pool :: module(),
         worker_module :: module(),
         client :: client()}).

-type conn() :: #conn{}.

-record(pool_st,
        {cluster,
         host :: host(),
         port :: inet:port_number(),
         max_connections :: non_neg_integer(),
         min_connections :: non_neg_integer(),
         %% List of free connections
         free :: [term()],
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
         min_connection_interval_ms :: non_neg_integer() | undefined,
         worker_module :: atom(),
         callback_info :: mfargs(),
         pool :: term(),
         last_connection_attempt :: non_neg_integer()}).

-callback transaction(client(), atom(), [term()]) ->
                         {error, term()} | {client(), {ok, any()}}.
-callback close(client(), Reason :: term()) -> _.
-callback connect(host(), inet:port_number(), mfargs()) ->
                     {ok, client()} | {error, term()}.
-callback controlling_process(client(), Parent :: pid()) -> ok | {error, term()}.

%%%=============================================================================
%%% External functions
%%%=============================================================================

start_link(ClusterName, Host, Port, PoolName, WorkerModule) ->
    proc_lib:start_link(?MODULE,
                        init,
                        [self(), ClusterName, Host, Port, PoolName, WorkerModule]).

%% @doc Checks out an element of the pool.
-spec checkout(atom(), TimeLimit :: tuple()) -> {ok, conn()} | {error, Reason :: term()}.
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
-spec checkin(Connection :: conn()) -> ok.
checkin(#conn{pool = PoolName} = Connection) ->
    safe_send(PoolName, {checkin, self(), Connection#conn{updated = os:timestamp()}}),
    ok.

%% @doc Return a connection that has been closed.
-spec checkin_closed(Connection :: conn()) -> ok.
checkin_closed(#conn{pool = PoolName}) ->
    safe_send(PoolName, {checkin_closed, self()}),
    ok.

%% @doc Executes an operation

-spec transaction(Connection :: conn(), atom(), list()) ->
                     {NewConnection :: conn(), {ok, any()}} | {error, any()}.
transaction(#conn{worker_module = WorkerModule, client = Client} = Conn,
            Function,
            Args) ->
    case WorkerModule:transaction(Client, Function, Args) of
        {error, Reason} ->
            {error, Reason};
        {NClient, Res} ->
            {Conn#conn{client = NClient}, Res}
    end.

close(#conn{worker_module = WorkerModule, client = Client}, Reason) ->
    WorkerModule:close(Client, Reason).

system_continue(Parent, Deb, State) ->
    pool_loop(State, Parent, Deb).

-spec system_terminate(term(), _, _, _) -> no_return().
system_terminate(Reason, _Parent, _Deb, _State) ->
    exit(Reason).

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

init(Parent, ClusterName, Host, Port, PoolName, WrkModule) ->
    case is_config_valid(ClusterName) of
        false ->
            proc_lib:init_ack(Parent, {error, invalid_config});
        true ->
            register(PoolName, self()),
            process_flag(trap_exit, true),
            Deb = sys:debug_options([]),
            {Module, Function} = mero_conf:stat_callback(),
            CallBackInfo = ?CALLBACK_CONTEXT(Module, Function, ClusterName, Host, Port),
            Initial = mero_conf:pool_initial_connections(ClusterName),
            spawn_connections(ClusterName, PoolName, WrkModule, Host, Port, CallBackInfo, Initial),
            proc_lib:init_ack(Parent, {ok, self()}),
            State =
                #pool_st{cluster = ClusterName,
                         free = [],
                         host = Host,
                         port = Port,
                         busy = dict:new(),
                         max_connections =
                             0, %%make dialyzer happy. These are populated from config
                         min_connections = 0,
                         num_connected = 0,
                         num_connecting = Initial,
                         num_failed_connecting = 0,
                         reconnect_wait_time = ?RECONNECT_WAIT_TIME,
                         pool = PoolName,
                         callback_info = CallBackInfo,
                         worker_module = WrkModule,
                         last_connection_attempt = 0},
            timer:send_interval(5000, reload_pool_min_max_settings),
            pool_loop(schedule_expiration(reload_pool_min_max_settings(State)), Parent, Deb)
    end.

%%% @doc Returns the specified PoolName state.
-spec state(PoolName :: atom()) -> term().
state(PoolName) ->
    MRef = erlang:monitor(process, PoolName),
    safe_send(PoolName, {state, {self(), MRef}}),
    receive
        {MRef, State} ->
            erlang:demonitor(MRef),
            PoolPid = whereis(PoolName),
            {links, Links} = process_info(PoolPid, links),
            {monitors, Monitors} = process_info(PoolPid, monitors),
            [process_info(PoolPid, message_queue_len),
             {links, length(Links)},
             {monitors, length(Monitors)},
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
            NumConnecting = State#pool_st.num_connecting,
            Connected = State#pool_st.num_connected,
            MaxConns = State#pool_st.max_connections,
            case NumConnecting + Connected > MaxConns of
                true ->
                    ?MODULE:pool_loop(State#pool_st{num_connecting = NumConnecting - 1},
                                      Parent,
                                      Deb);
                false ->
                    spawn_connect(State#pool_st.cluster,
                                  State#pool_st.pool,
                                  State#pool_st.worker_module,
                                  State#pool_st.host,
                                  State#pool_st.port,
                                  State#pool_st.callback_info),
                    ?MODULE:pool_loop(State#pool_st{last_connection_attempt =
                                                        erlang:system_time(millisecond)},
                                      Parent,
                                      Deb)
            end;
        reload_pool_min_max_settings ->
            ?MODULE:pool_loop(reload_pool_min_max_settings(State), Parent, Deb);
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
    give(State, From);
get_connection(State, {Pid, Ref} = _From) ->
    safe_send(Pid, {Ref, {reject, State}}),
    maybe_spawn_connect(State).

maybe_spawn_connect(#pool_st{free = Free,
                             num_connecting = Connecting,
                             num_connected = Connected,
                             max_connections = MaxConn,
                             min_connections = MinConn} =
                        State) ->
    %% Length could be big.. better to not have more than a few dozens of sockets
    %% May be worth to keep track of the length of the free in a counter.
    FreeSockets = length(Free),
    Needed = max(0, calculate_needed(FreeSockets, Connected, Connecting, MaxConn, MinConn)),
    maybe_spawn_connect(State, Needed, erlang:system_time(millisecond)).

%% Do not spawn new connections if
%% - There is no need for new connection
%% - There is minimum interval between connections, and that hasn't elapsed yet since the last
%%   connection
%% - There are in-flight connection attempts
maybe_spawn_connect(State =
                        #pool_st{min_connection_interval_ms = Min,
                                 last_connection_attempt = Last,
                                 num_connecting = Connecting},
                    Needed,
                    Now)
    when Min /= undefined, Now - Last < Min; Connecting > 0; Needed == 0 ->
    State;
maybe_spawn_connect(State =
                        #pool_st{num_failed_connecting = NumFailed,
                                 reconnect_wait_time = WaitTime,
                                 num_connecting = Connecting},
                    _Needed,
                    _Now)
    when NumFailed > 0 ->
    %% Wait before reconnection if more than one successive
    %% connection attempt has failed. Don't open more than
    %% one connection until an attempt has succeeded again.
    erlang:send_after(WaitTime, self(), connect),
    State#pool_st{num_connecting = Connecting + 1};
maybe_spawn_connect(State =
                        #pool_st{num_connecting = Connecting,
                                 pool = Pool,
                                 worker_module = WrkModule,
                                 cluster = ClusterName,
                                 host = Host,
                                 port = Port,
                                 callback_info = CallbackInfo},
                    Needed,
                    Now) ->
    spawn_connections(ClusterName, Pool, WrkModule, Host, Port, CallbackInfo, Needed),
    State#pool_st{num_connecting = Connecting + Needed, last_connection_attempt = Now}.

calculate_needed(FreeSockets, Connected, Connecting, MaxConn, MinConn) ->
    TotalSockets = Connected + Connecting,
    MaxAllowed = MaxConn - TotalSockets,
    IdleSockets = FreeSockets + Connecting,
    case MinConn - IdleSockets of
        MaxNeeded when MaxNeeded > MaxAllowed ->
            MaxAllowed;
        MaxNeeded ->
            MaxNeeded
    end.

connect_success(#pool_st{free = Free,
                         num_connected = Num,
                         num_connecting = NumConnecting} =
                    State,
                Conn) ->
    State#pool_st{free = [Conn | Free],
                  num_connected = Num + 1,
                  num_connecting = NumConnecting - 1,
                  num_failed_connecting = 0}.

connect_failed(#pool_st{num_connecting = Num, num_failed_connecting = NumFailed} =
                   State) ->
    maybe_spawn_connect(State#pool_st{num_connecting = Num - 1,
                                      num_failed_connecting = NumFailed + 1}).

checkin(#pool_st{busy = Busy, free = Free} = State, Pid, Conn) ->
    case dict:find(Pid, Busy) of
        {ok, {MRef, _}} ->
            erlang:demonitor(MRef),
            State#pool_st{busy = dict:erase(Pid, Busy), free = [Conn | Free]};
        error ->
            State
    end.

checkin_closed_pid(#pool_st{busy = Busy, num_connected = Num} = State, Pid) ->
    case dict:find(Pid, Busy) of
        {ok, {MRef, _}} ->
            erlang:demonitor(MRef),
            maybe_spawn_connect(State#pool_st{busy = dict:erase(Pid, Busy),
                                              num_connected = Num - 1});
        error ->
            State
    end.

down(#pool_st{busy = Busy, num_connected = Num, callback_info = CallbackInfo} = State,
     Pid) ->
    case dict:find(Pid, Busy) of
        {ok, {_, Conn}} ->
            catch close(Conn, down),
            ?LOG_EVENT(CallbackInfo, [socket, connect, close]),
            NewState = State#pool_st{busy = dict:erase(Pid, Busy), num_connected = Num - 1},
            maybe_spawn_connect(NewState);
        error ->
            State
    end.

give(#pool_st{free = [Conn | Free], busy = Busy} = State, {Pid, Ref}) ->
    MRef = erlang:monitor(process, Pid),
    safe_send(Pid, {Ref, Conn}),
    State#pool_st{busy = dict:store(Pid, {MRef, Conn}, Busy), free = Free}.

spawn_connect(ClusterName, Pool, WrkModule, Host, Port, CallbackInfo) ->
    MaxConnectionDelayTime = mero_conf:pool_max_connection_delay_time(ClusterName),
    do_spawn_connect(Pool, WrkModule, Host, Port, CallbackInfo, MaxConnectionDelayTime).

do_spawn_connect(Pool, WrkModule, Host, Port, CallbackInfo, SleepTime) ->
    spawn_link(fun() ->
                  case SleepTime > 0 of
                      true ->
                          %% Wait before reconnect
                          timer:sleep(
                              rand:uniform(SleepTime));
                      false -> ignore
                  end,
                  try_connect(Pool, WrkModule, Host, Port, CallbackInfo)
               end).

spawn_connections(ClusterName, Pool, WrkModule, Host, Port, CallbackInfo, 1) ->
    spawn_connect(ClusterName, Pool, WrkModule, Host, Port, CallbackInfo);
spawn_connections(ClusterName, Pool, WrkModule, Host, Port, CallbackInfo, Number)
    when Number > 0 ->
    SleepTime = mero_conf:pool_max_connection_delay_time(ClusterName),
    [do_spawn_connect(Pool, WrkModule, Host, Port, CallbackInfo, SleepTime)
     || _Number <- lists:seq(1, Number)].

try_connect(Pool, WrkModule, Host, Port, CallbackInfo) ->
    case connect(WrkModule, Host, Port, CallbackInfo) of
        {ok, Client} ->
            case controlling_process(WrkModule, Client, whereis(Pool)) of
                ok ->
                    safe_send(Pool,
                              {connect_success,
                               #conn{worker_module = WrkModule,
                                     client = Client,
                                     pool = Pool,
                                     updated = os:timestamp()}});
                {error, Reason} ->
                    safe_send(Pool, connect_failed),
                    WrkModule:close(Client, Reason)
            end;
        {error, _Reason} ->
            safe_send(Pool, connect_failed)
    end.

connect(WrkModule, Host, Port, CallbackInfo) ->
    WrkModule:connect(Host, Port, CallbackInfo).

controlling_process(WrkModule, WrkState, Parent) ->
    WrkModule:controlling_process(WrkState, Parent).

conn_time_to_live(ClusterName) ->
    case mero_conf:pool_connection_unused_max_time(ClusterName) of
        infinity ->
            infinity;
        Milliseconds ->
            Milliseconds * 1000
    end.

schedule_expiration(State = #pool_st{cluster = ClusterName}) ->
    erlang:send_after(
        mero_conf:pool_expiration_interval(ClusterName), self(), expire),
    State.

expire_connections(#pool_st{cluster = ClusterName, free = Conns, num_connected = Num} =
                       State) ->
    Now = os:timestamp(),
    try conn_time_to_live(ClusterName) of
        TTL ->
            case lists:foldl(fun filter_expired/2, {Now, TTL, [], []}, Conns) of
                {_, _, [], _} ->
                    State;
                {_, _, ExpConns, ActConns} ->
                    spawn_link(fun() -> close_connections(ExpConns) end),
                    maybe_spawn_connect(State#pool_st{free = ActConns,
                                                      num_connected = Num - length(ExpConns)})
            end
    catch
        error:badarg ->
            ok
    end.

checkout_cancel(#pool_st{busy = Busy, free = Free} = State, Pid) ->
    case dict:find(Pid, Busy) of
        {ok, {MRef, Conn}} ->
            erlang:demonitor(MRef),
            State#pool_st{busy = dict:erase(Pid, Busy), free = [Conn | Free]};
        error ->
            State
    end.

filter_expired(#conn{updated = Updated} = Conn, {Now, TTL, ExpConns, ActConns}) ->
    case timer:now_diff(Now, Updated) < TTL of
        true ->
            {Now, TTL, ExpConns, [Conn | ActConns]};
        false ->
            {Now, TTL, [Conn | ExpConns], ActConns}
    end.

%% Note: If current # of connections < new min_connections, new ones will be created
%%       next time we call maybe_spawn_connect/1.
%%       If current # of connections > new max_connections,  no action is taken to
%%       close the exceeding ones.  Instead, they won't be re-created once they
%%       terminate by themselves (because of timeouts, errors, inactivity, etc)
reload_pool_min_max_settings(State = #pool_st{cluster = ClusterName}) ->
    State#pool_st{min_connections = mero_conf:pool_min_free_connections(ClusterName),
                  max_connections = mero_conf:pool_max_connections(ClusterName),
                  min_connection_interval_ms = mero_conf:pool_min_connection_interval(ClusterName)}.

safe_send(PoolName, Cmd) ->
    catch PoolName ! Cmd.

close_connections([]) ->
    ok;
close_connections([Conn | Conns]) ->
    catch close(Conn, expire),
    close_connections(Conns).

is_config_valid(ClusterName) ->
    Initial = mero_conf:pool_initial_connections(ClusterName),
    Max = mero_conf:pool_max_connections(ClusterName),
    Min = mero_conf:pool_min_free_connections(ClusterName),
    case Min =< Initial andalso Initial =< Max of
        true ->
            true;
        false ->
            error_logger:error_report([{error, invalid_config},
                                       {min_connections, Min},
                                       {max_connections, Max},
                                       {initial_connections, Initial}]),
            false
    end.
