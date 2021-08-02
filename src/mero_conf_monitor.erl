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
-module(mero_conf_monitor).

-behaviour(gen_server).

-export([start_link/1, init/1, handle_call/3, handle_cast/2, handle_info/2,
         handle_continue/2]).

-record(state,
        {orig_config :: cluster_config(),
         processed_config :: cluster_config(),
         cluster_version :: undefined | pos_integer()}).

-type state() :: #state{}.
-type cluster_config() :: mero:cluster_config().
-type init_args() :: #{orig_config := cluster_config()}.

%%%-----------------------------------------------------------------------------
%%% API
%%%-----------------------------------------------------------------------------
-spec start_link(cluster_config()) -> {ok, pid()} | {error, term()}.
start_link(OrigConfig) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, #{orig_config => OrigConfig}, []).

%%%-----------------------------------------------------------------------------
%%% Interesting Callbacks
%%%-----------------------------------------------------------------------------
-spec init(init_args()) -> {ok, state(), {continue, reload}}.
init(#{orig_config := OrigConfig}) ->
    {ok,
     #state{orig_config = OrigConfig,
            processed_config = empty_config(OrigConfig),
            cluster_version = undefined},
     {continue, reload}}.

-spec handle_continue(reload | program_heartbeat, state()) ->
                         {noreply, state(), {continue, program_heartbeat}} |
                         {noreply, state(), {continue, idle}} |
                         {noreply, state()}.
handle_continue(reload, State) ->
    NewState =
        try
            update_cluster_defs(State)
        catch
            Kind:Desc:Stack ->
                error_logger:error_report([{error, mero_config_heartbeat_failed},
                                           {kind, Kind},
                                           {desc, Desc},
                                           {stack, Stack},
                                           {orig_config, State#state.orig_config},
                                           {processed_config, State#state.processed_config}]),
                State
        end,
    {noreply, NewState, {continue, program_heartbeat}};
handle_continue(program_heartbeat, State) ->
    program_heartbeat(),
    {noreply, State, {continue, idle}};
handle_continue(idle, State) ->
    {noreply, State}.

-spec handle_info(heartbeat | _, State) -> {noreply, State} when State :: state().
handle_info(heartbeat, State) ->
    {noreply, State, {continue, reload}};
handle_info(_, State) ->
    {noreply, State}.

%%%-----------------------------------------------------------------------------
%%% Boilerplate Callbacks
%%%-----------------------------------------------------------------------------
-spec handle_call(Msg, _From, State) -> {reply, {unknown_call, Msg}, State}
    when State :: state().
handle_call(Msg, _From, State) ->
    {reply, {unknown_call, Msg}, State}.

-spec handle_cast(_Msg, State) -> {noreply, State} when State :: state().
handle_cast(_Msg, State) ->
    {noreply, State}.

%%%-----------------------------------------------------------------------------
%%% Private Functions
%%%-----------------------------------------------------------------------------
empty_config(OrigConfig) ->
    [{C, [{servers, []}]} || {C, _} <- OrigConfig].

program_heartbeat() ->
    erlang:send_after(
        mero_conf:monitor_heartbeat_delay(), self(), heartbeat).

update_cluster_defs(#state{orig_config = OrigConfig} = State) ->
    update_cluster_defs(mero_conf:process_server_specs(OrigConfig), State).

update_cluster_defs(ProcessedConfig,
                    #state{processed_config = ProcessedConfig} = State) ->
    State; %% Nothing has changed
update_cluster_defs(NewProcessedConfig, State) ->
    #state{processed_config = OldProcessedConfig, cluster_version = OldClusterVersion} =
        State,
    ok = mero_cluster:load_clusters(NewProcessedConfig),
    NewClusterVersion = mero_cluster:version(),

    ok = update_clusters(OldProcessedConfig, NewProcessedConfig),

    ok = purge_if_version_changed(OldClusterVersion, NewClusterVersion),

    State#state{processed_config = NewProcessedConfig, cluster_version = NewClusterVersion}.

purge_if_version_changed(ClusterVersion, ClusterVersion) ->
    ok;
purge_if_version_changed(_OldVersion, _NewClusterVersion) ->
    mero_cluster:purge().

%% NOTE: since both cluster definitions are generated through mero_conf:process_server_specs/1
%%       with the same input, we can be sure that the resulting lists will contain the same number
%%       of elements, with the same keys in the same order.
update_clusters([], []) ->
    ok;
update_clusters([ClusterDef | OldClusterDefs],
                [ClusterDef | NewClusterDefs]) -> %% nothing changed
    update_clusters(OldClusterDefs, NewClusterDefs);
update_clusters([{ClusterName, OldAttrs} | OldClusterDefs],
                [{ClusterName, NewAttrs} | NewClusterDefs]) ->
    OldServers =
        lists:sort(
            proplists:get_value(servers, OldAttrs)),
    ok =
        case lists:sort(
                 proplists:get_value(servers, NewAttrs))
        of
            OldServers ->
                ok; %% Nothing of relevance changed
            _ ->
                mero_sup:restart_child(ClusterName)
        end,
    update_clusters(OldClusterDefs, NewClusterDefs).
