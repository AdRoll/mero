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
-module(mero_config_monitor).

-behaviour(gen_server).

-export([
    start_link/2,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-record(state, {
    cluster_config  :: cluster_config(),
    cluster_defs    :: cluster_defs()
}).

-type state() :: #state{}.

-type cluster_name()    :: atom().
-type cluster_config()  :: mero:cluster_config().
-type cluster_params()  :: [{cluster_name(), SupName :: atom(), mero_cluster:child_definitions()}].
-type init_args()       :: #{config := cluster_config(), params := cluster_params()}.
-type cluster_defs()    :: #{cluster_name() => mero_cluster:child_definitions()}.

%%%-----------------------------------------------------------------------------
%%% API
%%%-----------------------------------------------------------------------------
-spec start_link(cluster_config(), cluster_params()) -> {ok, pid()} | {error, term()}.
start_link(ClusterConfig, ClusterParams) ->
    gen_server:start_link(
        {local, ?MODULE}, ?MODULE, #{config => ClusterConfig, params => ClusterParams}, []).


%%%-----------------------------------------------------------------------------
%%% Interesting Callbacks
%%%-----------------------------------------------------------------------------
-spec init(init_args()) -> {ok, state()}.
init(#{config := ClusterConfig, params := ClusterParams}) ->
    ClusterDefs =
        maps:from_list(
            [{ClusterName, ChildDefs} || {ClusterName, _SupName, ChildDefs} <- ClusterParams]),
    {ok, #state{cluster_config = ClusterConfig, cluster_defs = ClusterDefs}}.

-spec handle_info(heartbeat | _, State) -> {noreply, State} when State :: state().
handle_info(heartbeat, State) ->
    {noreply, State};
handle_info(_, State) -> {noreply, State}.


%%%-----------------------------------------------------------------------------
%%% Boilerplate Callbacks
%%%-----------------------------------------------------------------------------
-spec handle_call(Msg, _From, State) -> {reply, {unknown_call, Msg}, State} when State :: state().
handle_call(Msg, _From, State) -> {reply, {unknown_call, Msg}, State}.

-spec handle_cast(_Msg, State) -> {noreply, State} when State :: state().
handle_cast(_Msg, State) -> {noreply, State}.


%%%-----------------------------------------------------------------------------
%%% Private Functions
%%%-----------------------------------------------------------------------------
