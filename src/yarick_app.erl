-module(yarick_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    Paths = [
	     "/home/vagrant/yarick/deps/mochiweb/ebin",
	     "/home/vagrant/yarick/deps/riak_dt/ebin",
	     "/home/vagrant/yarick/deps/riak_ql/ebin"
	     ],
    [code:add_patha(X) || X <- Paths],
    reloader:start(),
    yarick_sup:start_link().

stop(_State) ->
    ok.
