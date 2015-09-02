%%%-------------------------------------------------------------------
%%% @author    Gordon Guthrie
%%% @copyright (C) 2015, Basho
%%% @doc
%%%
%%% @end
%%% Created :  2 Sep 2015 by  gguthrie@basho.com
%%%-------------------------------------------------------------------
-module(africa_srv).

-behaviour(gen_server).

%% API
-export([start_link/0]).

-export([
	 run_sql/1
	]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-include_lib("riak_ql/include/riak_ql_ddl.hrl").
-include_lib("riak_ql/include/riak_ql_sql.hrl").

-define(SERVER, ?MODULE).

-record(state, {main_table_ddl      = [],
		secondary_table_ddl = [],
		can_take_queries    = [],
		'KVs'               = []}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

run_sql(String) when is_list(String) ->
    Toks = riak_ql_lexer:get_tokens(String),
    {ok, Return} = riak_ql_parser:parse(Toks),
    case Return of
	#ddl_v1{foreign_bucket = none,
		foreign_key    = none} ->
	    create_primary(Return);
	#ddl_v1{} ->
	    create_secondary(Return);
	#riak_sql_insert_v1{} ->
	    insert_data(Return);
	#riak_sql_v1{} ->
	    query_data(Return)
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    io:format("africa_srv inited~n"),
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

create_primary(Rec) ->
    io:format("Creating primary with ~p~n", [Rec]),
    ok.

create_secondary(Rec) ->
    io:format("Creating secondary with ~p~n", [Rec]),
    ok.

insert_data(Rec) ->
    io:format("inserting ~p~n", [Rec]),
    ok.

query_data(Rec) ->
    io:format("querying with ~p~n", [Rec]),
    ok.

