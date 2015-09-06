%%%-------------------------------------------------------------------
%%% @author    Gordon Guthrie
%%% @copyright (C) 2015, Basho
%%% @doc
%%%
%%% @end
%%% Created :  2 Sep 2015 by  gguthrie@basho.com
%%%-------------------------------------------------------------------
-module(afrika_srv).

-behaviour(gen_server).

%% API
-export([start_link/0]).

-export([
	 run_sql/1
	]).

%% debugging API
-export([
	 dump_db/0
	]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-include_lib("riak_ql/include/riak_ql_ddl.hrl").
-include_lib("riak_ql/include/riak_ql_sql.hrl").

-define(SERVER, ?MODULE).

-record(state, {
	  main_table_ddl      = none,
	  secondary_table_ddl = none,
	  'KVs'               = []
	 }).

-define(ACTOR, a).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

dump_db() ->
    gen_server:call(?MODULE, dump_db).    

run_sql(String) when is_list(String) ->
    Toks = riak_ql_lexer:get_tokens(String),
    case riak_ql_parser:parse(Toks) of
    {ok, SQL} ->
	    case SQL of
		#ddl_v1{foreign_bucket = none,
			foreign_key    = none} ->
		    gen_server:call(?MODULE, {create_primary, SQL});
		#ddl_v1{} ->
		    gen_server:call(?MODULE, {create_secondary, SQL});
		#riak_sql_insert_v1{} ->
		    gen_server:call(?MODULE, {insert_data, SQL});
		#riak_sql_v1{} ->
		    gen_server:call(?MODULE, {query_data, SQL})
	    end;
	{error, _} = E ->
	    io:format("Toks were ~p~n- leading to ~p~n", [Toks, E]),
	    E
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    io:format("afrika_srv inited~n"),
    {ok, #state{}}.

handle_call(dump_db, _From, #state{'KVs' = KVs} = State) ->
    PrintFun = fun({K, Map}) ->
		       io:format("Dumping key ~p~n", [K]),
		       DB = dump_map(Map),
		       io:format("DB is ~p~n", [DB])
	       end,
    [PrintFun({K, Map}) || {K, Map} <- KVs],
    {reply, ok, State};
handle_call({create_primary, #ddl_v1{bucket = B} = DDL}, _From, _State) ->
    Reply = io_lib:format("Afrika reset with primary table: ~p~n", 
			  [binary_to_list(B)]),
    {module, _Mod} = riak_ql_ddl_compiler:make_helper_mod(DDL, "/tmp"),
    %% trash the state
    NewState = #state{main_table_ddl = DDL},
    {reply, lists:flatten(Reply), NewState};
handle_call({create_secondary, #ddl_v1{bucket = B} = DDL}, _From, State) ->
    Reply = io_lib:format("Adding secondary table: ~p~n", [binary_to_list(B)]),
    {module, _Mod} = riak_ql_ddl_compiler:make_helper_mod(DDL, "/tmp"),
    NewState = State#state{secondary_table_ddl = DDL},
    {reply, lists:flatten(Reply), NewState};
handle_call({insert_data, #riak_sql_insert_v1{'INSERT INTO' = I,
					      'VALUES'      = V} = _SQL}, 
	    _From, State) ->
    V2 = format_values(I, V),
    {Reply, NewS} = case get_table(I, V2, State) of
			none ->
			    R = io_lib:format("There is no table ~p~n", [I]),
			    {R, State};
			duff_target ->
			    R = io_lib:format("~p is not defined~n", [I]),
			    {R, State};
			{main, Key} ->
			    insert_main(Key, V2, State);
			{secondary, Key} ->
			    insert_secondary(Key, V2, State)
			end,
    {reply, lists:flatten(Reply), NewS};
handle_call({query_data, #riak_sql_v1{'SELECT'     = [["*"]],
				      'FROM'       = SecTable,
				      'WHERE'      = [],
				      'ORDER BY'   = [],
				      'LIMIT'      = [],
				      'INNER JOIN' = MainTable,
				      'ON'         = _YeahWhoCares}},
	    _From, #state{main_table_ddl      = #ddl_v1{bucket = MainTable},
			  secondary_table_ddl = #ddl_v1{bucket = SecTable},
			  'KVs'               = KVs} = State) ->
    Data = [dump_map(Map) || {_K, Map} <- KVs],
    ProcessFun = fun(List) ->
			 Key = {'FOREIGN KEY', binary_to_list(SecTable)},
			 {value, Join, Main} = lists:keytake(Key, 1, List),
			 {_, Rows} = Join,
			 [lists:flatten(Main, tuple_to_list(X)) || X <- Rows]
		 end,
    Response = [ProcessFun(X) || X <- Data],
    Reply = io_lib:format("~p~n", [Response]),
    {reply, Reply, State};
handle_call({query_data, #riak_sql_v1{} = SQL}, _From, State) ->
    Reply = io_lib:format("can't process this select query:~n- ~p~n", [SQL]),
    {reply, Reply, State};
handle_call(Req, _From, State) ->
    Reply = io_lib:format("not handling request ~p~n", [Req]),
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
get_table(_Target, _V, #state{main_table_ddl      = none,
			  secondary_table_ddl = none}) ->
    none;
get_table(Target, V, #state{main_table_ddl = #ddl_v1{bucket    = Target,
						  local_key = K}}) ->
    #key_v1{ast = [#param_v1{name = [Nm]}]} = K,
    Mod = riak_ql_ddl:make_module_name(Target),
    Key = Mod:extract(V, [Nm]),
    {main, Key};
get_table(Target, V, #state{secondary_table_ddl = #ddl_v1{bucket      = Target,
						       foreign_key = FK}}) ->
    Mod = riak_ql_ddl:make_module_name(Target),
    Key = Mod:extract(V, [FK]),
    {secondary, Key};
get_table(Target, _V, #state{} = S) ->
    io:format("Target is ~p~nState is ~p~n", [Target, S]),
    duff_target.
 
insert_main(K, V, #state{main_table_ddl = M,
			 'KVs'          = KVs} = State) ->
    Table = binary_to_list(M#ddl_v1.bucket),
    Mod = riak_ql_ddl:make_module_name(M#ddl_v1.bucket),
    case lists:keysearch(K, 1, KVs) of
	false ->
	    case Mod:validate_obj(V) of
		true ->
		    {ok, Map2} = create_map(top_level, ?ACTOR, {Mod, M}, V),
		    KVs2 = [{K, Map2} | KVs],
		    Reply = io_lib:format("~p inserted into ~p on key ~p~n",
					  [V, Table, K]),
		    {Reply, State#state{'KVs' = KVs2}};
		false ->
		    Reply = io_lib:format("Values ~p don't validate for table ~p~n",
					  [V, Table]),
		    {Reply, State}
	    end;
	{value, {K, _V1}} ->
	    Reply = io_lib:format("Key ~p exists already in ~p- not overwriting~n", 
				  [K, Table]),
	    {Reply, State}
    end.

insert_secondary(K, V, #state{secondary_table_ddl = S,
			      'KVs'               = KVs} = State) ->
    Table = binary_to_list(S#ddl_v1.bucket), 
    ModS = riak_ql_ddl:make_module_name(S#ddl_v1.bucket),
    case lists:keysearch(K, 1, KVs) of
	false ->
	    Reply = io_lib:format("Key ~p doesn't exist~n", [K]),
	    {Reply, State};
	{value, {K, Map1}} ->
	    case ModS:validate_obj(V) of
		true ->
		    {ok, Map2} = create_map(foreign_key, ?ACTOR, {ModS, S}, V),
		    {ok, NewV} = append_fk(?ACTOR, S, Map1, Map2),
		    KVs2 = lists:keyreplace(K, 1, KVs, {K, NewV}),
		    Reply = io_lib:format("~p inserted into ~p on key ~p~n",
		     			  [V, Table, K]),
		    {Reply, State#state{'KVs' = KVs2}};
		false ->
		    Reply = io_lib:format("Values ~p dont validate for table ~p~n",
					  [V, Table]),
		    {Reply, State}
	    end    
    end.

create_map(Level, Actor, {Mod, #ddl_v1{fields    = F,
				local_key = LK}}, Row) ->
    F2 = strip_fields(F, LK),
    MakeUpdateFun = fun(K) ->
			    Val = Mod:extract(Row, K),
			    {update,
			     {make_name(K), riak_dt_lwwreg}, 
			     {assign, Val}}
		    end,
    Updates = [MakeUpdateFun(X) || X <- F2],
    U2 = case Level of
	     top_level   -> Updates;
	     foreign_key -> FK = crypto:hash(md4, term_to_binary({Actor, Row, now()})),
			    [{update, 
			      {primary_key, riak_dt_lwwreg},
			      {assign, base64:encode(FK)}} | Updates]
	 end,
    Map = riak_dt_map:new(),
    riak_dt_map:update({update, U2}, Actor, Map).

make_name([A]) ->
    A;
make_name(_) ->
    exit("you're havin a laff, int'ya?").

strip_fields(Fields, #key_v1{ast = Keys}) ->
    Exclusions = [Nm || #param_v1{name = Nm} <- Keys],
    StripFun = fun(#riak_field_v1{name = Nm}, Acc) ->
		       case lists:member(Nm, Exclusions) of
			   true  -> Acc;
			   false -> [[Nm] | Acc]
		       end
	       end,
    lists:foldl(StripFun, [], Fields).			       

format_values(Target, Values) ->
    Mod = riak_ql_ddl:make_module_name(Target),
    _V = list_to_tuple([format_v2({X, Y}, Mod) || {X, Y} <- Values]).

format_v2({Field, Val}, Mod) ->
    case Mod:get_field_type([Field]) of
	binary -> list_to_binary(Val);
	_      -> Val
    end.

append_fk(Actor, #ddl_v1{bucket = B} = _SDDL, PMap, InsertMap) ->
    Upd = {update, [
		    {update, {{'FOREIGN KEY', binary_to_list(B)}, riak_dt_orswot},
		     {add, InsertMap}}
		   ]},
    riak_dt_map:update(Upd, Actor, PMap).

dump_map(Map) ->
    Raw = lists:reverse(riak_dt_map:value(Map)),
    pretty_print(Raw).

pretty_print(Raw) ->
    PrettyPrFun = fun({{FieldName, riak_dt_orswot}, Set}) ->
			  {FieldName, [list_to_tuple(dump_map(X)) || X <- Set]};
		     ({{FieldName, _}, Val}) ->
			  {FieldName, Val}
		  end,
    lists:map(PrettyPrFun, Raw).
