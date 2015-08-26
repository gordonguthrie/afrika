%%% @author    Gordon Guthrie
%%% @copyright (C) 2015, basho.com
%%% @doc
%%%
%%% @end
%%% Created : 21 Aug 2015 by gguthrie@basho.com

-module(africa).

-export([
	 add_path/0,
	 testing/0
	 ]).

-include_lib("riak_ql/include/riak_ql_ddl.hrl").

add_paths() ->
        Paths = [
	     "/home/vagrant/yarick/deps/mochiweb/ebin",
	     "/home/vagrant/yarick/deps/riak_dt/ebin",
	     "/home/vagrant/yarick/deps/riak_ql/ebin"
	     ],
    [code:add_patha(X) || X <- Paths],
    reloader:start(),
    ok.

testing() ->
    io:format("in test~n"),
    {PDDL,     PMod}  = get_primary_ddl(),
    {_SDDLMod, _SMod} = get_secondary_ddl(),
    Map = create_map(PMod, PDDL, {"user1", "gordon", "guthrie", 52, "holiday account"}),
    io:format("Map is ~p~n", [Map]),
    ok.

create_map(Mod, #ddl_v1{fields    = F,
			local_key = LK}, Row) ->
    F2 = strip_fields(F, LK),
    io:format("Row is ~p~n", [Row]),
    io:format("F2 is ~p~n", [F2]),
    Map = riak_dt_map:new(),
    AddFieldFun = fun(K, Mp) ->
			  io:format("K is ~p~n", [K]),
			  Val = Mod:extract(Row, K),
			  io:format("Val is ~p~n", [Val]),
			  Op = bip,
			  Actor = yerp,
			  riak_dt_map:update(Op, Actor, Mp)
		  end,
    _NewMap = lists:foldl(AddFieldFun, Map, F2).

strip_fields(Fields, #key_v1{ast = Keys}) ->
    Exclusions = [Nm || #param_v1{name = Nm} <- Keys],
    StripFun = fun(#riak_field_v1{name = Nm}, Acc) ->
		       case lists:member(Nm, Exclusions) of
			   true  -> Acc;
			   false -> [[Nm] | Acc]
		       end
	       end,
    lists:foldl(StripFun, [], Fields).			       

get_primary_ddl() ->
    Statement = "CREATE TABLE keytable "
	++ "(userid varchar not null, "
	++ "firstname varchar not null, "
	++ "lastname varchar not null, "
	++ "age int, "
	++ "account_name varchar not null, "
	++ "primary key(userid))",
    get_ddl(Statement).

get_secondary_ddl() ->
    Statement = "CREATE TABLE details "
	++ "(userid varchar not null, "
	++ "auto varchar not null, "
	++ "date int not null, "
	++ "amount float not null, "
	++ "primary key(userid, auto))",
    get_ddl(Statement).
    
get_ddl(Statement) ->
    Toks = riak_ql_lexer:get_tokens(Statement),
    {ok, DDL} = riak_ql_parser:parse(Toks),
    {module, Mod} = riak_ql_ddl_compiler:make_helper_mod(DDL, "/tmp"),
    {DDL, Mod}.


