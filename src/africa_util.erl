%%% @author    Gordon Guthrie
%%% @copyright (C) 2015, basho.com
%%% @doc
%%%
%%% @end
%%% Created : 21 Aug 2015 by gguthrie@basho.com

-module(africa_util).

-export([
	 add_paths/0,
	 testing/0
	 ]).

-include_lib("riak_ql/include/riak_ql_ddl.hrl").

add_paths() ->
        Paths = [
	     "/home/vagrant/africa/deps/mochiweb/ebin",
	     "/home/vagrant/africa/deps/riak_dt/ebin",
	     "/home/vagrant/africa/deps/riak_ql/ebin"
	     ],
    [code:add_patha(X) || X <- Paths],
    reloader:start(),
    ok.

testing() ->
    io:format("in test~n"),
    {PDDL, PMod}  = get_primary_ddl(),
    {SDDL, SMod} = get_secondary_ddl(),
    Actor = a,
    {ok, Map1} = create_map(top_level, Actor, {PMod, PDDL}, 
			   {"user1", "gordon", "guthrie", 52, "holiday account"}),
    Vals1 = read_map(Map1),
    io:format("Vals1 is ~p~n", [Vals1]),
    {ok, Map2} = create_map(foreign_key, Actor, {SMod, SDDL}, {"user1", 12345, 3.2}),
    Vals2 = read_map(Map2),
    io:format("Vals2 is ~p~n", [Vals2]),
    {ok, Map3} = append_fk(Actor, SDDL, Map1, Map2),
    Vals3 = read_map(Map3),
    io:format("Vals3 is ~p~n", [Vals3]),
    {ok, Map4} = create_map(foreign_key, Actor, {SMod, SDDL}, {"user1", 12346, 33.4}),
    Vals4 = read_map(Map4),
    io:format("Vals4 is ~p~n", [Vals4]),
    {ok, Map5} = append_fk(Actor, SDDL, Map3, Map4),
    Vals5 = read_map(Map5),
    io:format("Vals5 is ~p~n", [Vals5]),
    ok.

append_fk(Actor, #ddl_v1{bucket = B} = _SDDL, PMap, InsertMap) ->
    io:format("Bucket is ~p~n", [B]),
    Update = {update, [
		       {update, {{'FOREIGN KEY', binary_to_list(B)}, riak_dt_orswot},
			{add, InsertMap}}
		      ]},
    riak_dt_map:update(Update, Actor, PMap).
		       
read_map(Map) ->
    io:format("in map ~p~n", [Map]),
    Raw = lists:reverse(riak_dt_map:value(Map)),
    pretty_print(Raw).

pretty_print(Raw) ->
    PrettyPrFun = fun({{FieldName, riak_dt_orswot}, Set}) ->
			  {FieldName, [list_to_tuple(read_map(X)) || X <- Set]};
		     ({{FieldName, _}, Val}) ->
			  {FieldName, Val}
		  end,
    lists:map(PrettyPrFun, Raw).

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
	++ "date int not null, "
	++ "amount float not null, "
	++ "primary key(userid))",
    get_ddl(Statement).
    
get_ddl(Statement) ->
    Toks = riak_ql_lexer:get_tokens(Statement),
    {ok, DDL} = riak_ql_parser:parse(Toks),
    {module, Mod} = riak_ql_ddl_compiler:make_helper_mod(DDL, "/tmp"),
    {DDL, Mod}.

