%%% @author  <vagrant@vagrant-ubuntu-trusty-64>
%%% @copyright (C) 2015, 
%%% @doc
%%%
%%% @end
%%% Created : 21 Aug 2015 by  <vagrant@vagrant-ubuntu-trusty-64>

-module(gubbins).

-export([
	 testing/0
	 ]).

-include_lib("riak_ql/include/riak_ql_ddl.hrl").

testing() ->
    io:format("in test~n"),
    DDL = get_ddl(),
    Map = create_map(DDL),
    io:format("Map is ~p~n", [Map]),
    ok.

create_map(#ddl_v1{fields    = F,
		   local_key = LK}) ->
    io:format(user, "Fields is ~p~nLK is ~p~n", [F, LK]),
    erko.

get_ddl() ->
    Statement = "CREATE TABLE keytable "
	++ "(userid varchar not null, "
	++ "firstname varchar not null, "
	++ "lastname varchar not null, "
	++ "age int, "
	++ "primary key(userid))",
    Toks = riak_ql_lexer:get_tokens(Statement),
    io:format(user, "Toks is ~p~n", [Toks]),
    {ok, DDL} = riak_ql_parser:parse(Toks),
    DDL.


