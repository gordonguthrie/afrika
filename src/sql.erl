%%% @author    Gordon Guthrie
%%% @copyright (C) 2015, Basho
%%% @doc       The API for the africa server
%%%
%%% @end
%%% Created :  2 Sep 2015 by gguthrie@basho.com

-module(sql).

-export([
	 test/1,
	 r/1
	]).


test(run) ->
    Calls = make_calls(),
    [r(X) || X <- Calls],
    ok;
test(dryrun) ->
    Calls = make_calls(),
    io:format("Dumping out a set of calls to africa:~n"),
    io:format("*************************************~n~n"),
    [io:format(" ~p~n~n", [X]) || X <- Calls],
    io:format("*************************************~n~n"),
    ok.

make_calls() ->
    [
     _PrimaryTable   = get_primary(),
     _SecondaryTable = get_secondary(),
     _FirstInsert    = "INSERT INTO keytable "
     ++ "(userid, firstname, lastname, age, account_name) "
     ++ "VALUES ('user1', 'Gordon', 'Guthrie', 52, 'Holiday Money')",
     _SecondInsertA  = get_second_queries("1st Sept 15", 400),
     _SecondInsertB  = get_second_queries("2nd Sept 15", -40),
     _SecondInsertC  = get_second_queries("3rd Sept 15", -20),
     _Query          = "SELECT * FROM details INNER JOIN keytable "
     ++ "ON details.userid = keytable.userid"
    ].

r(String) ->
    africa_srv:run_sql(String).

get_primary() ->
    _Statement = "CREATE TABLE keytable "
	++ "(userid varchar not null, "
	++ "firstname varchar not null, "
	++ "lastname varchar not null, "
	++ "age int, "
	++ "account_name varchar not null, "
	++ "primary key(userid))".

get_secondary() ->
    _Statement = "CREATE TABLE details "
	++ "(userid varchar not null, "
	++ "date int not null, "
	++ "amount float not null, "
	++ "primary key(userid), "
	++ "foreign key(keytable, userid))".

get_second_queries(Date, Amount) when is_list(Date)   andalso
			      is_integer(Amount) ->
    "INSERT INTO details (userid, date, amount) VALUES ('user1', '" 
	++ Date ++ "', " ++ integer_to_list(Amount) ++ ")".
