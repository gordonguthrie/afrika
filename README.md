# afrika
yet another riak

yeah, a bit rubbish

`./rebar compile`
`cd ebin/`
`erl`

Then in the Erlang shell

`code:add_patha("../deps/riak_ql/ebin").`
`code:add_path("../deps/riak_dt/ebin").`

You can dump some commands with:
`sql:test(dryrun).`

The copy and paste the string into:

`sql:r/1`

Or run:

`sql:test(run).`
