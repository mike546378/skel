-module(sk_stencil).

-export([
        make/2
        ]).

-include("skel.hrl").

-ifdef(TEST).
-compile(export_all).
-endif.

-spec make(workflow(), pos_integer()) -> maker_fun().

make(WorkFlow, NSize) when is_integer(NSize) ->
    fun(NextPid) ->
        WorkerPid = sk_utils:start_worker(WorkFlow, NextPid),
        spawn(sk_stencil_partitioner, start, [WorkerPid, {square, NSize}])
    end;

make(WorkFlow, TNeighbourhood) ->
    fun(NextPid) ->
        WorkerPid = sk_utils:start_worker(WorkFlow, NextPid),
        spawn(sk_stencil_partitioner, start, [WorkerPid, TNeighbourhood])
    end.