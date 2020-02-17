-module(sk_stencil).

-export([
        make/3
        ]).

-include("skel.hrl").

-ifdef(TEST).
-compile(export_all).
-endif.

-spec make(function(), pos_integer(), pos_integer()) -> maker_fun().

make(WorkerFun, NumWorkers, NSize) when is_integer(NSize) ->
    fun(NextPid) ->
        Combiner = spawn(sk_stencil_combiner, start, [NextPid, NumWorkers]),
        spawn(sk_stencil_partitioner, start, [Combiner, WorkerFun, NumWorkers, {square, NSize}])
    end;

make(WorkerFun, NumWorkers, TNeighbourhood) ->
    fun(NextPid) ->
        Combiner = spawn(sk_stencil_combiner, start, [NextPid, NumWorkers]),
        spawn(sk_stencil_partitioner, start, [Combiner, WorkerFun, NumWorkers, TNeighbourhood])
    end.
