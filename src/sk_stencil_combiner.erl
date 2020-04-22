%%%----------------------------------------------------------------------------
%%% @author Michael Reid <mreid@dundee.ac.uk>
%%% @copyright 2012 University of St Andrews (See LICENCE)
%%% @headerfile "skel.hrl"
%%% 
%%% @doc This module contains 'stencil' skeleton partitioning logic.
%%%
%%% The 'stencil' skeleton can partition a 2-dimentional list such that each 
%%% element becomes a sub-list containing all its neighbours in a desired region
%%% determined by the provided neighbourhood scheme. 
%%%
%%% The stencil combiner waits to recieves input from a known number of workers.
%%% Messages are re-ordered as they are recieved to preserve the original workflow
%%% order before being returned to stencil's encompassing workflow.
%%% 
%%% @end
%%%----------------------------------------------------------------------------
-module(sk_stencil_combiner).

-export([
         start/2
        ]).

-include("skel.hrl").

-spec start(pid(), tuple()) -> 'eos'.
%% @doc Starts stencil combiner, recieves
%% and reorders data before sending to NextPID. 
start(NextPid, NumWorkers) ->
    sk_tracer:t(75, self(), {?MODULE, start}, []),
    DMs = loop([],0,maps:new(),NumWorkers),
    sk_tracer:t(50, self(), NextPid, {?MODULE, data}, [{output, DMs}]),
    NextPid ! {data, DMs, []},
    NextPid ! {system, eos}.

-spec loop(list(), integer(), list(), integer()) -> 'eos'.
%% @doc Recursively receives input from workers, accumulating the data messages into a single
%% 2-dimentional list. If an input is recieved out of order, it is added to a buffer map which
%% gets flushed any time the next correct index is recieved.
loop(Accum, WorkersReceived, _OrdBuffer, TotalWorkers) when WorkersReceived == TotalWorkers -> combine(Accum, []);
loop(Accum, WorkersReceived, OrdBuffer, TotalWorkers) ->
  receive
    {sk_stencil_worker, Index, Data} ->
      case Index of
        WorkersReceived ->
          {NewAccum, NewWorkerCount, NewBuffer} = flush_buffer([Data|Accum], WorkersReceived+1, OrdBuffer),
          loop(NewAccum, NewWorkerCount, NewBuffer, TotalWorkers);
        _ ->
          loop(Accum, WorkersReceived, maps:put(Index, Data, OrdBuffer), TotalWorkers)
        end
  end.

combine([], Accum) -> Accum;
combine(Data, Accum) -> 
  [H|T] = Data,
  combine(T, H++Accum).

%% @doc Looks up a buffer map for all sequential values starting at a key index. Returns all
%% found values along with the new Index and buffer map containing any remaining key/value pairs.
flush_buffer(Data, Index, Buffer) -> flush_buffer(Data, Index, Buffer, maps:is_key(Index, Buffer)).
flush_buffer(Data, Index, Buffer, false) -> {Data, Index, Buffer};
flush_buffer(Data, Index, Buffer, true) ->
  KeyValue = maps:get(Index, Buffer), 
  NewData = [KeyValue|Data],
  NewBuffer = maps:remove(Index, Buffer),
  flush_buffer(NewData, Index+1, NewBuffer, maps:is_key(Index+1, Buffer)).

