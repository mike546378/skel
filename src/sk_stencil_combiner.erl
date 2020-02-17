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
%%% NOTE: All neighbourhood sizes should be odd unless you wish uneven distribution 
%%% on either side of each element
%%%
%%% Possible neighbourhood schemes;
%%% {square, Size}           -    A square region of size Size x Size
%%% {rect, SizeX, SizeY}     -    A rectangular region of size SizeX x SizeY
%%% {cross, SizeX, SizeY}    -    A cross region of size SizeX x SizeY (same as rect but
%%%                                 ignoring neighbours not on same axis as the element)
%%% {coords, [{-1,-1},{0,0},{1,1},...]} - Defined list of offset coordinates from 0,0
%%%
%%% @end
%%%----------------------------------------------------------------------------
-module(sk_stencil_combiner).

-export([
         start/2
        ]).

-include("skel.hrl").

%% @doc Starts the tagger, labelling each input so that the order of all 
%% inputs is recorded. 
-spec start(pid(), tuple()) -> 'eos'.
start(NextPid, NumWorkers) ->
    sk_tracer:t(75, self(), {?MODULE, start}, []),
    DMs = loop([],0,[],NumWorkers),
    sk_tracer:t(50, self(), NextPid, {?MODULE, data}, [{output, DMs}]),
    NextPid ! {data, DMs, []},
    NextPid ! {system, eos}.

-spec loop(list(), integer(), list(), integer()) -> 'eos'.
%% @doc Recursively receives input, accumulating the data messages into a single
%% 2-dimentional list which can then have the stencil functions applied to it.
loop(Accum, WorkersReceived, _OrdBuffer, TotalWorkers) when WorkersReceived == TotalWorkers -> combine(Accum, []);
loop(Accum, WorkersReceived, OrdBuffer, TotalWorkers) ->
  %io:format("~lp~n~n", [OrdBuffer]),
  receive
    {sk_stencil_worker, Index, Data} ->
      case Index of
        WorkersReceived ->
          {NewAccum, NewWorkerCount, NewBuffer} = flush_buffer([Data|Accum], WorkersReceived+1, OrdBuffer),
          loop(NewAccum, NewWorkerCount, NewBuffer, TotalWorkers);
        _ ->
          loop(Accum, WorkersReceived, [{Index, Data}|OrdBuffer], TotalWorkers)
        end
  end.

combine([], Accum) -> Accum;
combine(Data, Accum) -> 
  [H|T] = Data,
  combine(T, H++Accum).

%% @doc Iterates the buffer to find next index to add to accumulator
flush_buffer(Data, Index, Buffer) -> flush_buffer(Data, Index, Buffer, []).
flush_buffer(Data, Index, [], []) -> {Data, Index, []};
flush_buffer(Data, Index, [{HIndex, HBuffer}|T], BufferAccum) when Index == HIndex -> 
  NewData = [HBuffer|Data],
  flush_buffer(NewData, Index+1, T ++ BufferAccum, []);
flush_buffer(NewData, Index, [{_HIndex, _HBuffer} = OrdBufferH|T], BufferAccum) ->
  flush_buffer(NewData, Index, T, [OrdBufferH|BufferAccum]);
flush_buffer(NewData, Index, [], BufferAccum) -> {NewData, Index, BufferAccum};
flush_buffer(NewData, Index, [OrdBufferH|T], BufferAccum) -> io:format("ERROR: ~lp~n", [OrdBufferH]).
