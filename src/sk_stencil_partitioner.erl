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
%%% The partitioner recieves all data messages from a prior workflow, spawns
%%% the desired number of workers requested by the user then batches and 
%%% distributes the data message rows based on the input stencil partitioning
%%% scheme.
%%% 
%%% @end
%%% 
%%%----------------------------------------------------------------------------

-module(sk_stencil_partitioner).

-export([
         start/4
        ]).

-include("skel.hrl").

%% @doc Starts the stencil partitioner, generating and distributing 
%% batches of data to all workers
-spec start(pid(), function(), integer(), tuple()) -> 'eos'.
start(CombinerPID, WorkerFun, NumWorkers, TNeighbourhood) ->
    sk_tracer:t(75, self(), {?MODULE, start}, []),
    DMs = loop([]),
    WorkerLen = math:ceil(length(DMs)/NumWorkers),
    WorkerPids = spawnWorkers(NumWorkers, CombinerPID, WorkerFun, {0, WorkerLen}, TNeighbourhood, []),
    NeighbourhoodHeight = neighbourhood_height(TNeighbourhood),
    Windows = generate_windows(NumWorkers, WorkerLen, NeighbourhoodHeight),
    emitter(DMs,Windows, WorkerPids),
    io:format("sk_stencil_partitioner finished~n").

-spec loop(list()) -> 'eos'.
%% @doc Recursively receives input, accumulating the data messages into a single
%% 2-dimentional list which can then have the stencil functions applied to it.
loop(Accum) ->
  receive
    {data, Value, _Idx} ->
      loop([Value|Accum]);
    {system, eos} ->
        [H|_T] = Accum,
        case length(Accum) of
            1 -> H;
            _ -> lists:reverse(Accum)
        end
  end.

%% @doc Spawns N number of sk_stencil_worker processes, returning their PID's as a list
spawnWorkers(NumWorkers, CombinerPID, WorkerFun, {Index, WorkerLen} = _Identifier, TNeighbourhood, Accum) when NumWorkers >= 1 ->
    WorkerPID = spawn(sk_stencil_worker, start, [CombinerPID, Index, {Index*WorkerLen, WorkerLen}, TNeighbourhood, WorkerFun]),
    spawnWorkers(NumWorkers-1, CombinerPID, WorkerFun, {Index+1, WorkerLen}, TNeighbourhood, [WorkerPID|Accum]);
spawnWorkers(_,_,_,_,_,Accum) -> lists:reverse(Accum).

%% @doc Generalising neighbourhood schemes, reducing number of overflow functions later on,
%% used to init a new stencil
neighbourhood_height({square, Offset}) -> Offset;
neighbourhood_height({rect, _OffsetX, OffsetY}) -> OffsetY;
neighbourhood_height({cross, Offset}) -> Offset;
neighbourhood_height({cross, _OffsetX, OffsetY}) -> OffsetY;
neighbourhood_height({coords, _CoordList}) -> 3. %@todo Implement coordList height iteration

%% @doc Generates overlapping start and stop positions for data windows to be send to procs
generate_windows(WorkerCount, WorkerLen, NeighbourhoodHeight) -> 
    generate_windows(WorkerCount, WorkerLen, NeighbourhoodHeight, {[], []}, 0).
generate_windows(WCount, WLen, NHeight, {Start, Stop} = _Windows, Pos) when WCount >= 1 ->
    StartPos = Pos*WLen-((NHeight-1)/2),
    StopPos = Pos*WLen+(WLen)+((NHeight-1)/2),
    generate_windows(WCount-1, WLen, NHeight, {[StartPos|Start], [StopPos|Stop]}, Pos+1);
generate_windows(_,_,_,{StartPositions, StopPositions},_)->
    {lists:reverse(StartPositions), lists:reverse(StopPositions)}.

-spec emitter(list(), tuple(), list()) -> 'eos'.
%% @doc splits data messages into windowed messages and sends to worker processes.
%% Each recursion either marks a new PID as active, removes a PID from the active 
%% pool, or sends the current row to all active worker PIDs
emitter(DMs, {StartPositions, StopPositions}, WorkerPids) ->
    emitter(DMs, StartPositions, StopPositions, WorkerPids, [], 0).
emitter([DHead|DTail], [StartH|StartT], StopPositions, [WPidH|WPidT], ActivePids, CurrentPos) 
    when CurrentPos >= StartH,
        ActivePids == [] ->
    emitter([DHead|DTail], StartT, StopPositions, WPidT, [WPidH], CurrentPos);
emitter([DHead|DTail], [StartH|StartT], StopPositions, [WPidH|WPidT], ActivePids, CurrentPos) 
    when CurrentPos >= StartH ->
    emitter([DHead|DTail], StartT, StopPositions, WPidT,ActivePids++[WPidH], CurrentPos);
emitter([DHead|DTail], StartPositions, [StopH|StopT], WorkerPids, [ActivePidH|ActivePidT], CurrentPos) 
    when CurrentPos >= StopH ->
    broadcast({system, eos}, [ActivePidH]),
    emitter([DHead|DTail], StartPositions, StopT, WorkerPids, ActivePidT, CurrentPos);
emitter(Data, _StartPositions, _StopPositions, WorkerPids, ActivePids, _CurrentPos)
    when Data == [] ->
    broadcast({system, eos}, ActivePids),
    broadcast({system, eos}, WorkerPids);
emitter([DHead|DTail], StartPositions, StopPositions, WorkerPids, ActivePids, CurrentPos) ->
    broadcast({data, DHead}, ActivePids),
    emitter(DTail, StartPositions, StopPositions, WorkerPids, ActivePids, CurrentPos+1).

%% @doc broadcasts a data message to all Pid's in provided list
broadcast(Data, [PidH|PidT]) ->
    sk_tracer:t(75, self(), PidH, {?MODULE, data}, [{output, Data}]),
    PidH ! Data,
    broadcast(Data, PidT);
broadcast(_,[]) -> ok.