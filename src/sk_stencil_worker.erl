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
%%% The worker contains all neighbourhood generation logic and applies the
%%% stencil function across all nodes in the window range. Also contains helper
%%% functions for handling 2D data via the arrays module.
%%%
%%% @end
%%%----------------------------------------------------------------------------
-module(sk_stencil_worker).

-export([
         start/5
        ]).

-include("skel.hrl").

-spec start(pid(), integer(), tuple(), tuple(), function()) -> 'eos'.
%% @doc Starts the stencil worker, recieving all data messages, finding neighbourhoods
%% and applying worker function to them.
start(CombinerPID, Index, {StartPos, RowCount}, TNeighbourhood, WorkerFun) ->
    sk_tracer:t(75, self(), {?MODULE, start}, []),
    DMs = loop([]),
    case StartPos of
        0.0 -> 
            StencilData = new_stencil(DMs, 0, RowCount, TNeighbourhood);
        _ -> 
            StencilData = new_stencil(DMs, Index/StartPos, RowCount, TNeighbourhood)
    end,
    NewData = apply_func_2d(StencilData, WorkerFun),
    sk_tracer:t(75, self(), CombinerPID, {?MODULE, data}, [{output, {sk_stencil_worker, Index, NewData}}]),
    CombinerPID ! {sk_stencil_worker, Index, NewData}.

-spec loop(list()) -> list().
%% @doc Recursively receives input, accumulating the data messages into a single 2-dimentional list.
loop(Accum) ->
  receive
    {data, Value} ->
      loop([Value|Accum]);
    {system, eos} ->
        Accum
  end.

-spec new_stencil(list(), integer(), integer(), tuple()) -> list().
%% @doc Generalising neighbourhood schemes, reducing number of overflow functions later on,
%% used to init a new stencil
new_stencil(ListArray,StartPos, RowCount, {square, Offset}) ->
    stencil(ListArray, StartPos, RowCount, neighbourhood_rect(Offset, Offset));
new_stencil(ListArray,StartPos, RowCount, {rect, OffsetX, OffsetY}) ->
    stencil(ListArray, StartPos, RowCount, neighbourhood_rect(OffsetX, OffsetY));
new_stencil(ListArray,StartPos, RowCount, {cross, Offset}) ->
    stencil(ListArray, StartPos, RowCount, neighbourhood_cross(Offset, Offset));
new_stencil(ListArray,StartPos, RowCount, {cross, OffsetX, OffsetY}) ->
    stencil(ListArray, StartPos, RowCount, neighbourhood_cross(OffsetX, OffsetY));
new_stencil(ListArray,StartPos, RowCount, {coords, CoordList}) ->
    stencil(ListArray, StartPos, RowCount, CoordList).

-spec stencil(list(), integer(), integer(), tuple()) -> list().
%% @doc Accepts a list of lists and neighbourhood scheme, recursively iterates each element in
%% each list, adding it to a new array containing neighbourhood'd elements
stencil(_ListArray = [], _StartPos, _RowCount, _TNeighbourhood) -> [];
stencil(ListArray = [[_|_]|_], StartPos, RowCount, TNeighbourhood) ->
    OrigArray = to_array_2d(ListArray),
    Size = {array:size(array:get(0, OrigArray)), array:size(OrigArray)},
    stencil(OrigArray, TNeighbourhood, {0,StartPos+1}, Size, RowCount, new_array_2d(Size)).

stencil(OrigArray, Neighbourhood, Pos = {Px, Py}, Size = {Sx, _Sy}, RowCount, NewArray) when Px < Sx ->
    Neighbours = get_neighbours(OrigArray, Size, {Px,Py}, Neighbourhood, []),
    UpdatedArray = set_2d(NewArray, Pos, Neighbours),
    stencil(OrigArray, Neighbourhood, {Px+1, Py}, Size, RowCount, UpdatedArray);
stencil(OrigArray, Neighbourhood, {_Px, Py}, Size = {_Sx, Sy}, RowCount, NewArray) when Py < Sy-1, RowCount > 1 ->
    stencil(OrigArray, Neighbourhood, {0, Py+1}, Size, RowCount-1, NewArray);
stencil(_OrigArray, _Neighbourhood, _Pos, _Size, _RowCount, NewArray) -> 
    to_list_2d(NewArray).

-spec get_neighbours('array', tuple(), tuple(), list(), list()) -> 'array'.
%% Recurses through each relative coordinate in the neighbourhood, retrieving
%% the associated value from the original array and appending it to a new sub-list
%% within the new array at the same position.
get_neighbours(OrigArray, ASize = {ASx, ASy}, Pos = {Px,Py}, [{Hx,Hy}|T], Accum) when 
    Px+Hx >= 0,
    Px+Hx < ASx,
    Py+Hy >= 0,
    Py+Hy < ASy 
->
    get_neighbours(OrigArray, ASize, Pos, T, [get_2d(OrigArray, {Px+Hx,Py+Hy})|Accum]);
get_neighbours(OrigArray, ASize, Pos, [_H|T], Accum) ->
    get_neighbours(OrigArray, ASize, Pos, T, Accum);
get_neighbours(_,_,_,[], Accum) ->
    Accum.

-spec neighbourhood_rect(integer(), integer()) -> list().
%% @doc Generates a list of neighbourhood offset coordinates relative to 0,0.
%% Can generate either square region or cross shaped region
neighbourhood_rect(X,Y) ->
    rect({trunc(-X/2),trunc(-Y/2)},{X,Y},X, []).
rect({Px, Py},{Sx, Sy}, OrigX, Accum) when Sx > 0 ->
    rect({Px+1, Py},{Sx-1, Sy}, OrigX, [{Px,Py}|Accum]);
rect({_Px, Py},{_Sx, Sy}, OrigX, Accum) when Sy > 1 ->
    rect({trunc(-OrigX/2), Py+1},{OrigX, Sy-1}, OrigX, Accum);
rect(_,_,_,Accum) ->
    Accum.

neighbourhood_cross(X,Y) ->
    cross_x(trunc(-X/2), X, [])
        ++ cross_y(trunc(-Y/2), Y, []).

cross_x(Px, Len, Accum) when Len > 0 -> 
    cross_x(Px+1, Len-1, [{Px,0}|Accum]);
cross_x(_,_, Accum) -> 
    Accum.
cross_y(Py, Len, Accum) when Len > 0, Py =/= 0 -> 
    cross_y(Py+1, Len-1, [{0,Py}|Accum]);
cross_y(Py, Len, Accum) when Len > 0 -> 
    cross_y(Py+1, Len-1, Accum);
cross_y(_,_, Accum) -> 
    Accum.

-spec to_array_2d(list()) -> 'array'.
%% @doc Converts a 2-dimentional list into a 2-dimentional array via the 
%% built in array module. These arrays have much faster random access times
%% compared to lists.
to_array_2d(L1 = [_H|_T]) ->
    array:fix(array:from_list(to_array_2d(L1, []))).    
to_array_2d([H|T], Accum) ->
    [array:fix(array:from_list(H)) | to_array_2d(T, Accum)];
to_array_2d([], Accum) -> Accum.

-spec to_list_2d('array') -> list().
%% @doc Converts a 2-dimentional array into a 2-dimentional list via the 
%% built in array module. Reverses the operation of {@link to_array_2d/1},
%% used prior to passing data to next workflow pid.
to_list_2d(Array) -> to_list_2d(array:to_list(Array), []).

to_list_2d([H|T], Accum) -> to_list_2d(T, [array:to_list(H)|Accum]);
to_list_2d([], Accum) -> Accum.

-spec new_array_2d(tuple()) -> 'array'.
%% @doc Creates a new 2-dimentional array via the built in array module of
%% size SizeX x SizeY.
new_array_2d({SizeX, SizeY}) -> new_array_2d(SizeX, SizeY).
new_array_2d(SizeX, SizeY) ->
    array:fix(array:new(SizeY, {default, array:new(SizeX, {default, []})})).

-spec get_2d('array', tuple()) -> any().
%% @doc Retrieves an element at given X,Y from the provided array
get_2d(Array, {X,Y}) ->
    array:get(trunc(X), array:get(trunc(Y), Array)).

-spec set_2d('array', tuple(), any()) -> ok.
%% @doc Sets an element's value at given X,Y within the provided array
set_2d(Array, {X,Y}, Value) ->
    array:set(trunc(Y), array:set(trunc(X), Value, array:get(trunc(Y), Array)), Array).

-spec apply_func(list(), function()) -> list().
%% @doc Recursively applies given function to each element in list, returns new 
%% 2-dimentional list as output
apply_func_2d(Data, WorkerFun) -> apply_func_2d(Data, WorkerFun, []).
apply_func_2d([H|T], WorkerFun, Accum) -> 
    NewRow = apply_func(H, WorkerFun),
    case NewRow of
        [] ->
            apply_func_2d(T, WorkerFun, Accum);
        _ ->
            apply_func_2d(T, WorkerFun, [NewRow|Accum])
    end;
apply_func_2d([], _WorkerFun, Accum) -> lists:reverse(Accum).

apply_func([H|T], WorkerFun) ->
    apply_func([H|T], WorkerFun, []).
apply_func([H|T], WorkerFun, Accum) when H == [] ->
    apply_func(T, WorkerFun, Accum);
apply_func([H|T], WorkerFun, Accum) ->
    apply_func(T, WorkerFun, [WorkerFun(H)|Accum]);
apply_func([], _, Accum) ->
    lists:reverse(Accum).