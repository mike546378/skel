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
-module(sk_stencil_partitioner).

-export([
         start/2
        ]).

-include("skel.hrl").

%% @doc Starts the tagger, labelling each input so that the order of all 
%% inputs is recorded. 
-spec start(pid(), tuple()) -> 'eos'.
start(NextPid, TNeighbourhood) ->
    sk_tracer:t(75, self(), {?MODULE, start}, []),
    DMs = loop(NextPid, []),
    StencilData = new_stencil(DMs, TNeighbourhood),
    send_data(NextPid, StencilData),
    io:format("sk_stencil_partitioner finished~n").

-spec loop(pid(), list()) -> 'eos'.
%% @doc Recursively receives input, accumulating the data messages into a single
%% 2-dimentional list which can then have the stencil functions applied to it.
loop(NextPid, Accum) ->
  receive
    {data, Value, _Idx} ->
      loop(NextPid, [Value|Accum]);
    {system, eos} ->
        Accum
  end.


%% @doc Generalising neighbourhood schemes, reducing number of overflow functions later on,
%% used to init a new stencil
new_stencil(ListArray, {square, Offset}) ->
    stencil(ListArray, neighbourhood_rect(Offset, Offset));
new_stencil(ListArray, {rect, OffsetX, OffsetY}) ->
    stencil(ListArray, neighbourhood_rect(OffsetX, OffsetY));
new_stencil(ListArray, {cross, Offset}) ->
    stencil(ListArray, neighbourhood_cross(Offset, Offset));
new_stencil(ListArray, {cross, OffsetX, OffsetY}) ->
    stencil(ListArray, neighbourhood_cross(OffsetX, OffsetY));
new_stencil(ListArray, {coords, CoordList}) ->
    stencil(ListArray, CoordList).

%% @doc Accepts a list of lists and neighbourhood scheme, recursively iterates each element in
%% each list, adding it to a new array containing neighbourhood'd elements
stencil(ListArray = [[_|_]|_], TNeighbourhood) ->
    OrigArray = to_array_2d(ListArray),
    Size = {array:size(array:get(0, OrigArray)), array:size(OrigArray)},
    stencil(OrigArray, TNeighbourhood, {0,0}, Size, new_array_2d(Size)).

stencil(OrigArray, Neighbourhood, Pos = {Px, Py}, Size = {Sx, _Sy}, NewArray) when Px < Sx ->
    Neighbours = get_neighbours(OrigArray, Size, {Px,Py}, Neighbourhood, []),
    UpdatedArray = set_2d(NewArray, Pos, Neighbours),
    stencil(OrigArray, Neighbourhood, {Px+1, Py}, Size, UpdatedArray);
stencil(OrigArray, Neighbourhood, {_Px, Py}, Size = {_Sx, Sy}, NewArray) when Py < Sy-1 ->
    stencil(OrigArray, Neighbourhood, {0, Py+1}, Size, NewArray);
stencil(_OrigArray, _Neighbourhood, _Pos, _Size, NewArray) -> 
    to_list_2d(NewArray).

%% @doc Recursively sends each new sub-list to next workflow item
send_data(NextPid, [H|T]) ->
    Message = {data, H, []},
    sk_tracer:t(50, self(), NextPid, {?MODULE, data}, [{output, Message}]),
    NextPid ! Message,
    send_data(NextPid, T);
send_data(NextPid, []) ->
    sk_tracer:t(75, self(), NextPid, {?MODULE, system}, [{message, eos}]),
    NextPid ! {system, eos},
    eos.

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



%% @doc Converts a 2-dimentional list into a 2-dimentional array via the 
%% built in array module. These arrays have much faster random access times
%% compared to lists.
to_array_2d(L1 = [_H|_T]) ->
    array:fix(array:from_list(to_array_2d(L1, []))).    
to_array_2d([H|T], Accum) ->
    [array:fix(array:from_list(H)) | to_array_2d(T, Accum)];
to_array_2d([], Accum) -> Accum.

%% @doc Converts a 2-dimentional array into a 2-dimentional list via the 
%% built in array module. Reverses the operation of {@link to_array_2d/1},
%% used prior to passing data to next workflow pid.
to_list_2d([H|T], Accum) -> to_list_2d(T, array:to_list(H) ++ Accum);
to_list_2d([], Accum) -> Accum.

to_list_2d(Array) -> to_list_2d(array:to_list(Array), []).

%% @doc Creates a new 2-dimentional array via the built in array module of
%% size SizeX x SizeY.
new_array_2d({SizeX, SizeY}) -> new_array_2d(SizeX, SizeY).
new_array_2d(SizeX, SizeY) ->
    array:fix(array:new(SizeY, {default, array:new(SizeX, {default, []})})).

%% @doc Retrieves an element at given X,Y from the provided array
get_2d(Array, {X,Y}) ->
    array:get(trunc(X), array:get(trunc(Y), Array)).

%% @doc Sets an element's value at given X,Y within the provided array
set_2d(Array, {X,Y}, Value) ->
    array:set(trunc(Y), array:set(trunc(X), Value, array:get(trunc(Y), Array)), Array).