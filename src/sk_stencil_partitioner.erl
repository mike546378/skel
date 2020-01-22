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
%%%
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
    send_data(NextPid, StencilData).

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
    stencil(ListArray, {rect, Offset, Offset});
new_stencil(ListArray, {rect, OffsetX, OffsetY}) ->
    stencil(ListArray, {rect, OffsetX, OffsetY});
new_stencil(ListArray, {cross, Offset}) ->
    stencil(ListArray, {cross, Offset, Offset});
new_stencil(ListArray, {cross, OffsetX, OffsetY}) ->
    stencil(ListArray, {cross, OffsetX, OffsetY}).

%% @doc Accepts a list of lists and neighbourhood scheme, recursively iterates each element in
%% each list, adding it to a new array containing neighbourhood'd elements
stencil(ListArray = [[_|_]|_], TNeighbourhood) ->
    Array = to_array_2d(ListArray),
    Size = {array:size(array:get(0, Array)), array:size(Array)},
    stencil(Array, TNeighbourhood, {0,0}, Size, new_array_2d(Size)).

stencil(Array, TNeighbourhood, Pos = {Px, Py}, Size = {Sx, _Sy}, Accum) when Px < Sx ->
    NewAccum = add_to_neighbours(get_2d(Array, {Px,Py}), Pos, TNeighbourhood, Accum),
    stencil(Array, TNeighbourhood, {Px+1, Py}, Size, NewAccum);
stencil(Array, TNeighbourhood, {_Px, Py}, Size = {_Sx, Sy}, Accum) when Py < Sy-1 ->
    stencil(Array, TNeighbourhood, {0, Py+1}, Size, Accum);
stencil(_Array, _TNeighbourhood, _Pos, _Size, Accum) -> 
    to_list_2d(Accum).

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

%% Determines what neighbourhood scheme to use then adds Element to all
%% associated neighbours.
add_to_neighbours(Element, _Pos = {Px,Py}, {rect, Sx, Sy}, Accum) ->
    rect_neighbourhood(Element, {Px-(Sx-1)/2, Py-((Sy-1)/2)}, {Sx, Sy}, Sx, Accum);
add_to_neighbours(Element, _Pos = {Px,Py}, {cross, Sx, Sy}, Accum) ->
    cross_neighbourhood(Element, {Px, Py-((Sy-1)/2)}, {Sx, Sy}, Sy, Accum).

%% @doc Recursively adds Element to all neighbours in a rectangular region, 
%% returning the newly updated array. Recurses from top left element to
%% bottom right.
rect_neighbourhood(Element, {Px,Py}, {Sx, Sy}, Xlen, Accum) when Px < 0 ->                  % Skipping out of range positions
    rect_neighbourhood(Element, {Px+1,Py}, {Sx-1, Sy}, Xlen, Accum);
rect_neighbourhood(Element, {Px,Py}, {Sx, Sy}, Xlen, Accum) when Py < 0 ->
    rect_neighbourhood(Element, {Px,Py+1}, {Sx, Sy-1}, Xlen, Accum);

rect_neighbourhood(Element, Pos = {Px,Py}, {Sx, Sy}, Xlen, Accum) when Sx > 0, Sy > 0 ->    % Attempt add element to array, increasing X pos
    Res = (catch set_2d(Accum, Pos, get_2d(Accum, Pos)++[Element])),
    case Res of
        {array, _, _, _, _} ->
            rect_neighbourhood(Element, {Px+1,Py}, {Sx-1, Sy}, Xlen, Res);
        _ ->
            rect_neighbourhood(Element, {Px+1,Py}, {Sx-1, Sy}, Xlen, Accum)
    end;
rect_neighbourhood(Element, {Px,Py}, {_Sx, Sy}, Xlen, Accum) when Sy > 0 ->                 % Increase Y pos or return
    rect_neighbourhood(Element, {Px-Xlen, Py+1}, {Xlen,Sy-1},Xlen,Accum);
rect_neighbourhood(_Element, _Pos, _Size, _Xlen, Accum)-> Accum.

%% @doc Recursively adds Element to all neighbours in a cross region, 
%% returning the newly updated array. Recurses from top left element to
%% bottom right.
cross_neighbourhood(Element, {Px,Py}, {Sx, Sy}, Ylen, Accum) when Px < 0, Py =/= (Ylen/2) ->                  % Skipping out of range positions
    cross_neighbourhood(Element, {Px+1,Py}, {Sx-1, Sy}, Xlen, Accum);
cross_neighbourhood(Element, {Px,Py}, {Sx, Sy}, Xlen, Accum) when Py < 0 ->
    cross_neighbourhood(Element, {Px,Py+1}, {Sx, Sy-1}, Xlen, Accum);

cross_neighbourhood(Element, Pos = {Px,Py}, {Sx, Sy}, Xlen, Accum) when Sx > 0, Sy > 0 ->    % Attempt add element to array, increasing X pos
    Res = (catch set_2d(Accum, Pos, get_2d(Accum, Pos)++[Element])),
    case Res of
        {array, _, _, _, _} ->
            cross_neighbourhood(Element, {Px+1,Py}, {Sx-1, Sy}, Xlen, Res);
        _ ->
            cross_neighbourhood(Element, {Px+1,Py}, {Sx-1, Sy}, Xlen, Accum)
    end;
cross_neighbourhood(Element, {Px,Py}, {_Sx, Sy}, Xlen, Accum) when Sy > 0 ->                 % Increase Y pos or return
    cross_neighbourhood(Element, {Px-Xlen, Py+1}, {Xlen,Sy-1},Xlen,Accum);
cross_neighbourhood(_Element, _Pos, _Size, _Xlen, Accum)-> Accum.


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