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
%%% === Example ===
%%% 
%%% 	```skel:do([{stencil, fun ?MODULE:p/1, 5, {square, 3}}], Data)'''
%%% 
%%% 	In this example, we produce a stencil with 5 workers and a 3x3 
%%%  neighbourhood scheme to run the sequential, developer-defined function `p/1' using 
%%%  the two-dimentional list `Data'.
%%%
%%% @end
%%%----------------------------------------------------------------------------

-module(sk_stencil).

-export([
        make/3
        ]).

-include("skel.hrl").

-ifdef(TEST).
-compile(export_all).
-endif.

-spec make(function(), pos_integer(), tuple()) -> maker_fun().
%% @doc Initialises a Stencil skeleton given the Worker function, number of workers and their 
%% a neighbourhood scheme, respectfully.
make(WorkerFun, NumWorkers, TNeighbourhood) ->
    fun(NextPid) ->
        Combiner = spawn(sk_stencil_combiner, start, [NextPid, NumWorkers]),
        spawn(sk_stencil_partitioner, start, [Combiner, WorkerFun, NumWorkers, TNeighbourhood])
    end.
