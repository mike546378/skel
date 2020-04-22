%%%----------------------------------------------------------------------------
%%% @author Michael Reid <mreid@dundee.ac.uk>
%%% @copyright 2012 University of St Andrews (See LICENCE)
%%% @headerfile "skel.hrl"
%%% 
%%% @doc This module contains the initialization logic of a Divide and Conquer skeleton.
%%%
%%% The 'divconquer' skeleton applies and Divide and Conquer algorithm over a dataset,
%%% recursively splitting splitting the data into two sub-problems via a divide function, before 
%%% applying a conquer function then merging the data back up.
%%%
%%% There are two possible skeleton definitions; one involves declaring functions for all three
%%% stages, the second only requires a single stage to be defined. If using the first definition,
%%% it is possible to substitite any of the functions for an atom which will use the associated 
%%% pre-programmed default function.
%%%
%%% Both definitions require a maximum depth before a node will continue sequentially rather than 
%%% spawning further worker processes.
%%%
%%% === Example ===
%%% 
%%% 	```skel:do([{divconquer, {merge, fun ?MODULE:mergeFun/1}, 5}], Data)'''
%%% 
%%% 	In this example, we produce a divconquer skeleton with a custom merge function `mergeFun/1`. 
%%%  The workers will traverse down 5 levels before continuing sequentially. Default split and conquer  
%%%  functions will be used.
%%%
%%% 	```skel:do([{divconquer, fun ?MODULE:mergeFun/1, fun ?MODULE:conquerFun/1, fun ?MODULE:splitFun/1, 5}], Data)'''
%%%
%%%     This example explicitely declares all three possible functions along with the aforementioned 
%%%  maximum depth. Replacing any of the functions with a `default` atom will use the associated
%%%  default function.
%%%
%%% @end
%%%----------------------------------------------------------------------------

-module(sk_divconquer).

-export([
        make/4,
        make/2,
        default_merge/2,
        default_conquer/1,
        default_split/1
        ]).

-include("skel.hrl").

-ifdef(TEST).
    -compile(export_all).
-endif.

-spec make(tuple(), pos_integer()) -> maker_fun().
%% @doc Initialises a divconquer skeleton given either a split, merge or conquer function withina tuple alongwith the 
%% maximum depth respectively. The other two functions are set to default.
make({merge, MergeFun}, MaxDepth) when is_integer(MaxDepth), is_function(MergeFun) ->
    fun(NextPid) ->
        spawn(sk_divconquer_worker, start, [NextPid, MergeFun, fun default_conquer/1, fun default_split/1, MaxDepth])
    end;
make({conquer, ConquerFun}, MaxDepth) ->
    fun(NextPid) ->
        spawn(sk_divconquer_worker, start, [NextPid, fun default_merge/2, ConquerFun, fun default_split/1, MaxDepth])
    end;
make({divide, DivFun}, MaxDepth) ->
    fun(NextPid) ->
        spawn(sk_divconquer_worker, start, [NextPid, fun default_merge/2, fun default_conquer/1, DivFun, MaxDepth])
    end.

-spec make(function(), function(), function(), pos_integer()) -> maker_fun().
%% @doc Initialises a divconquer skeleton given the three function parameters and maximum depth respectfully. 
%% Any non-function function parameter is replaced by the associated default function.
make(MergeFun, ConquerFun, SplitFun, MaxDepth) when is_integer(MaxDepth) ->
    fun(NextPid) ->
        start(NextPid, MergeFun, ConquerFun, SplitFun, MaxDepth)
    end.

start(NextPid, MergeFun, ConquerFun, SplitFun, MaxDepth) when is_function(MergeFun) == false ->
    start(NextPid, fun default_merge/2, ConquerFun, SplitFun, MaxDepth);
start(NextPid, MergeFun, ConquerFun, SplitFun, MaxDepth) when is_function(ConquerFun) == false ->
    start(NextPid, MergeFun, fun default_conquer/1, SplitFun, MaxDepth);
start(NextPid, MergeFun, ConquerFun, SplitFun, MaxDepth) when is_function(SplitFun) == false ->
    start(NextPid, MergeFun, ConquerFun, fun default_split/1, MaxDepth);

start(NextPid, MergeFun, ConquerFun, SplitFun, MaxDepth) ->
        spawn(sk_divconquer_worker, start, [NextPid, MergeFun, ConquerFun, SplitFun, MaxDepth]).

-spec default_merge(list(), list()) -> list().
%% Default merge function
default_merge(List1, List2) ->
    List1++List2.

-spec default_conquer(any()) -> list().
%% Default merge function
default_conquer(Element) ->
    [Element].

-spec default_split(list()) -> list().
%% Default divide function
default_split([Elem]) ->
    [Elem];
default_split(List) ->
    lists:split(trunc(length(List)/2), List).