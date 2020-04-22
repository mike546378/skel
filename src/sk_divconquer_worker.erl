%%%----------------------------------------------------------------------------
%%% @author Michael Reid <mreid@dundee.ac.uk>
%%% @copyright 2012 University of St Andrews (See LICENCE)
%%% @headerfile "skel.hrl"
%%% 
%%% @doc This module contains the divconquer skeleton worker logic.
%%%
%%% The 'divconquer' skeleton applies and Divide and Conquer algorithm over a dataset,
%%% recursively splitting the data into two sub-problems via a divide function, before 
%%% applying a conquer function then merging the data back up.
%%%
%%% This worker module is responsible for handling all stages of a node in Divide
%%% and conquer. If maximum depth has not yet been reached and there is still input data
%%% left, two new child workers processes will be spawned. Otherwise, the remaining data 
%%% will be parsed sequentially.
%%%
%%% @end
%%%----------------------------------------------------------------------------
-module(sk_divconquer_worker).

-export([
         start/7,
         start/5
        ]).

-include("skel.hrl").

-spec start(pid(), function(), function(), function(), pos_integer(), number(), number()) -> 'eos'.
-spec start(pid(), function(), function(), function(), pos_integer()) -> 'eos'.
%% @doc Starts the worker, recieving all data from parent node, applying divide and conquer algorithm
%% then sending output back to the parent. 
start(NextPid, MergeFun, ConquerFun, SplitFun, MaxDepth) ->
    start(NextPid, MergeFun, ConquerFun, SplitFun, MaxDepth, 0, 0).

start(ParentPid, MergeFun, ConquerFun, SplitFun, MaxDepth, CurrentDepth, CurrIndex) ->
    sk_tracer:t(75, self(), {?MODULE, start}, []),

    receive
        {data, InputList, SkelParams} ->
            InputList;
        {data, InputList} ->
            SkelParams = [],
            InputList
    end,
    
    MergedList = divide_conquer(InputList, MergeFun, ConquerFun, SplitFun, MaxDepth, CurrentDepth),
    sk_tracer:t(75, self(), ParentPid, {?MODULE, data}, [{output, {sk_divconquer_worker, {CurrIndex, MergedList}}}]),
    case CurrentDepth of
        0 ->
            ParentPid ! {data, MergedList, SkelParams},
            ParentPid ! {system, eos};
        _ ->
            ParentPid ! {data, {CurrIndex, MergedList}},
            ParentPid ! {system, eos}
    end.


%% @doc main divide and conquer logic; either sequentially perform all remaining operations if max depth has been reached,
%% otherwise apply the split function followed by either spawning two new processes then waiting for s response or apply
%% the Conquer function if Split only returns a single element. 
divide_conquer(List, MergeFun, ConquerFun, SplitFun, MaxDepth, CurrentDepth) 
    when CurrentDepth == MaxDepth ->
        seq_divide_conquer(List, MergeFun, ConquerFun, SplitFun);

divide_conquer(List, MergeFun, ConquerFun, SplitFun, MaxDepth, CurrentDepth) ->
    case SplitFun(List) of
        {Front, Back} -> 
            W1 = spawn(sk_divconquer_worker, start, [self(), MergeFun, ConquerFun, SplitFun, MaxDepth, CurrentDepth+1, 1]),
            sk_tracer:t(75, self(), W1, {?MODULE, data}, [{output, {sk_divconquer_worker, {data, Front}}}]),
            W1 ! {data, Front},
            W2 = spawn(sk_divconquer_worker, start, [self(), MergeFun, ConquerFun, SplitFun, MaxDepth, CurrentDepth+1, 2]),
            sk_tracer:t(75, self(), W2, {?MODULE, data}, [{output, {sk_divconquer_worker, {data, Back}}}]),
            W2 ! {data, Back},
            {NewFront, NewBack} = receive_data(null, null),
            MergeFun(NewFront, NewBack);
        [Res] ->  
            ConquerFun(Res);
        [] -> 
            []
    end.

%% @doc Recieves data back from both children, preserves the order it was sent in.
receive_data(Front, Back) when Front == null; Back == null ->
    receive
        {data, {1, Data}} ->
            receive_data(Data, Back);
        {data, {2, Data}} ->
            receive_data(Front, Data)
    end;
receive_data(Front, Back) -> {Front, Back}.

%% @doc sequential divide and conquer logic; recursively apply the split function to both halves of the
%% data until it can not be broken down any further, at which point the Conquer function is applied before
%% merging back up the tree.
seq_divide_conquer(List, MergeFun, ConquerFun, SplitFun) ->
    case SplitFun(List) of
        {Front, Back} ->
            MergeFun(seq_divide_conquer(Front, MergeFun, ConquerFun, SplitFun), seq_divide_conquer(Back, MergeFun, ConquerFun, SplitFun));
        [Res] ->
            ConquerFun(Res);
        [] ->
            []
    end.
