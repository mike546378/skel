-module(stencil).
-export([
    t4/1,t5/1,t6/1,t7/2,t8/2,t9/2,t10/0,
    average/1,
    benchmark/0
    ]).

-define(Threads, 12).

load_skel() ->
    code:del_path("../../skel/ebin"),
    code:add_path("../../skel/ebin").

benchmark() ->
    sk_profile:benchmark(fun ?MODULE:test/0, [], 100).


t4(Size) ->         %% 1-dimentional sequential
    load_skel(),
    sk_tracer:start(),
    Data = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20],
    Res = skel:do([{stencil, [{seq, fun average/1}], Size}], [Data]),
    io:format("~n~lp~n", [Res]).

t5(Size) ->     %% 2-dimentional farmed
    load_skel(),
    sk_tracer:start(),
    Data = [
        [01,02,03,04,05,06,07,08,09,10],
        [11,12,13,14,15,16,17,18,19,20],
        [21,22,23,24,25,26,27,28,29,30],
        [31,32,33,34,35,36,37,38,39,40]
    ],
    Res = skel:do([{stencil, [{farm, [{seq, fun average/1}], 10}], Size}], Data),
    io:format("~n~lp~n", [Res]).

t6(Size) ->     %% 2-dimentional ordered farmed
    load_skel(),
    sk_tracer:start(),
    Data = [
        [01,02,03,04,05,06,07,08,09,10],
        [11,12,13,14,15,16,17,18,19,20],
        [21,22,23,24,25,26,27,28,29,30],
        [31,32,33,34,35,36,37,38,39,40]
    ],
    Res = skel:do([{stencil, [{ord, [{farm, [{seq, fun average/1}], 10}]}], Size}], Data),
    io:format("~n~lp~n", [Res]).

t7(SizeX, SizeY) -> %% 2-dimentional rect farmed
    load_skel(),
    sk_tracer:start(),
    Data = [
        [01,02,03,04,05,06,07,08,09,10],
        [11,12,13,14,15,16,17,18,19,20],
        [21,22,23,24,25,26,27,28,29,30],
        [31,32,33,34,35,36,37,38,39,40]
    ],
    Res = skel:do([{stencil, [{farm, [{seq, fun average/1}], 10}], {rect, SizeX, SizeY}}], Data),
    io:format("~n~lp~n", [Res]).

t8(DataSize, GridSize) ->   %% large 2-dimentional sequential
    load_skel(),
    Data = generate(DataSize),
    sk_profile:benchmark(fun skel:do/2, [[{stencil, [{seq, fun average/1}], {square, GridSize}}], Data], 1).

t9(DataSize, GridSize) ->   %% large 2-dimentional farmed
    load_skel(),
    Data = generate(DataSize),
    sk_profile:benchmark(fun skel:do/2, [[{stencil, [{farm, [{seq, fun average/1}], 10}], {square, GridSize}}], Data], 1).

t10() ->     %% 2-dimentional ordered farmed - custom coords
    load_skel(),
    sk_tracer:start(),
    Data = [
        [01,02,03,04,05,06,07,08,09,10],
        [11,12,13,14,15,16,17,18,19,20],
        [21,22,23,24,25,26,27,28,29,30],
        [31,32,33,34,35,36,37,38,39,40]
    ],
    Coords = [{-1,-1},{1,1},{0,0},{-1,1},{1,-1}],
    Res = skel:do([{stencil, [{ord, [{farm, [{seq, fun average/1}], 10}]}], {coords, Coords}}], Data),
    io:format("~n~lp~n", [Res]).

%% Average List elements
average(List) -> %io:format("~lp~n", [List]),
    average(List, 0, 0).
average([H|T], Total, Count) ->
    average(T, Total+H, Count+1);
average([], Total, Count) ->
    Total/Count.

%% Large data generation
generate(Size) ->
    Generate = fun() -> rand:uniform(1000) end,
    MakeList = fun() -> populate(Generate, Size, []) end,
    populate(MakeList, Size, []).

populate(_, Size, Acc) when Size < 0 ->
    Acc;
populate(Generate, Size, Acc) ->
    populate(Generate, Size - 1, [Generate() | Acc]).