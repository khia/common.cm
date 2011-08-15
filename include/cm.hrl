-define(value(V), {value, term, V}).
-define(range(Min, Max, Type), {value, Type, {Min, Max}}).
-define(min(V, Type), {min, Type, V}).
-define(max(V, Type), {min, Type, V}).
-define(tuple(Tuple), {tuple, Tuple, []}).
-define(proplist(Body), {proplist, Body, []}).
-define(user(Body, Fun), {user, Body, Fun}).
%% Modifications
-define(enum(Body), {enum, list, Body}).
-define(mand(Body), {mand, term, Boby}).
