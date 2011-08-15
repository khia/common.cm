%% DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
%% Copyright (c) 2009-2011, ILYA Khlopotov. All rights reserved.
%%
%% The contents of this file are subject to the terms of either the GNU
%% General Public License ("GPL") version 2, or any later version,
%% or the Common Development and Distribution License ("CDDL") 
%% (collectively, the "License"). 
%% You may not use this file except in compliance with the License. 
%% 
%% This program is distributed in the hope that it will be useful,
%% but WITHOUT ANY WARRANTY; without even the implied warranty of
%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
%%
%% You should have received a copy of the GNU General Public License
%% along with this program.  If not, see http://www.gnu.org/licenses/
%%
%% You should have received a copy of the 
%% Common Development and Distribution License along with this program.  
%% If not, see http://www.opensource.org/licenses/

-module(common.cm.db).
-include("utils.hrl").
-include("debug.hrl").
-include("cm.hrl").

-import(common.db.db, [create_table/3, load_tables/2]).
-import(common.utils.lists, [errors/1]).
-import(proplists).
-import(code).
-import(common_cm_data).

-export([%%create_tables/1, 
	 init/1, is_active/0,
	 test/0]).

-export([
	 is_local/0, %% TODO Rename this	 
	 %% functions to work with parameters of all nodes
	 init_param/2,
	 init_app_param/3,
	 init_app_param/4,
	 set_param/2,
	 set_app_param/3,
	 create_param/2, %% optimized combination of init_param and set_param 
	 create_param/3, %% will be used for multiple instances 
	 create_param/4, %% create_param({Key, Instance1}, Value)

	 notify_add_local/2, 
	 notify_add_local/3, 
	 notify_add/2, 
	 notify_add_app/3,

	 get_param/1,
	 get_param/2,
	 get_app_param/2,
	 get_app_param/3,
	 clear_param/1,
	 clear_app_param/2,
	 delete_param/1,
	 delete_app_param/2,
	 is_defined/1,
	 is_defined/2,
	 is_set/1,
	 is_set/2,
	 %% functions to work with local for current node parameters
	 init_local_param/2,
	 init_local_app_param/3,
	 init_local_app_param/4,
	 set_local_param/2,
	 set_local_app_param/3,
	 create_local_param/2, %% optimized combination of 
	                       %% init_local_param and set_local_param 
	 create_local_param/3, %% will be used for multiple instances 
	 create_local_param/4, %% create_param({Key, Instance1}, Value)

	 get_local_param/1,
	 get_local_param/2,
	 get_local_app_param/2,
	 get_local_app_param/3,
	 clear_local_param/1,
	 clear_local_app_param/2,
	 delete_local_param/1,
	 delete_local_app_param/2,
	 is_local_defined/1,
	 is_local_defined/2,
	 is_local_set/1,
	 is_local_set/2
	]).
%% get_param/1.2.3
%% get_params
%% get_all_keys/1
%% is_defined
%% is_default
%% lookup
%% get_all_values
%% substitute_aliases
%% "Test %variable%" replaces with {variable, "Replacement string"}
%% -record(common_cm_dataset, 
%% 	{source,
%% 	 files,
%% 	 validation,
%% 	 data,
%% 	 target   
%% 	}).

%% -record(common_cm_config, 
%% 	{name :: atom(),   %% name of the configuration set
%% 	 order :: false    %% when we don't need initialization 
%% 	      | 0          %% initialization will be done before create_tables 
%% 	      | integer(), %%  
%% 	 status :: loaded | active, 
%% 	 datasets :: #common_cm_dataset{}, 
%% %%	 dump, 
%% %%	 loadable, 
%% 	 app, 
%% 	 notif, 
%% 	 loaded_ts, 
%% 	 stored_ts}).

-record(common_cm_data,
	{id :: integer(),
	 app :: atom(),
	 key :: term(),
	 default :: term(),
	 is_defined :: boolean(),
	 notify = [] :: [#mfa{} | atom() | fun()], %% if atom() we will send the message to 
	 %% process with such name??
	 value :: term(),
	 created_on :: term() %% this field required for erlydb.
	}).

is_active() ->
    case {code:which(common_cm_data), code:which(common_cm_local)} of
	{_, non_existing} -> false;
	{non_existing, _} -> false;
	_Else -> true
    end.
	    
     
%% We need CM before db so we will force the creation of the table 
init(Nodes) ->
    case create_tables(Nodes) of
	{error, _Reason} = Error ->
	    ?debug("Cannot create tables, reason ~p.", [Error], init),
	    Error;
	ok ->
	    case errors(load_tables(mnesia, 
				    [common_cm_data, common_cm_local])) of
		[] -> true;
		Errors -> 
		    ?error("Cannot create mnesia table for configuration, ~p.", 
			   [Errors], init),
		    {error, {cannot_create_tables, Errors}}
	    end
    end.

-type(reason() :: term()).
-spec(create_tables/1 :: 
      (Nodes :: [node()]) -> ok | {atomic, ok} | {error, reason()}).
create_tables(Nodes) ->
    ?debug("About to create_tables(~p).",[Nodes], create_tables),
    Properties = 
	[
         %%{Field, {Type, Modifier}, Null, Key, Default, Extra, MnesiaType}
	 {id ,{integer,undefined},false,primary,undefined,identity,integer},
	 {app,{atom,undefined},false,undefined,undefined,undefined,atom},
	 {key, {binary,undefined},false,undefined,undefined,undefined,binary},
	 {is_defined,{atom,undefined},false,undefined,undefined,undefined,atom},
	 {notify, {binary,undefined},false,undefined,undefined,undefined,binary},
	 {value, {binary,undefined},false,undefined,undefined,undefined,binary}
	],

    Res0 = create_table(common_cm_data, 
 		 [
 		  {attributes, 
 		   record_info(fields, common_cm_data)},
 		  {disc_copies, Nodes},
		  {user_properties, Properties}
 		 ]),
    Res = Res0 ++ create_table(common_cm_local, 
 		 [
 		  {attributes, record_info(fields, common_cm_data)},
		  {record_name, common_cm_data},
 		  {ram_copies, Nodes}, %% FIXME
		  {local_content, true},
		  {user_properties, Properties}
 		 ]),
    case errors(Res) of
	[] -> ok;
	Errors -> 
	    ?error("Cannot create mnesia table for configuration", [], init),
	    {error, {cannot_create_tables, Errors}}
    end.
    

create_table(Name, Opts) -> 
    case create_table(mnesia, Name, Opts) of
	{atomic, ok} -> [{Name, true}];
	Error -> [{Name, Error}]
    end.
	    
    
    
    %% Application atom
%%    ok.

get_local_param(Key) ->
    get_app_param(common_cm_local, global, Key, 
		  {error, {undefined_param, global, Key}}).

get_local_param(Key, Default) ->
    get_app_param(common_cm_local, global, Key, Default).

get_local_app_param(Application, Key) ->
    get_app_param(common_cm_local, Application, Key, 
		  {error, {undefined_param, Application, Key}}).
get_local_app_param(Application, Key, Default) ->
    get_app_param(common_cm_local, Application, Key, Default).

get_param(Key) ->
    get_app_param(global, Key, {error, {undefined_param, global, Key}}).

get_param(Key, Default) ->
    get_app_param(global, Key, Default).

get_app_param(Application, Key) ->
    get_app_param(Application, Key, 
		  {error, {undefined_param, Application, Key}}).

get_app_param(Application, Key, Default) ->
    get_app_param(common_cm_data, Application, Key, Default).

%% FIXME Default parameter is not used
get_app_param(M, Application, Key, Default) ->
    case M:find([
			      {{app, '=', Application}, 'and', 
			       {key, '=', term_to_binary(Key)}}
			     ]) of
	[] -> Default;
	[Record] -> case M:value(Record) of
			<<>> -> M:default(Record);
			Value -> binary_to_term(Value)
		    end
    end.

is_local_defined(Key) ->
    is_defined(common_cm_local, global, Key).
is_local_defined(Application, Key) ->
    is_defined(common_cm_local, Application, Key).

is_defined(Key) ->
    is_defined(common_cm_data, global, Key).
is_defined(Application, Key) ->
    is_defined(common_cm_data, Application, Key).

is_defined(M, Application, Key) ->
    case M:find([
		 {{app, '=', Application}, 'and', 
		  {key, '=', term_to_binary(Key)}}
		]) of
	[] -> false;
	[_Record] -> true
    end.

is_local_set(Key) ->
    is_set(common_cm_local, global, Key).
is_local_set(Application, Key) ->
    is_set(common_cm_local, Application, Key).
is_set(Key) ->
    is_set(common_cm_data, global, Key).
is_set(Application, Key) -> 
    is_set(common_cm_data, Application, Key).
is_set(M, Application, Key) -> 
    try M:find([
		{{app, '=', Application}, 
		 'and', 
		 {key, '=', term_to_binary(Key)}}
	       ]) of
	[] -> false;
	[Record] -> M:is_defined(Record)
    catch _:_ -> false
		 end.
	
set_local_param(Key, Value) ->
    set_app_param(common_cm_local, global, Key, Value).
set_local_app_param(Application, Key, Value) ->
    set_app_param(common_cm_local, Application, Key, Value).

set_param(Key, Value) ->
    set_app_param(common_cm_data, global, Key, Value).

set_app_param(Application, Key, Value) ->
    set_app_param(common_cm_data, Application, Key, Value).
set_app_param(M, Application, Key, Value) ->
    case M:transaction(
	   fun() ->
		   case M:find(
			  [{{app, '=', Application}, 
			    'and', 
			    {key, '=', term_to_binary(Key)}}
			  ]) of
		       [] -> {error, {undefined_param, Application, Key}};
		       [Record] -> 
			   Modified = 
			       M:set_fields(Record, 
					    [{is_defined, true},
					     {value, term_to_binary(Value)}]),
			   M:save(Modified),
			   {Record, Modified}
		   end
	   end) of
	{atomic, {Old, New}} -> notify(M, Application, Key, Old, New), New;
	Error -> Error
    end.

notify_add_local(Key, Notification) ->
    notify_add_app(common_cm_local, global, Key, Notification).
notify_add_local(Application, Key, Notification) ->
    notify_add_app(common_cm_local, Application, Key, Notification).

notify_add(Key, Notification) ->
    notify_add_app(common_cm_data, global, Key, Notification).

notify_add_app(Application, Key, Notification) ->
    notify_add_app(common_cm_data, Application, Key, Notification).
notify_add_app(M, Application, Key, Notification) ->
    case M:transaction(
	   fun() ->
		   case M:find(
			  [{{app, '=', Application}, 
			    'and', 
			    {key, '=', term_to_binary(Key)}}
			  ]) of
		       [] -> {error, {undefined_param, Application, Key}};
		       [Record] -> 
			   Old = M:notify(Record),
%%			   New = [Notification] ++ Old, %% TODO check if exist
			   Binary = term_to_binary(Notification),
			   New = <<Old/binary, Binary/binary>>,
			   Modified = M:set_fields(Record, [{notify, New}]),
			   M:save(Modified),
			   New
		   end
	   end) of
	{atomic, New} -> New;
	Error -> Error
    end.

clear_local_param(Key) ->
    clear_app_param(common_cm_local, global, Key).
clear_local_app_param(Application, Key) ->
    clear_app_param(common_cm_local, Application, Key).

clear_param(Key) ->
    clear_app_param(common_cm_data, global, Key).

clear_app_param(Application, Key) ->
    clear_app_param(common_cm_data, Application, Key).
clear_app_param(M, Application, Key) ->
    case M:transaction(
	   fun() ->
		   case M:find(
			  [{{app, '=', Application}, 
			    'and', 
			    {key, '=', term_to_binary(Key)}}
			  ]) of
		       [] -> {error, {undefined_param, Application, Key}};
		       [Record] -> 
			   Modified = M:set_fields(Record, [{is_defined, false},
							    {value, <<>>}]),
			   M:save(Modified),
			   {Record, Modified}
		   end
	   end) of
	{atomic, {Old, New}} -> notify(M, Application, Key, Old, New), New;
	Error -> Error
    end.

delete_local_param(Key) ->
    delete_app_param(common_cm_local, global, Key).
delete_local_app_param(Application, Key) ->
    delete_app_param(common_cm_local, Application, Key).

delete_param(Key) ->
    delete_app_param(common_cm_data, global, Key).

delete_app_param(Application, Key) ->
    delete_app_param(common_cm_data, Application, Key).
delete_app_param(M, Application, Key) ->
    case M:transaction(
	   fun() ->
		   case M:find(
			  [{{app, '=', Application}, 
			    'and', 
			    {key, '=', term_to_binary(Key)}}
			  ]) of
		       [] -> {error, {undefined_param, Application, Key}};
		       [Record] -> 
			   M:delete(Record),
			   {Record, deleted}
		   end
	   end) of
	{atomic, {Old, New}} -> notify(M, Application, Key, Old, New), New;
	Error -> Error
    end.

create_local_param(Key, Value) ->
    create_param(common_cm_local, global, Key, Value, []).
create_local_param(Application, Key, Value) ->
    create_param(common_cm_local, Application, Key, Value, []).
create_local_param(Application, Key, Value, Options) ->
    create_param(common_cm_local, Application, Key, Value, Options).

create_param(Key, Value) ->
    create_param(common_cm_data, global, Key, Value, []).
create_param(Application, Key, Value) ->
    create_param(common_cm_data, Application, Key, Value, []).
create_param(Application, Key, Value, Options) ->
    create_param(common_cm_data, Application, Key, Value, Options).
create_param(M, Application, Key, Value, Options) ->
    Notification = proplists:get_value(notif, Options, []),
    Record = M:new_with([{app, Application}, {key, term_to_binary(Key)},
			 {is_defined, true},{default, <<>>},
			 {notify, term_to_binary(Notification)},
			 {value, term_to_binary(Value)}]),
    case M:transaction(
	   fun() ->
		   case M:find(
			  [{{app, '=', Application}, 
			    'and', 
			    {key, '=', term_to_binary(Key)}}
			  ]) of
		       [] -> M:save(Record);
		       _Records -> {error, {already_exist, {Application, Key}}}
		   end
	   end) of
	{atomic, _Res} -> notify(M, Application, Key, init, Record), Record;
	Error -> Error
    end.

init_local_param(Key, Default) ->
    init_app_param(common_cm_local, global, Key, Default, []).
init_local_app_param(Application, Key, Default) ->
    init_app_param(common_cm_local, Application, Key, Default, []).
init_local_app_param(Application, Key, Default, Options) ->
    init_app_param(common_cm_local, Application, Key, Default, Options).

init_param(Key, Default) ->
    init_app_param(common_cm_data, global, Key, Default, []).

init_app_param(Application, Key, Default) ->
    init_app_param(common_cm_data, Application, Key, Default, []).

init_app_param(Application, Key, Default, Options) ->
    init_app_param(common_cm_data, Application, Key, Default, Options).
init_app_param(M, Application, Key, Default, Options) ->
    Notification = proplists:get_value(notify, Options, []),
    Record = M:new_with([{app, Application}, {key, term_to_binary(Key)},
				      {is_defined, false},{default, Default},
				      {notify, term_to_binary(Notification)},
				      {value, <<>>}]),
    case M:transaction(
	   fun() ->
		   case M:find(
			  [{{app, '=', Application}, 
			    'and', 
			    {key, '=', term_to_binary(Key)}}
			  ]) of
		       [] -> M:save(Record);
		       _Records -> {error, {already_exist, {Application, Key}}}
		   end
	   end) of
	{atomic, _Res} -> Record;
	Error -> Error
    end.
    
test() -> 
    init_param(param0, default_value0),
    init_param({param1}, {default_value1}),
    init_param({param1, key1}, {default_value1}),
    init_param([param2], [default_value2]),
    init_param({param3, ip0}, {default_value3, true, undefined}).

%% substitute_aliases
%% "Test %variable%" replaces with {variable, "Replacement string"}
%%-define(token, {key, format, ri, validator}).

%%substitute_aliases()

%% new(Str) ->
%%     scan(Str).

%% scan(Str) ->
%%     scan(Str, []).

%% scan([$%, $% | Rest], Res) ->
%%     scan([$% | Rest], Res);
%% scan([$% | Rest], Res) ->
%%     scan_var([$% | Rest], Res);

is_local() -> 
    lists:member(common_cm_data, common.db.db:local_tables(mnesia)).
%%activity(AccessContext, Fun, Args, AccessMod) -> ResultOfFun | exit(Reason)
%% AccessMod:read(ActivityId, Opaque, Tab, Key, LockKind)

notify(M, Application, Key, init, New) ->
    case M:notify(New) of
	[] -> ok;
	Modules -> 
	    New_Value = value(M, New),
	    notify(to_term(Modules), {Application, Key, init, New_Value})
    end;
notify(M, Application, Key, Old, deleted) ->
    case M:notify(Old) of
	[] -> ok;
	Modules -> 
	    Old_Value = value(M, Old),
	    notify(to_term(Modules), {Application, Key, Old_Value, deleted})
    end;
notify(M, Application, Key, Old, New) ->
    ?debug("~p ~p", [Old, New], notify),
    case M:notify(Old) of
	[] -> ok;
	Modules -> 
	    Old_Value = value(M, Old),
	    New_Value = value(M, New),
	    notify(to_term(Modules), {Application, Key, Old_Value, New_Value})
    end.

value(M, Record) -> to_term(M:value(Record)).

to_term(<<>>)  -> [];
to_term(Value) -> binary_to_term(Value).
    

%%#mfa{} | atom() | fun()	    
%% Change me to work on binaries
notify([], _Changes) -> ok;
notify([Module | Rest], Changes) when is_function(Module) ->
    catch Module(Changes, []),
notify(Rest, Changes);
notify([Module | Rest], Changes) when is_atom(Module) ->
    Module ! Changes, %% can be dangerous 
notify(Rest, Changes);
%% TODO how to notify application on remote node
notify([#mfa{m = Module, f = Fun, a = Arg} | Rest], Changes) ->
    catch Module:Fun(Changes, Arg),
notify(Rest, Changes);
notify([_Module | Rest], Changes) -> %% skip unknown formats
notify(Rest, Changes).

