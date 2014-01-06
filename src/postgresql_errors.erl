%%% vim: ts=4 sts=4 sw=4 expandtab:

-module(postgresql_errors).
-export([
    internal_error_reason_to_string/1,
    query_error_code/1,
    public_error_reason/1
]).

-ifdef(TEST).
-export([parse_error_reason/1]).
-endif.

-include("postgresql_types.hrl").

-spec internal_error_reason_to_string(Reason :: postgresql_error_reasons()) -> nonempty_string().
internal_error_reason_to_string(Reason) ->
    case parse_error_reason(Reason) of
        {known_code, KnownReason} ->
            "error_" ++ atom_to_list(KnownReason);
        {unknown_code, {ErrorCode, _ErrorMessage}} ->
            "error_" ++ type_utils:to_list(ErrorCode);
        {expected, {Type, Source}} ->
            atom_to_list(Type) ++ "_" ++ atom_to_list(Source);
        {custom, ParsedReason} ->
            "custom_" ++ atom_to_list(ParsedReason);
        {unexpected, _ParsedReason} ->
            "undefined"
    end.

-spec query_error_code(Reason :: postgresql_error_reasons()) -> 'undefined' | known_errors_codes() | unknown_errors_codes().
query_error_code(Reason) ->
    case parse_error_reason(Reason) of
        {known_code, KnownReason} ->
            KnownReason;
        {unknown_code, {ErrorCode, _ErrorMessage}} ->
            ErrorCode;
        _ ->
            undefined
    end.

-spec public_error_reason(Reason :: postgresql_error_reasons()) -> public_error_reasons().
public_error_reason(Reason) ->
    case parse_error_reason(Reason) of
        {known_code, KnownReason} ->
            KnownReason;
        {unknown_code, {_ErrorCode, _ErrorMessage} = UnknownReason} ->
            UnknownReason;
        {expected, {_Type, _Source} = ExpectedReason} ->
            ExpectedReason;
        {custom, CustomReason} ->
            {custom, CustomReason};
        {unexpected, Reason} ->
            Reason
    end.

% private functions

parse_error_reason({{error, _, _, _, _} = #error{code = <<"23505">>}, pgsql}) ->
    {known_code, duplicate_key};
parse_error_reason({{error, _, _, _, _} = #error{code = ErrorCode, message = ErrorMessage}, pgsql}) ->
    {unknown_code, {ErrorCode, ErrorMessage}};

parse_error_reason({Type, Source} = Reason) when is_atom(Source) andalso
    (Type =:= busy orelse Type =:= timeout orelse Type =:= closed orelse Type =:= nodedown) ->
    {expected, Reason};
parse_error_reason({custom, Reason} = Error) when is_atom(Reason) ->
    Error;
parse_error_reason(Reason) ->
    {unexpected, Reason}.
