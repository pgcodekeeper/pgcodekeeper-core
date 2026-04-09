grammar DependenciesList;

options {
    language=Java;
}

compileUnit
    : deps_definition+ EOF
    ;

deps_definition
    : source=definition ARROW target=definition SEMI_COLON
    ;

definition
    : object_type schema_qualified_name function_args?
    ;

schema_qualified_name
    : identifier (DOT identifier)*
    ;

function_args
    : LEFT_PAREN arg_list? RIGHT_PAREN
    ;

arg_list
    : arg (COMMA arg)*
    ;

arg
    : identifier (LEFT_PAREN numeric_literal (COMMA numeric_literal)? RIGHT_PAREN)?
    ;

numeric_literal
    : NUMBER
    ;

object_type
    : identifier
    | identifier identifier
    | identifier identifier identifier
    ;

identifier
    : Identifier
    ;

ARROW: '->';
LEFT_PAREN: '(';
RIGHT_PAREN: ')';
COMMA: ',';
DOT: '.';
SEMI_COLON : ';';

NUMBER: [0-9]+ ('.' [0-9]+)?;

Identifier: [a-z_A-Z][a-z_A-Z0-9]*;

NewLine: [\r\n]+ -> skip;
WS: [ \t] -> skip;