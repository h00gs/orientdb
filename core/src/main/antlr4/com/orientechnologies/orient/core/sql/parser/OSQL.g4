grammar OSQL;

options {
    language = Java;
}

//-----------------------------------------------------------------//
// LEXER
//-----------------------------------------------------------------//


// KEYWORDS -------------------------------------------
INSERT : I N S E R T ;
INTO : I N T O ;
VALUES : V A L U E S ;
CLUSTER : C L U S T E R ;
INDEX : I N D E X ;
SET : S E T ;


// GLOBAL STUFF ---------------------------------------
COMMA 	: ',';
DOUBLEDOT 	: ':';
DOT 	: '.';
WS  :   ( ' ' | '\t' | '\r'| '\n' ) -> skip ;
UNARY : '+' | '-' ;
MULT : '*' | '/' ;
EQUALS : '=' ;
fragment DIGIT : '0'..'9' ;
    
// case insensitive
fragment A: ('a'|'A');
fragment B: ('b'|'B');
fragment C: ('c'|'C');
fragment D: ('d'|'D');
fragment E: ('e'|'E');
fragment F: ('f'|'F');
fragment G: ('g'|'G');
fragment H: ('h'|'H');
fragment I: ('i'|'I');
fragment J: ('j'|'J');
fragment K: ('k'|'K');
fragment L: ('l'|'L');
fragment M: ('m'|'M');
fragment N: ('n'|'N');
fragment O: ('o'|'O');
fragment P: ('p'|'P');
fragment Q: ('q'|'Q');
fragment R: ('r'|'R');
fragment S: ('s'|'S');
fragment T: ('t'|'T');
fragment U: ('u'|'U');
fragment V: ('v'|'V');
fragment W: ('w'|'W');
fragment X: ('x'|'X');
fragment Y: ('y'|'Y');
fragment Z: ('z'|'Z');
fragment LETTER : ~('0'..'9' | ' ' | '\t' | '\r'| '\n' | ',' | '-' | '+' | '*' | '/' | '(' | ')' | '{' | '}' | '[' | ']'| '=' | '.'| ':' | '#');

LPAREN : '(';
RPAREN : ')';
LBRACKET : '[';
RBRACKET : ']';
LACCOLADE : '{';
RACCOLADE : '}';
    

//LITERALS  ----------------------------------------------

UNSET : '?';
NULL : N U L L ;
IDENTIFIER : '#';

TEXT : ('\'' ( ESC_SEQ | ~('\\'|'\'') )* '\'') 
     | ('"'  ( ESC_SEQ | ~('\\'|'"' ) )* '"' );

INT : DIGIT+ ;
FLOAT
    :   ('0'..'9')+ '.' ('0'..'9')* EXPONENT?
    |   '.' ('0'..'9')+ EXPONENT?
    |   ('0'..'9')+ EXPONENT
    ;

WORD : LETTER (DIGIT|LETTER)* ;




// FRAGMENT -------------------------------------------

fragment EXPONENT : ('e'|'E') ('+'|'-')? ('0'..'9')+ ;
fragment HEX_DIGIT : ('0'..'9'|'a'..'f'|'A'..'F') ;

fragment
ESC_SEQ
    :   '\\' ('b'|'t'|'n'|'f'|'r'|'\"'|'\''|'\\')
    |   UNICODE_ESC
    |   OCTAL_ESC
    ;

fragment
OCTAL_ESC
    :   '\\' ('0'..'3') ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7')
    ;

fragment
UNICODE_ESC
    :   '\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
    ;
    
 
    
    
//-----------------------------------------------------------------//
// PARSER
//-----------------------------------------------------------------//
    
word : WORD ;

identifier : IDENTIFIER INT ':' INT;

unset : UNSET;

number
	: (UNARY^)? (INT|FLOAT)
	;

map
  : LACCOLADE (literal DOUBLEDOT expression (COMMA literal DOUBLEDOT expression)*)? RACCOLADE
  ;

collection
  : LBRACKET (expression (COMMA expression)*)? RBRACKET
  ;

literal	
  : NULL
  | TEXT
	| number
	;

arguments
  : LPAREN (expression (COMMA expression)*)? RPAREN
  ;

functionCall
	: word arguments
	;

methodCall
	: DOT word arguments*
	;

expression
  : literal
  | map
  | collection
  | identifier
  | unset
  | word
  | LPAREN expression RPAREN
  | functionCall
  | expression methodCall
  ;

commandUnknowned 
  : expression (expression)*
  ;
commandInsertIntoByValues
  : INSERT INTO ((CLUSTER|INDEX) DOUBLEDOT)? word commandInsertIntoCluster? commandInsertIntoFields VALUES commandInsertIntoEntry (COMMA commandInsertIntoEntry)*
  ;
commandInsertIntoBySet
  : INSERT INTO ((CLUSTER|INDEX) DOUBLEDOT)? word commandInsertIntoCluster? SET commandInsertIntoSet (COMMA commandInsertIntoSet)*
  ;
commandInsertIntoCluster
  : CLUSTER word
  ;
commandInsertIntoEntry
  : LPAREN expression (COMMA expression)* RPAREN
  ;
commandInsertIntoSet
  : word EQUALS expression
  ;
commandInsertIntoFields
  : LPAREN word(COMMA word)* RPAREN
  ;

command
	: commandUnknowned
  | commandInsertIntoByValues
  | commandInsertIntoBySet
  ;
