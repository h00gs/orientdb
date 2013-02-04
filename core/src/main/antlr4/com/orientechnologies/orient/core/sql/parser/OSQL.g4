grammar OSQL;

options {
    language = Java;
}

//-----------------------------------------------------------------//
// LEXER
//-----------------------------------------------------------------//


// GLOBAL STUFF ---------------------------------------

COMMA 	: ',';
DOT 	: '.';
WS  :   ( ' ' | '\t' | '\r'| '\n' ) -> skip ;
UNARY : '+' | '-' ;
MULT : '*' | '/' ;
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
fragment LETTER : ~('0'..'9' | ' ' | '\t' | '\r'| '\n' | ',' | '-' | '+' | '*' | '/' | '(' | ')' | '=' | '.');

LPAREN : '(';
RPAREN : ')';
LBRACKET : '[';
RBRACKET : ']';
    

//LITERALS  ----------------------------------------------

TEXT1 :  '\'' ( ESC_SEQ | ~('\\'|'\'') )* '\'' ;
TEXT2 :  '"'  ( ESC_SEQ | ~('\\'|'"' ) )* '"'  ;
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

literal_number
	: (UNARY^)? (INT|FLOAT)
	;

literal	
	: TEXT1
	| TEXT2
	| literal_number
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

indexCall
	: LBRACKET (INT|TEXT1|TEXT2) RBRACKET
	;

expression
  : literal
  | word
  | LPAREN expression RPAREN
  | functionCall
  | expression methodCall
  ;

sentence
	: expression (expression)*
  ;
