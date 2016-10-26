#from pyparsing import Word, alphas, Forward, CaselessKeyword

from pyparsing import CaselessKeyword, delimitedList, Each, Forward, Group, \
        Optional, Word, alphas,alphanums, nums, oneOf, ZeroOrMore, quotedString, \
        Upcase, Keyword, Combine, Literal, Upcase
#tokens
binop = oneOf("= != < > >= <= eq ne lt le gt ge", caseless=True)

selectToken = Keyword("select", caseless=True);
selectMaxToken = Keyword("select max", caseless=True);
selectMinToken = Keyword("select min", caseless=True);
selectAvgToken = Keyword("select avg", caseless=True);
selectSpecialToken = selectMaxToken | selectMinToken | selectAvgToken
selectToken = Keyword("select", caseless=True);
fromToken   = Keyword("from", caseless=True);
whereToken   = Keyword("where", caseless=True);
insertToken   = Keyword("insert into", caseless=True);
deleteToken   = Keyword("delete from", caseless=True);
valuesToken   = Keyword("values", caseless=True);
createToken   = Keyword("create table", caseless=True);
truncateToken = Keyword("truncate table", caseless=True);
dropToken = Keyword("drop table", caseless=True);
exitToken   = Keyword("exit", caseless=True);
intToken   = Keyword("int", caseless=True);
and_   = Keyword("and", caseless=True);
or_   = Keyword("or", caseless=True);
table = column = Word(alphanums);
columns = Group(delimitedList(column));
table_column = Combine(Optional(table + ".") + column);
sumToken = Keyword("sum", caseless=True);
maxToken = Keyword("max", caseless=True);
minToken = Keyword("min", caseless=True);
avgToken = Keyword("avg", caseless=True);
distinctToken = Keyword("distinct", caseless=True);
table_columns = Group(delimitedList(table_column));
func_names = [sumToken, maxToken, minToken, avgToken, distinctToken];
specialToken = sumToken | maxToken | minToken | avgToken | distinctToken
func_table_column = Group(specialToken + '(' + table_column + ')');
tables = Group(delimitedList(table));
number = Word(nums)
plusorminus = Literal('+') | Literal('-')
intNum = Combine( Optional(plusorminus) + number )
intNums = Group(delimitedList(intNum));
columnL =  columnR = table_column | intNum
whereCondition = Group(columnL + binop + columnR).setResultsName("cond");
whereExpression = Forward()
whereExpression << whereCondition + ZeroOrMore( ( and_ | or_ ) + whereCondition )

def parseQuery(queryString):
    try:
        parser = Forward();
        # parser << (Word(alphas).setResultsName( "first" ) + \
        #         #(' ').setResultsName( "delim" ) + \
        #         '*' + Word(alphas).setResultsName( "second"))
        # selectSpecialStmt = Forward.setResultsName("selectSpecialStmt");
        # selectSpecialStmt << (selectSpecialToken + "(" + table_columns + ")" + fromToken \
        #             + table.setResultsName("table"));
        selectStmt = Forward().setResultsName("selectStmt");
        selectStmt << ( selectToken + ( '*' | func_table_column | table_columns).setResultsName( "columns" ) \
                    + fromToken + tables.setResultsName("tables") \
                    + Optional(whereToken + whereExpression.setResultsName("conds") ) );
        deleteStmt = Forward().setResultsName("deleteStmt");
        deleteStmt << ( deleteToken + table.setResultsName("table") \
                    + whereToken + whereExpression.setResultsName("conds"));
        insertStmt = Forward().setResultsName("insertStmt");
        insertStmt << ( insertToken + table.setResultsName("table") + valuesToken \
                    + "(" + intNums.setResultsName("intValues") + ")" );
        createStmt = Forward().setResultsName("createStmt");
        createStmt << ( createToken + table.setResultsName("table") + "(" \
                    + Group(delimitedList(column + intToken)).setResultsName("fields") + ")" );
        truncateStmt = Forward().setResultsName("truncateStmt");
        truncateStmt << ( truncateToken + table.setResultsName("table"));
        dropStmt = Forward().setResultsName("dropStmt");
        dropStmt << ( dropToken + table.setResultsName("table"));
        parser = selectStmt | insertStmt | deleteStmt | createStmt | truncateStmt | dropStmt | exitToken;
        tokens = parser.parseString(queryString);
        # import pdb; pdb.set_trace()
        return tokens
    except Exception as e:
        # print e;
        print "Error in format."
        return [];
    # print tokens.first
    # print tokens.delim
    # print tokens.second
