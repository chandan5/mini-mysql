import csv
import itertools
import os

#dectionary with tablename as key and fields as list in value
metadata = {};
#dictionary with tablename as key and fields as (dictionary with column as key and values as list)
db = {};


def readTable(tablename, table):
    with open(tablename + ".csv", 'rb') as csvfile:
        tablereader = csv.reader(csvfile, delimiter=',')
        for row in tablereader:
            row = [int(x) for x in row];
            table.append(row);
    return table;

def truncateTable(table, db):
    if(not db.has_key(table)):
        print "Table " + table + " does not exists";
        return -1;
    else:
        db[table] = [];
        toAddLines = '';
        with open(table + '.csv', 'wb') as table_file:
            table_file.write(toAddLines);
        return 0;

def printRows(header, rows):
    # import pdb; pdb.set_trace();
    for col_name in header:
        print col_name,
        print '\t',
    # print "\t".join(header);
    print '';
    for row in rows:
        for col in row:
            print col,
            print '\t',
        print '';
        # print "\t".join(row[0]);
def colNoFromColName(table_column, tables, metadata):
    table_and_column = table_column.split(".");
    if len(table_and_column) == 2:
        table_name = table_and_column[0]
        col_name = table_and_column[1]
        col_no = 0;
        i=0;
        flag = 0;
        j = 0;
        ret_col_no = 0;
        flag = 0;
        for table in tables:
            if table_name == table:
                if col_name in metadata[table]:
                    col_no = metadata[table].index(col_name);
                    j, ret_col_no = (i, col_no);
                    flag = 1;
                else:
                    print col_name + " does not exist in " + table_name;
                    return (-1, -1);
            else:
                i += 1;
        if flag:
            return (j, ret_col_no);
        else:
            print "Column " + col_name + " does not exist in table " + table_name;
    else:
        col_name = table_and_column[0]
        col_no = 0;
        i=0;
        flag = 0;
        j = 0;
        ret_col_no = 0;
        flag = 0;
        for table in tables:
            if col_name in metadata[table]:
                col_no = metadata[table].index(col_name);
                j, ret_col_no = (i, col_no);
                if flag:
                    print "Multiple columns in give tables have name " + col_name;
                    return (-1, -1);
                flag = 1;
            else:
                i += 1;
        if flag:
            return (j, ret_col_no);
        else:
            print "Column " + col_name + " does not exist in given table(s)";

    return (-1, -1);

def executeInsert(table, values, db, metadata):
    values = [str(value) for value in values]
    if metadata.has_key(table):
        if len(metadata[table]) == len(values):
            with open(table + '.csv', 'a') as table_file:
                toAddLine = ','.join(values)
                toAddLine += '\r\n';
                table_file.write(toAddLine);
                db[table].append([value for value in values]);
                return 0;
        else:
            print "number of values to be inserted should be " + str(len(metadata[table]));
            return -1;
    else:
        print "table " + table + " does not exist";
        return -1;

def whereClause(row, conds, tables, metadata):
    res_where = []
    for cond in conds:
        if isinstance(cond,str):
            res_where.append(cond);
            continue;
        if not (cond[0].isdigit() or (cond[0].startswith('-') and cond[0][1:].isdigit())):
            i, col_no = colNoFromColName(cond[0], tables, metadata);
            if not (i == -1 and col_no == -1):
                lhs = row[i][col_no];
            else:
                print "column " + cond[0] + " does not exist";
                return -1;
        else:
            lhs = int(cond[0])
        if not (cond[2].isdigit() or (cond[2].startswith('-') and cond[2][1:].isdigit())):
            i, col_no = colNoFromColName(cond[2], tables, metadata);
            if not(i == -1 and col_no == -1):
                rhs = row[i][col_no];
            else:
                print "column " + cond[2] + " does not exist";
                return -1;
        else:
            rhs = int(cond[2])
        eq = ["=", "eq"];
        ne = ["!=", "ne"];
        lt = ["<", "lt"];
        le = ["<=", "le"];
        gt = [">", "gt"];
        ge = [">=", "ge"];
        # print lhs, rhs
        if(cond[1] in eq):
            res_where.append(lhs == rhs);
        elif(cond[1] in ne):
            res_where.append(lhs != rhs);
        elif(cond[1] in lt):
            res_where.append(lhs < rhs);
        elif(cond[1] in le):
            res_where.append(lhs <= rhs);
        elif(cond[1] in gt):
            res_where.append(lhs > rhs);
        elif(cond[1] in ge):
            res_where.append(lhs >= rhs);
        print lhs, rhs
    return res_where

def executeDelete(tables, where, conds, db, metadata):
    for table in tables:
        if not db.has_key(table):
            print "table " + table + " does not exist";
            return -1;
    db_tables = [db[table] for table in tables]
    metadata_tables = [metadata[table] for table in tables]
    rows = []
    for row in itertools.product(*db_tables):
        res_where = [];
        if where == True:
            res_where = whereClause(row, conds, tables, metadata);
            if isinstance(res_where,int) and res_where == -1:
                return -1;
            if eval(' '.join([ str(z) for z in res_where])) == True:
                continue;
        row = [i for i in row[0]];
        rows.append(row);
        print row, res_where

    if(truncateTable(tables[0],db) == -1):
        return -1;
    else:
        for row in rows:
            if (executeInsert(tables[0],row, db, metadata) == -1):
                return -1;
    return 0;

def executeSelect(allColumns, columns, tables, where, conds, db, metadata):
    # print allColumns
    # print columns
    # print tables
    # print where
    # print conds

    for table in tables:
        if not db.has_key(table):
            print "table " + table + " does not exist";
            return -1;
    db_tables = [db[table] for table in tables]
    metadata_tables = [metadata[table] for table in tables]
    retRows = []
    header = []
    headerMap = {};
    specialToken = 0;
    specialTokens = ["MAX", "MIN", "AVG", "SUM", "DISTINCT"];
    specialFlag = 0;
    if columns[0].upper() in specialTokens :
        columns[0] = columns[0].upper()
        specialToken = 1;
        headerMap[columns[2]] = colNoFromColName(columns[2], tables, metadata);
        if(headerMap[columns[2]] == (-1, -1)):
            return -1;
    if allColumns == 1:
        for table in tables:
            for field in metadata[table]:
                header.append(table+"."+field);
    else:
        if columns[0] == "DISTINCT":
            header = [columns[2]]
        elif specialToken == 0:
            header = [column for column in columns]
        else :
            header = [''.join(columns)]

    if specialToken == 0:
        for column in header:
            headerMap[column] = colNoFromColName(column, tables, metadata);
            if(headerMap[column] == (-1, -1)):
                return -1;
    count = 0;

    for row in itertools.product(*db_tables):
        # print row

        res_where = [];
        if where == True:
            res_where = whereClause(row, conds, tables, metadata);
            if isinstance(res_where,int) and res_where == -1:
                return -1;
            # print row, res_where

            if eval(' '.join([ str(z) for z in res_where])) == False:
                continue;
        retRow = []
        for column in header:
            # print headerMap[column]
            # print row[headerMap[column][0]][headerMap[column][1]]

            if specialToken == 0 or columns[0] == "DISTINCT":
                retRow.append(row[headerMap[column][0]][headerMap[column][1]]);
            else:
                val = row[headerMap[columns[2]][0]][headerMap[columns[2]][1]];
                if columns[0] == "MAX":
                    if specialFlag == 1:
                        specialValue = max(specialValue,val)
                    else:
                        specialValue = val;
                        specialFlag = 1;
                elif columns[0] == "MIN":
                    if specialFlag == 1:
                        specialValue = min(specialValue,val)
                    else:
                        specialValue = val;
                        specialFlag = 1;
                elif columns[0] == "AVG" or "SUM":
                    if specialFlag == 1:
                        specialValue += val
                    else:
                        specialValue = val;
                        specialFlag = 1;
        count += 1;
        if specialToken == 0 or columns[0] == "DISTINCT":
            retRows.append(retRow)
    # import pdb; pdb.set_trace()

    if specialToken == 1 and columns[0] != "DISTINCT":
        retRows = []
        if specialFlag == 1:
            if columns[0] == "MAX" or columns[0] == "MIN" or columns[0] == "SUM":
                retRows.append([specialValue])
            elif columns[0] == "AVG":
                retRows.append([specialValue/count]);
        else:
            print "No columns found that satisfy Where Clause Or No Columns in given table(s)"
            return -1;
    if columns[0] == "DISTINCT":
        distinctRetRows = []
        for row in retRows:
            if row not in distinctRetRows:
                distinctRetRows.append(row)
        printRows(header, distinctRetRows);
    else:
        printRows(header, retRows);
    # import pdb; pdb.set_trace()
    return 0;

def execute(tokens, db, metadata):
    if(len(tokens) > 0):
        if(tokens[0] == 'exit'):
            return exit();
        elif(tokens[0] == 'select'):
            allColumns = 0;
            where = False;
            columns = tokens.columns
            if(columns == '*'):
                allColumns = 1;
            tables = tokens.tables
            conds = tokens.conds
            if conds != '':
                where = True;
            # import pdb; pdb.set_trace()

            return executeSelect(allColumns, columns, tables, where, conds, db, metadata);
        elif(tokens[0] == 'delete from'):
            table = tokens.table;
            tables = [table]
            values = tokens.intValues;
            conds = tokens.conds
            where = False;
            if conds != '':
                where = True;
            executeDelete(tables, where, conds, db, metadata);
            return 0;
        elif(tokens[0] == 'insert into'):
            table = tokens.table;
            values = tokens.intValues;
            executeInsert(table, values, db, metadata);
            return 0;
        elif(tokens[0] == "create table"):
            table = tokens.table;
            if(metadata.has_key(table)):
                print "Table " + table + " already exists";
                return -1;
            else:
                fields = [field for field in tokens.fields if field != "int"];
                db, metadata = addTable(table, fields);
                return 0;
        elif(tokens[0] == "truncate table"):
            table = tokens.table
            return truncateTable(table, db);
        elif(tokens[0] == "drop table"):
            table = tokens.table
            if(not db.has_key(table)):
                print "Table " + table + " does not exists";
                return -1;
            else:
                db.pop(table,None);
                metadata.pop(table,None);
                toAddLines = '';
                os.remove(table+'.csv');
                with open('metadata.txt', 'wb') as table_file:
                    table_file.write(toAddLines);
                for table in metadata:
                    addTable(table,metadata[table]);
                return 0;

def readMetadataAndConstructDb():
    with open("metadata.txt") as metadata_file:
        metadata_lines = metadata_file.readlines()
    key = "";
    metadata_lines = [metadata_line.strip() for metadata_line in metadata_lines];
    for i in xrange(0,len(metadata_lines)-1):
        if metadata_lines[i] == "<begin_table>":
            i += 1;
            if i < len(metadata_lines):
                tablename = metadata_lines[i];
                db[tablename] = [];
                readTable(tablename, db[tablename]);
                metadata[tablename] = [];
                i += 1;
            while i < len(metadata_lines) and metadata_lines[i] != "<end_table>":
                col_name = metadata_lines[i];
                metadata[tablename].append(col_name);
                i += 1;
    return (db, metadata);

def addTable(tablename, fields):
    toAddLines = "";
    toAddLines += "\n<begin_table>";
    toAddLines += ("\n" + tablename);
    for field in fields:
        toAddLines += ( "\n" +  field);
    toAddLines += "\n<end_table>";
    open(tablename +  '.csv', 'wb');
    with open('metadata.txt', 'a') as metadata_file:
        metadata_file.write(toAddLines);
    return readMetadataAndConstructDb();
