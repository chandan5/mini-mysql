import csv_metadata
import parse_query

# db, metadata = csv_metadata.addTable("my_table", ["X", "Y"]);
# csv_metadata.readTable('table1',db['table1']);

# import pdb;pdb.set_trace()
db, metadata = csv_metadata.readMetadataAndConstructDb();
#print metadata
while 1:
    queryString = raw_input("Simple SQL> ");
    tokens = parse_query.parseQuery(queryString);
    # print tokens;
    #import pdb; pdb.set_trace()
    db, metadata = csv_metadata.readMetadataAndConstructDb();

    csv_metadata.execute(tokens, db, metadata);
