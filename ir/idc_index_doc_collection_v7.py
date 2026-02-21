"""*****************************************************************************

                         idc_index_doc_collection_v7.py 17.12.25

Program to index .json collection using Python client.

Thanks to Catalin-Andrei Preda for a new version which is considerably faster
than the original. It indexes the files in batches rather than individually.

This version changes timeouts when Elasticsearch client is created on line 26.

*****************************************************************************"""

import time

from elasticsearch import Elasticsearch, helpers
import json
from pathlib import Path

# Disable security warnings:
import warnings
from elasticsearch.exceptions import ElasticsearchWarning
warnings.simplefilter('ignore', ElasticsearchWarning)

# Connect to ElasticSearch
es = Elasticsearch( "http://localhost:9200", request_timeout=30, max_retries=10, retry_on_timeout=True )

"""-----------------------------------------------------------------------------

Creates and index. If it already exists, deletes it

"""

def idc_create_index( index_name ):

    # Check if the index exists
    if es.indices.exists( index=index_name ):
        # If the index exists, tell user, then delete it

        print( 'Index', index_name, 'already exists, deleting it.' )
        es.indices.delete( index=index_name )


    es.indices.create( index=index_name )
    print( 'New index', index_name, 'has been created.')
    print( 'Press RETURN to continue.' )
    input()

"""-----------------------------------------------------------------------------

Creates an Elasticsearch index of some documents in .json.

1. You need a .json which has pairs of lines, the first gives the DocID, the second gives the document. Just like accounts.json we used before.

2. Make sure Elasticsearch is running

3. When you load this program, a Python client will connect to Elasticsearch (see above).

4. Suppose your .json is called collection_1500_docs_per_topic_utf8_2025.json and you wish to call your index student_index. You should call this function like this:

idc_index( 'collection_1500_docs_per_topic_utf8_2025.json', 'student_index' ) 

If student_index already exists, it will be deleted first.

5. Then you can search it - see below.

"""

def idc_index( filename, index_name ):

    filename = Path( filename )
    if not filename.is_file():
        print( f"File '{filename}' containing docs to be indexed does not exist." )
        return

    idc_create_index( index_name )

    actions = []  # List to hold bulk actions

    # Start the timer
    start_time = time.time()

    with open( filename, 'r', encoding='utf-8' ) as f:
        while True:
            docid_json_str = f.readline()    # Read string containing JSON for ID
            content_json_str = f.readline()  # Read string containing JSON for doc

            if not docid_json_str:           # Break if no more lines
                break

            docid_json = json.loads( docid_json_str )  # Convert strings to dicts
            content_json = json.loads( content_json_str )

            docid = docid_json[ 'index' ][ '_id' ]     # Extract the DocID
            print( 'DocID:', docid )

            # Prepare the action for bulk indexing
            action = {
                "_index": index_name,
                "_id": docid,
                "_source": content_json
            }
            actions.append( action )

            # If actions list has reached a certain size, execute bulk indexing
            if len( actions ) >= 10000:  # 10,000 seems a good figure.
                helpers.bulk( es, actions )
                actions = []  # Reset actions list

    # Index any remaining documents
    if actions:
        helpers.bulk( es, actions )

    # End the timer
    end_time = time.time()

    # Calculate and print the execution time
    elapsed_time = end_time - start_time
    print( f'Execution time: {elapsed_time} seconds' )

"""-----------------------------------------------------------------------------

Search your index. You need to create it first (see above). You can also search
any index your created previously with Kebana.

1. Load this program

2. Decide on a query, e.g. 'Makis Keravnos'

3. Do something like this:

>>> r = idc_search( 'makis keravnos', 'student_index' )
>>> r[ 'hits' ][ 'hits' ][ 0 ] # First hit - see 'title', 'parsedParagraphs'
>>> r[ 'hits' ][ 'hits' ][ 1 ] # Second hit
>>> # etc. (only two hits for this in the 10-doc collection)

4. Hint: To see the structure, do the same search in Kibana.

"""

def idc_search( query_string, index ):           # e.g. 'Makis Keravnos'

    result = es.search(
        index = index,
        query = {
            'multi_match' : {
                'query' : query_string,
                "fields": [],             # 'title', 'parsedParagraphs' or both
                "type":'phrase'           # 'phrase' or 'best_fields'
            }
        } )

    return( result )


print( 'To index documents, do a command like this:' )
print( 'idc_index( \'result_v3_utf8_2500_docs.json\', \'student_index_2500_docs_2025\' )' )

# Index the documents in the .json file and create an Elasticsearch index
# called 'student_index':
#
# idc_index( 'result_v3_utf8_2500_docs.json', 'student_index_2500_docs_2025' )
