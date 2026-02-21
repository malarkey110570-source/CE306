"""*****************************************************************************

                         eqs_evaluate_query_set_v5.py

V5 25.02.25. Added function eqs_check_student_queries()

Reads queries from Gold Standard .json file. Submits each one to Elasticsearch,
compares  the  results  to  the  Gold Standard  answers,  and  evaluates  using
Precision and Recall.

Elasticsearch must be running.

The  names  of   the  Gold  Standard  file  and  the   Elastic  index  must  be
correct. Check those below before you run this program!

*****************************************************************************

"""

from elasticsearch import Elasticsearch
import json
# from rgs_read_gold_standard_v6 import rgs_read
# now have eqs_read() below

es = Elasticsearch( "http://localhost:9200" )
# Create the client. Make sure Elasticsearch is already running!

import warnings
from elasticsearch.exceptions import ElasticsearchWarning
warnings.simplefilter('ignore', ElasticsearchWarning)

"""-----------------------------------------------------------------------------

Read a gold standard JSON file. json.loads() reads a string containing a JSON
and converts it to a Python dictionary. Returns this dictionary.

"""

def eqs_read( gold_standard ):
    with open( gold_standard, 'r', encoding='utf-8-sig' ) as f:
        s = f.read()
        d = json.loads( s )
    return d

"""-----------------------------------------------------------------------------

Input: list of gold standard matches for query, read from, say, gold_standard_v2.
Output: List of DocIDs extracted from this.

Note: DocIDs in Wikipedia are always integers  but in Elastic and here they are
stored as strings.

"""

def eqs_gold_docid_list( match_list ):

    ans = []
    for match in match_list:

        ans += [ match[ 'docid' ] ]

    return ans


"""-----------------------------------------------------------------------------

Input: List of hits as returned in [ "hits" ] [ "hists" ].
Output: List of DocIDs extracted from this.

Note: DocIDs in Wikipedia are always integers  but in Elastic and here they are
stored as strings.

"""

def eqs_returned_docid_list( result_list ):

    ans = []
    for hit in result_list:

        ans += [ hit[ '_id' ] ]

    return ans

"""-----------------------------------------------------------------------------

query_type: 'keyword_query' or 'kibana_query'
returned_hits: List of DocIDs returned for query - list of strings
gold_hits: List of Gold DocIDs for query - from .json
n_vals: List of required values for n, e.g. [ 5, 10 ]

Calculates Precision and Recall at the values of n specified.  

"""

def eqs_eval_query( query_type, returned_hits, gold_hits, n_vals ):

    # print( 'eqs_eval_query:' )
    # print( query_type, returned_hits, gold_hits, n_vals )

    for n in n_vals:

        print( '\nResults for n=', n )
        eqs_eval_query_n( query_type, returned_hits, gold_hits, n )

"""-----------------------------------------------------------------------------

Args as above, except last arg is an integer value of n.

"""

def eqs_eval_query_n( query_type, returned_hits, gold_hits, n ):

    if len( returned_hits ) < n:

        print( 'Not enough results to compute P/R for value of n=', n )

    else:

        matching_docids_in_collection = len( gold_hits )
        if matching_docids_in_collection == 0:
            print( 'ERROR: No gold_hits! .json must be wrong!' )

        matching_docids_in_results = 0
        for docid in gold_hits:
            if docid in returned_hits[ 0: n ]:
                matching_docids_in_results += 1

        print( 'Precision = %.2f' % ( matching_docids_in_results / n ) )
        print( 'Recall    = %.2f' % \
               ( matching_docids_in_results / matching_docids_in_collection ) )

"""-----------------------------------------------------------------------------

Evaluates the queries on the Gold Standard. Always double-check:
1. Elastic is running
2. Gold Standard file is correct
3. Index being searched is correct

20.03.25 now takes gold_standard filename as a parameter. Now read in using
eqs_read() above.

"""

def eqs_eval( gold_standard ):

    d = eqs_read( gold_standard )

    query_list = d[ "queries" ]

    print( 'Number of queries in Gold Standard:', len( query_list ) )
 

    for q in query_list:
        original_query = q[ "original_query" ]
        keyword_query = q[ "keyword_query" ]
        kibana_query = q[ "kibana_query" ]
        # This is a query in Kibana Query Language
        # So we can submit it to Elasticsearch

        print( '\n==========================================================' )

        print( '\noriginal_query:', original_query )
        print( 'keyword_query:', keyword_query )
        print( 'kibana_query:', kibana_query )

        if keyword_query != '':

            query = { "multi_match": { "query": keyword_query, "fields": ["parsedParagraphs","title"],\
                       "type": "best_fields"}}
            print( 'keyword_query actually submitted:', query )
            keyword_result = es.search(
                index = 'student_index',
                size = 40, # Max number of hits to return. Default is 10.
                query = query )
        else:
            print( 'Could not submit keyword_query=''' )
            keyword_result = []


        # Blank queries, i.e. {} crash Elastic...
        if kibana_query[ "query" ] != {}:
            kibana_result = es.search(
                index = 'student_index',
                size = 40, # Max number of hits to return. Default is 10.
                query = kibana_query[ "query" ])
        else:
            print( 'Could not submit kibana_query={}' )
            kibana_result = []

        print( "\nResult of Keyword Elastic search:" )
        print( 'Number of hits:', len( keyword_result[ 'hits' ] [ 'hits' ] ) )
        # print( keyword_result[ 'hits' ] [ 'hits' ] [ 0 ] [ '_id' ] )
        # print( keyword_result[ 'hits' ] [ 'hits' ] [ 1 ] [ '_id' ] )
        # print( keyword_result[ 'hits' ] [ 'hits' ] [ 2 ] [ '_id' ] )
        # print( keyword_result[ 'hits' ] [ 'hits' ] [ 3 ] [ '_id' ] )
        # print( 'All answers returned:', \
        #        eqs_returned_docid_list( keyword_result[ 'hits' ] [ 'hits' ] ) )
        # print( 'Gold Standard matches:', \
        #         eqs_gold_docid_list( q[ "matches" ] ) )

        eqs_eval_query( 'keyword_result', \
            eqs_returned_docid_list( keyword_result[ 'hits' ] [ 'hits' ] ),\
            eqs_gold_docid_list( q[ "matches" ] ), [ 5, 10 ] )

        print( "\nResult of Kibana Elastic search:" )
        print( 'Number of hits:', len( kibana_result[ 'hits' ] [ 'hits' ] ) )
        # print( kibana_result[ 'hits' ] [ 'hits' ] [ 0 ] [ '_id' ] )
        # print( kibana_result[ 'hits' ] [ 'hits' ] [ 1 ] [ '_id' ] )
        # print( kibana_result[ 'hits' ] [ 'hits' ] [ 2 ] [ '_id' ] )
        # print( kibana_result[ 'hits' ] [ 'hits' ] [ 3 ] [ '_id' ] )
        # print( 'All answers returned:', 
        #        eqs_returned_docid_list( kibana_result[ 'hits' ] [ 'hits' ] ) )
        # print( 'Gold Standard matches:', \
        #        eqs_gold_docid_list( q[ "matches" ] ) )

        eqs_eval_query( 'kibana_result', \
            eqs_returned_docid_list( kibana_result[ 'hits' ] [ 'hits' ] ),\
            eqs_gold_docid_list( q[ "matches" ] ), [ 5, 10 ] )

#eqs_eval( 'gold_standard_v5.json' )
# Evaluate Results
# ASSUMES
# 1. Elasticsearch is running
# 2. You have rgs_read_gold_standard_v5.py (see top) (no longer needed)
# 3. You have created a suitable index (check with CURL) (student_index assumed)
# 4. .json file contains queries you want to test
# 5. The program refers to the CORRECT index above
# 6. The program refers to the CORRECT .json in rgs_read_gold_standard_v6.py
print( 'To run this do: eqs_eval( \'gold_standard_v5.json\' )' )

