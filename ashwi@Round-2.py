from elasticsearch import Elasticsearch
import pandas as pd
import numpy as np
es = Elasticsearch([{'host': 'localhost', 'port': 8989, 'scheme': 'http'}])
def createCollection(p_collection_name):
    """Create a collection (index) in Elasticsearch."""
    if not es.indices.exists(index=p_collection_name):
        es.indices.create(index=p_collection_name)
        print(f"Collection '{p_collection_name}' created.")
    else:
        print(f"Collection '{p_collection_name}' already exists.")

def indexData(p_collection_name, p_exclude_column):
    """Index employee data into the specified collection, excluding the specified column."""
    try:
        df = pd.read_csv(r'C:\Users\91902\Desktop\Employee Sample Data 1.csv', encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(r'C:\Users\91902\Desktop\Employee Sample Data 1.csv', encoding='latin1')
    df.drop(columns=[p_exclude_column], inplace=True)
    df = df.replace({np.nan: None})
    records = df.to_dict(orient='records')
    for record in records:
        try:
            es.index(index=p_collection_name, id=record['Employee ID'], body=record)
        except Exception as e:
            print(f"Failed to index record {record['Employee ID']}: {e}")

    print(f"Indexed data into '{p_collection_name}', excluding '{p_exclude_column}'.")

def searchByColumn(p_collection_name, p_column_name, p_column_value):
    """Search within the specified collection for records where the column matches the value."""
    query = {
        "query": {
            "match": {
                p_column_name: p_column_value
            }
        }
    }
    results = es.search(index=p_collection_name, body=query)
    return results['hits']['hits']

def getEmpCount(p_collection_name):
    """Get the count of employees in the specified collection."""
    count = es.count(index=p_collection_name)
    return count['count']

def delEmpById(p_collection_name, p_employee_id):
    """Delete an employee by their ID from the specified collection."""
    try:
        es.delete(index=p_collection_name, id=p_employee_id)
        print(f"Employee ID '{p_employee_id}' deleted from '{p_collection_name}'.")
    except Exception as e:
        print(f"Error deleting employee ID '{p_employee_id}': {e}")

def getDepFacet(p_collection_name):
    """Retrieve the count of employees grouped by department."""
    query = {
        "size": 0,
        "aggs": {
            "departments": {
                "terms": {
                    "field": "Department.keyword"
                }
            }
        }
    }
    results = es.search(index=p_collection_name, body=query)
    return results['aggregations']['departments']['buckets']
if __name__ == "__main__":
    v_nameCollection = 'hash_ashwitha'
    v_phoneCollection = 'hash_4987'     
    createCollection(v_nameCollection)
    createCollection(v_phoneCollection)    
    emp_count_name = getEmpCount(v_nameCollection)
    print(f"Employee count in {v_nameCollection}: {emp_count_name}")
    indexData(v_nameCollection, 'Department')
    indexData(v_phoneCollection, 'Gender')
    emp_count_name_updated = getEmpCount(v_nameCollection)
    print(f"Updated employee count in {v_nameCollection}: {emp_count_name_updated}")
    
    delEmpById(v_nameCollection, 'E02003')

    emp_count_after_deletion = getEmpCount(v_nameCollection)
    print(f"Employee count after deletion in {v_nameCollection}: {emp_count_after_deletion}")

    search_it = searchByColumn(v_nameCollection, 'Department', 'IT')
    print(f"Search results for Department 'IT': {search_it}")

    search_male = searchByColumn(v_nameCollection, 'Gender', 'Male')
    print(f"Search results for Gender 'Male': {search_male}")

    search_it_phone = searchByColumn(v_phoneCollection, 'Department', 'IT')
    print(f"Phone collection search results for Department 'IT': {search_it_phone}")

    dep_facet_name = getDepFacet(v_nameCollection)
    print(f"Department facet for {v_nameCollection}: {dep_facet_name}")

    dep_facet_phone = getDepFacet(v_phoneCollection)
    print(f"Department facet for {v_phoneCollection}: {dep_facet_phone}")







































