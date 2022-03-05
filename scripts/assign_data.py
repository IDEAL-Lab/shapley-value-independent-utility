import sys, getopt
import numpy as np
import configparser
from random import sample, shuffle
import pandas as pd
import os
import json
import polars as pl

def print_usage():
    print('assign_data.py -d dataset -a <alpha> -b <beta> -k <number_of_data_owner> -m <max_copy> -o <equal owners> -r <equal records> -f <output dir>')

def get_parameter(argv):
    try:
        # opts is a list of returning key-value pairs, args is the options left after striped
        # the short options 'hi:o:', if an option requires an input, it should be followed by a ":"
        # the long options 'ifile=' is an option that requires an input, followed by a "="
        opts, args = getopt.getopt(argv, "hd:a:b:k:m:o:r:f:", ["dataset", "alpha=", "beta=", "number_of_data_owner=", "max_copy=", "assign_owner_equally", "assign_record_equally", "output_dir"])
    except getopt.GetoptError:
        print("error in get opts, args")
        print_usage()
        sys.exit(2)

    if not opts:
        print("error in getting opts")
        sys.exit(2)

    # print arguments
    for opt, arg in opts:
        if opt == '-h':
            print_usage()
            sys.exit(2)
        elif opt in ("-d", "--dataset"):
            dataset = arg
        elif opt in ("-a", "--alpha"):
            alpha = float(arg)
        elif opt in ("-b", "--beta"):
            beta = float(arg)
        elif opt in ("-k", "--number"):
            number_of_data_owner = int(arg)
        elif opt in ("-m", "--copy"):
            max_copy = int(arg)
        elif opt in ("-o", "--equalowners"):
            print(f"arg: {arg}")
            assign_owner_equally = bool(int(arg))
        elif opt in ("-r", "--equalrecords"):
            assign_record_equally = bool(int(arg))
        elif opt in ("-f", "--output"):
            output_dir = arg

    return (dataset, alpha, beta, number_of_data_owner, max_copy, assign_owner_equally, assign_record_equally, output_dir)

def get_tables(dataset):
    if dataset == "world":
        return ["city", "country", "countrylanguage"]
    elif dataset == "tpch":
        return ["customer","lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]

def get_config(dataset):
    result = {}

    config = configparser.RawConfigParser()
    config.read("../config/config.cfg")

    ### General
    config_dict = dict(config.items("general"))
    default_alpha = config_dict.get("default_alpha")
    default_alpha = int(default_alpha)
    result["default_alpha"] = default_alpha

    default_max_copy = config_dict.get("default_max_copy")
    default_max_copy = int(default_max_copy)
    result["default_max_copy"] = default_max_copy

    default_beta = config_dict.get("default_beta")
    default_beta = int(default_beta)
    result["default_beta"] = default_beta

    
    config_dict = dict(config.items(dataset))

    default_number_of_data_owner = config_dict.get("default_number_of_data_owner")
    default_number_of_data_owner = int(default_number_of_data_owner)
    result["default_number_of_data_owner"] = default_number_of_data_owner

    default_basic_owner = config_dict.get("default_basic_owner")
    default_basic_owner = int(default_basic_owner)
    result["default_basic_owner"] = default_basic_owner

    timeout = config_dict.get("timeout")
    timeout = int(timeout)
    result["timeout"] = timeout

    default_sample_size = config_dict.get("default_sample_size")
    default_sample_size = int(default_sample_size)
    result["default_sample_size"] = default_sample_size

    for table in get_tables(dataset):
        number_of_record = config_dict.get(table)
        number_of_record = int(number_of_record)
        result[table] = number_of_record

    return result

def set_output_dir(output_dir):
    if not os.path.exists(output_dir):
        number_of_sub_folders = 0
    else: 
        number_of_sub_folders = len(next(os.walk(output_dir))[1])
    
    output_dir = f"{output_dir}/{number_of_sub_folders+1}"
    os.makedirs(output_dir)

    return output_dir

def assign_data(dataset, alpha, beta, number_of_data_owner, max_copy, assign_owner_equally, assign_record_equally, output_dir):

    tables = get_tables(dataset)

    owners = assign_owners_to_tables(beta, assign_owner_equally, number_of_data_owner, dataset, tables)
    print("owners: ", owners)

    if assign_owner_equally: 
        assign_owner_mode = "equalowner"
    else:
        assign_owner_mode = "inequalowner"
    
    if assign_record_equally:
        assign_record_mode = "equalrecord"
    else:
        assign_record_mode = "inequalrecord"

    output_dir = f"{output_dir}/{dataset}/{assign_owner_mode}+{assign_record_mode}/alpha-{alpha}-beta-{beta}-copy-{max_copy}-owner-{number_of_data_owner}"

    output_dir = set_output_dir(output_dir)

    for index in range(len(tables)): 
        # generate seller list
        assign_records_to_owners(alpha, owners[index], max_copy, assign_record_equally, dataset, tables[index], output_dir) 

def assign_owners_to_tables(beta, assign_owner_equally, number_of_data_owner, dataset, table):
    if assign_owner_equally:
        return assign_owners_to_tables_equally(dataset, number_of_data_owner, table)
    else:
        return assign_owners_to_tables_inequally(beta, number_of_data_owner, dataset, table)

def assign_owners_to_tables_equally(dataset, number_of_data_owner, tables):
    table_sellers_count_list = []
    
    if dataset.startswith("world"):
        table_sellers_count_list = np.array([number_of_data_owner,number_of_data_owner,number_of_data_owner])    
        
    if dataset.startswith("tpch"):
        sellers_count_list = []
        for table in tables:
            if table == "region" or table == "nation":
                sellers_count_list.append(1)
            else:
                sellers_count_list.append(number_of_data_owner)
        table_sellers_count_list = np.array(sellers_count_list)
    
    table_sellers_count_list = table_sellers_count_list.astype(int)
    sorted_table_to_sellers_list = []
    start = 0
    for i in range(len(table_sellers_count_list)):
        end = int(start + table_sellers_count_list[i])
        sorted_table_to_sellers_list.append([*range(start,end)])
        start += table_sellers_count_list[i]
    
    return sorted_table_to_sellers_list

def assign_owners_to_tables_inequally(beta, number_of_data_owner, dataset, tables):
    config = get_config(dataset)

    default_basic_owner = config.get("default_basic_owner")

    table_sellers_count_list = []
    
    if dataset.startswith("world"):
        sellers_count_list = []
        for table in tables:
            if table == 'city':
                sellers_count_list.append(number_of_data_owner)
            else:
                sellers_count_list.append(default_basic_owner)
        table_sellers_count_list = np.array(sellers_count_list)    
    
    if dataset.startswith("tpch"):
        sellers_count_list = []
        for table in tables:
            if table == 'lineitem':
                sellers_count_list.append(number_of_data_owner)
            else:
                sellers_count_list.append(default_basic_owner)
        table_sellers_count_list = np.array(sellers_count_list)  
    
    
    table_sellers_count_list = table_sellers_count_list.astype(int)
    sorted_table_to_sellers_list = []
    start = 0
    for i in range(len(table_sellers_count_list)):
        end = int(start + table_sellers_count_list[i])
        sorted_table_to_sellers_list.append([*range(start,end)])
        start += table_sellers_count_list[i]
    
    return sorted_table_to_sellers_list

def assign_records_to_owners(alpha, owners, max_copy, assign_record_equally, dataset, table, output_dir):
    if assign_record_equally:
         (data, records) = assign_records_to_owners_for_table_equally(alpha, owners, max_copy, dataset, table)
    else:
        (data, records) =  assign_records_to_owners_for_table_inequally(alpha, owners, max_copy, dataset, table, beta)
    
    save_metadata_to_dir(output_dir, data, records, table)
    print_owners_and_records(data, records)

def assign_records_to_owners_for_table_equally(alpha, owners, max_copy, dataset, table):
    config = get_config(dataset)
    number_of_records = int(config.get(table))

    if not ( 
        (dataset.startswith("world") and table == "city" ) or 
        (dataset.startswith("tpch") and table == "lineitem")
        ):
        alpha = config.get("default_alpha")
        max_copy = config.get("default_max_copy")

    print("table: ", table)
    print("alpha: ", alpha)
    print("max_copy: ", max_copy)
    number_of_records_per_copy = get_number_of_records_by_zipfian(alpha, 
                                                    min(len(owners),max_copy), 
                                                    number_of_records)    
    record_list = []
    data_owner_list = []
    
    # Assign each record to sellers
    record_index = 0
    current_number_of_copy = 1
    for count in number_of_records_per_copy:
        for i in range(0, count):
            owners_for_a_record = sample(owners, min(len(owners), current_number_of_copy))
            for owner in owners_for_a_record:
                record_list.append(record_index)
                data_owner_list.append(owner)
            record_index += 1
        current_number_of_copy += 1
    
    records = [i for i in range(number_of_records)]
    shuffle(records)
    
    data = {"index": record_list, "seller": data_owner_list}
    
    return (data, records)

def assign_records_to_owners_for_table_inequally(alpha, owners, max_copy, dataset, table, beta):
    config = get_config(dataset)
    number_of_records = int(config.get(table))

    if not ( 
        (dataset.startswith("world") and table == "city" ) or 
        (dataset.startswith("tpch") and table == "lineitem")
        ):
        alpha = config.get("default_alpha")
        max_copy = config.get("default_max_copy")

    print("table: ", table)
    print("alpha: ", alpha)
    print("max_copy: ", max_copy)
    number_of_records_per_copy = get_number_of_records_by_zipfian(alpha, 
                                                    min(len(owners),max_copy), 
                                                    number_of_records)    
    record_list = []
    data_owner_list = []
    
    # Assign each record to owners
    record_index = 0
    current_number_of_copy = 1
    
    pvals = get_zipfian(beta, len(owners))
    for count in number_of_records_per_copy:
        for i in range(0, count):
            owners_for_a_record = np.random.choice(owners, current_number_of_copy,
                                                   False, pvals)
            for owner in owners_for_a_record:
                record_list.append(record_index)
                data_owner_list.append(int(owner))
            record_index += 1
        current_number_of_copy += 1

    assign_records_to_owners_without_records(owners, data_owner_list)
    
    records = [i for i in range(number_of_records)]
    shuffle(records)
    
    data = {"index": record_list, "seller": data_owner_list}
    
    return (data, records)


def assign_records_to_owners_without_records(seller_list, data_owner_list):
    owners_without_records = list(set(seller_list) - set(data_owner_list))
    len_before = len(data_owner_list)
    if len(owners_without_records) > 0:
        most_common = max(data_owner_list, key = data_owner_list.count)
        print(most_common)
        assert data_owner_list.count(most_common) > len(owners_without_records)

        current_index = 0
        for index in range(len(data_owner_list)):
            if data_owner_list[index] == most_common:
                data_owner_list[index] = owners_without_records[current_index]
            
                current_index += 1
                if current_index == len(owners_without_records):
                    break
    len_after = len(data_owner_list)
    assert(len_before == len_after)

def save_metadata_to_dir(output_dir, data, records, table):
    record_to_seller_df = pd.DataFrame(data)
    record_to_seller_df.to_json(f'{output_dir}/{table}-seller.json')
    
    with open(f'{output_dir}/{table}-index.json', 'w') as json_file:
        json.dump(records, json_file)

    return 

def print_owners_and_records(data, records):
    owners_and_reocords = pl.DataFrame(data)

    q = (
    owners_and_reocords.lazy()
    .groupby("seller")
    .agg(
        [
            pl.count("seller"),
        ]
        )
    )

    df = q.collect()
    print('seller_count: ', df['seller_count'].to_list())

def get_number_of_records_by_zipfian(alpha_zipfian, number_of_copy, number_of_records):
    pvals = get_zipfian(alpha_zipfian, number_of_copy)

    number_of_records_per_copy = np.array(pvals) * number_of_records
    number_of_records_per_copy = number_of_records_per_copy.astype(int)
    total_count = np.sum(number_of_records_per_copy)

    if total_count < number_of_records:
        number_of_records_per_copy[0] = number_of_records_per_copy[0] +  number_of_records - total_count
    
    return number_of_records_per_copy

def get_zipfian(a, k):
    probs = np.zeros(k)
    for i in range(1, k+1):
        probs[i-1] = 1/pow(i,a)
    
    total_prob = sum(probs)
    probs = probs/total_prob
    return probs


if __name__ == "__main__":
    # print(sys.argv[1:])

    if len(sys.argv) != 17:
        print("incorrect number of inputs")
        print_usage()
        sys.exit(1)
    
    (dataset, alpha, beta, number_of_data_owner, max_copy, assign_owner_equally, assign_record_equally, output_dir) = get_parameter(sys.argv[1:])
    print("dataset: ", dataset)
    print("alpha: ", alpha)
    print("beta: ", beta)
    print("number_of_data_owner: ", number_of_data_owner)
    print("max_copy: ", max_copy)
    print("assign_owner_equally: ", assign_owner_equally)
    print("assign_record_equally: ", assign_record_equally)

    assign_data(dataset, alpha, beta, number_of_data_owner, max_copy, assign_owner_equally, assign_record_equally, output_dir)