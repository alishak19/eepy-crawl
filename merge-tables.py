import difflib
import os
import shutil
import threading

import hashlib

num_conflicting_pages_under_70_similarity = 0

def compute_checksum(file_path, algorithm="sha256"):
    """
    Compute the checksum of a file.
    :param file_path: Path to the file.
    :param algorithm: Hashing algorithm ('md5', 'sha1', 'sha256').
    :return: Hexadecimal checksum as a string.
    """
    hash_func = getattr(hashlib, algorithm)()
    with open(file_path, "rb") as f:
        # Read the file in chunks to handle large files
        for chunk in iter(lambda: f.read(4096), b""):
            hash_func.update(chunk)
    return hash_func.hexdigest()

def merge_final_tables(table1_path, table2_path, merged_table_path, table_name, identicalKeyConflictResolver):
    """
    Merge two tables from given paths into one table.
    :param table1_path: Path to the first table folder.
        EX: /path/to/table1

    :param table2_path: Path to the second table folder.
        EX: /path/to/table2

    :param merged_table_path: Path where the merged table will be stored.
        EX: /path/to/merged-table

    :param table_name: Name of the table to merge
        EX: pt-crawl
    """

    # If merged_table_path already exists, ask the user if they really want to override it
    if os.path.exists(merged_table_path):
        response = input(f"The merged table path {merged_table_path} already exists. Do you want to override it? (yes/no): ")
        if response.lower() != "yes":
            print("Merge operation aborted.")
            return
        else :
            shutil.rmtree(merged_table_path)
    
    # if table1 or table2 doesn't exist, return error
    if not os.path.exists(table1_path):
        print("Failed with error: table 1 does not exist!")
        return
    if not os.path.exists(table2_path):
        print("Failed with error: table 2 does not exist!")
        return
    
    os.makedirs(merged_table_path)

    # Get list of worker directories in each table
    # EX: ['worker1', 'worker2', 'worker3']
    worker_folder_dir_1 = [os.path.join(table1_path, d) for d in os.listdir(table1_path) if os.path.isdir(os.path.join(table1_path, d))]
    worker_folder_dir_2 = [os.path.join(table2_path, d) for d in os.listdir(table2_path) if os.path.isdir(os.path.join(table2_path, d))]

    # Make sure the worker directory names match
    if set([os.path.basename(f) for f in worker_folder_dir_1]) != set([os.path.basename(f) for f in worker_folder_dir_2]):
        print("Failed to merge: worker folders do not match")
        return
    
    threads = []
    for worker_folder_dir in worker_folder_dir_1:
        thread = threading.Thread(target=process_worker, args=(worker_folder_dir, table1_path, table2_path, merged_table_path, table_name, identicalKeyConflictResolver))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print(f"Tables merged successfully into {args.merged_table_path}")

def process_worker(worker_folder_dir, table1_path, table2_path, merged_table_path, table_name, identicalKeyConflictResolver):
    # get the worker folder name
    # EX: 'worker1'
    worker_folder_name = os.path.basename(worker_folder_dir)

    # Get the ID file path for each worker folder in both tables
    # EX: /path/to/table1/worker1/id
    worker_folder_path_in_1 = os.path.join(table1_path, worker_folder_name)
    id_file1 = os.path.join(worker_folder_path_in_1, "id")
    worker_folder_path_in_2 = os.path.join(table2_path, worker_folder_name)
    id_file2 = os.path.join(worker_folder_path_in_2, "id")
    
    if not os.path.exists(id_file1) or not os.path.exists(id_file2):
        print(f"Failed to merge: ID file missing in {worker_folder_path_in_1} or {worker_folder_path_in_2}")
        return
    
    # Get the target worker folder path in the merged table
    # EX: /path/to/merged-table/worker1
    target_worker_folder = os.path.join(merged_table_path, os.path.basename(worker_folder_name))
    if not os.path.exists(target_worker_folder):
        os.makedirs(target_worker_folder)
    
    # Copy ID file from final table 1 to the merged table
    target_id_file = os.path.join(target_worker_folder, "id")
    if not os.path.exists(target_id_file):
        shutil.copy(id_file1, target_id_file)
        print("Copied ID file from worker folder " + 
              worker_folder_name + 
              " in table " + os.path.basename(table1_path) + 
              " to the merged table")
        
    # For each final table, get the directory at level of the table_name we want to merge
    # EX: /path/to/table1/worker1/pt-crawl
    table_folder1 = os.path.join(worker_folder_path_in_1, table_name)
    table_folder2 = os.path.join(worker_folder_path_in_2, table_name)

    if not os.path.exists(table_folder1) or not os.path.exists(table_folder2):
        print(f"Failed to merge: {table_name} folder missing in {table_folder1} or {table_folder2}")
        return
    
    # Get the list of directory names for both tables with their full paths
    # EX: {'__ac': 'table1/worker1/pt-crawl/__ac', '__ao': 'table1/worker1/pt-crawl/__ao'}
    dirs1_full = {d: os.path.join(table_folder1, d) for d in os.listdir(table_folder1) if os.path.isdir(os.path.join(table_folder1, d))}
    dirs2_full = {d: os.path.join(table_folder2, d) for d in os.listdir(table_folder2) if os.path.isdir(os.path.join(table_folder2, d))}

    # Extract the relative paths
    # EX: {'__ac', '__ao'}
    dirs1_relative = set(dirs1_full.keys())
    dirs2_relative = set(dirs2_full.keys())

    # Find shared and differing relative paths
    shared_dirs_relative = dirs1_relative & dirs2_relative
    differing_dirs_relative = dirs1_relative ^ dirs2_relative

    differing_dirs_only_from_1 = differing_dirs_relative & dirs1_relative
    differing_dirs_only_from_2 = differing_dirs_relative & dirs2_relative

    print("\nIDENTICAL DIRECTORY NAME processing ---------\n")

    # Process shared directories (sharded by ID)
    for d in shared_dirs_relative:

        # full_path_in_1 = /path/to/table1/worker1/pt-crawl/__ac
        full_path_in_1 = table_folder1 + "/" + d
        files_in_1 = [os.path.join(full_path_in_1, f) for f in os.listdir(full_path_in_1) if os.path.isfile(os.path.join(full_path_in_1, f))]
        
        # full_path_in_2 = /path/to/table2/worker1/pt-crawl/__ac
        full_path_in_2 = table_folder2 + "/" + d
        files_in_2 = [os.path.join(full_path_in_2, f) for f in os.listdir(full_path_in_2) if os.path.isfile(os.path.join(full_path_in_2, f))]

        # Extract the relative paths of the files
        # EX: {'aclsfmyimmacwkjesehaoqusrqykmgfghqealqgo', 'acgoqkfofgrieoyecofgvaqmimpipiggcqxijocg', ...}
        files_relative_in_1 = set([os.path.basename(f) for f in files_in_1])
        files_relative_in_2 = set([os.path.basename(f) for f in files_in_2])
        
        # Find shared and differing relative paths
        shared_files_relative = files_relative_in_1 & files_relative_in_2
        differing_files_relative = files_relative_in_1 ^ files_relative_in_2

        differing_files_only_from_1 = differing_files_relative & files_relative_in_1
        differing_files_only_from_2 = differing_files_relative & files_relative_in_2

        # print("diff files 1: ", differing_files_only_from_1)
        # print("diff files 2: ", differing_files_only_from_2)

        print("\n IDENTICAL FILE NAME processing ---------\n")

        # Check if shared files have identical content, otherwise merge / fail them
        for f in shared_files_relative:
            file_path_in_1 = full_path_in_1 + "/" + f
            file_path_in_2 = full_path_in_2 + "/" + f

            # Check if the checksums of the files are the same
            # If they are different, then we have two files with the same KEY but different VALUES
            if compute_checksum(file_path_in_1) == compute_checksum(file_path_in_2):
                # if the directory does not exist in the merged table, create it
                if not os.path.exists(os.path.join(target_worker_folder, table_name, d)):
                    os.makedirs(os.path.join(target_worker_folder, table_name, d))
                
                # Copy the file from final table 1 to the merged table
                target_file = os.path.join(target_worker_folder, table_name, d, f)
                if not os.path.exists(target_file):
                    shutil.copy(file_path_in_1, target_file)
                    print("Copied file " + f + " from " + os.path.basename(table1_path) + " to the merged table")

            else:
                # TODO: MERGE operation: what happens when two files have the same key but have different values?
                # This is dependent on the type of table
                # pt-crawl: select the file with the most recent timestamp
                # pt-pagerank: combine the pageranks
                print(f"Merge Conflict: {file_path_in_1} and {file_path_in_2} have the same KEY but different VALUES")

                # if the directory does not exist in the merged table, create it
                if not os.path.exists(os.path.join(target_worker_folder, table_name, d)):
                    os.makedirs(os.path.join(target_worker_folder, table_name, d))

                identicalKeyConflictResolver(table1_path, table2_path, file_path_in_1, file_path_in_2, target_worker_folder, table_name, d, f)

        print("\n DIFFERING FILE NAME processing --------- \n")

        for f in differing_files_only_from_1:
            file_path_in_1 = full_path_in_1 + "/" + f
            target_file = os.path.join(target_worker_folder, table_name, d, f)
            if not os.path.exists(target_file):
                
                # make directory if it doesn't exist
                if not os.path.exists(os.path.join(target_worker_folder, table_name, d)):
                    os.makedirs(os.path.join(target_worker_folder, table_name, d))

                shutil.copy(file_path_in_1, target_file)
                print("Copied differing file " + f + " from " + os.path.basename(table1_path) + " to the merged table")
            
        for f in differing_files_only_from_2:
            file_path_in_2 = full_path_in_2 + "/" + f
            target_file = os.path.join(target_worker_folder, table_name, d, f)
            if not os.path.exists(target_file):

#                make directory if it doesn't exist
                if not os.path.exists(os.path.join(target_worker_folder, table_name, d)):
                    os.makedirs(os.path.join(target_worker_folder, table_name, d))

                shutil.copy(file_path_in_2, target_file)
                print("Copied differing file " + f + " from " + os.path.basename(table2_path) + " to the merged table")

    print("\n DIFFERING DIRECTORY NAME processing --------- \n")

    # Process differing directories (sharded by ID)
    for d in differing_dirs_only_from_1:
        full_path_in_1 = table_folder1 + "/" + d
        files_in_1 = [os.path.join(full_path_in_1, f) for f in os.listdir(full_path_in_1) if os.path.isfile(os.path.join(full_path_in_1, f))]

        # if the directory does not exist in the merged table, create it
        if not os.path.exists(os.path.join(target_worker_folder, table_name, d)):
            os.makedirs(os.path.join(target_worker_folder, table_name, d))

        for f in files_in_1:
            target_file = os.path.join(target_worker_folder, table_name, d, os.path.basename(f))
            if not os.path.exists(target_file):
                shutil.copy(f, target_file)
                print("Copied file " + f + " from " + os.path.basename(table1_path) + " to the merged table")

    for d in differing_dirs_only_from_2:
        full_path_in_2 = table_folder2 + "/" + d
        files_in_2 = [os.path.join(full_path_in_2, f) for f in os.listdir(full_path_in_2) if os.path.isfile(os.path.join(full_path_in_2, f))]

        # if the directory does not exist in the merged table, create it
        if not os.path.exists(os.path.join(target_worker_folder, table_name, d)):
            os.makedirs(os.path.join(target_worker_folder, table_name, d))

        for f in files_in_2:
            target_file = os.path.join(target_worker_folder, table_name, d, os.path.basename(f))
            if not os.path.exists(target_file):
                shutil.copy(f, target_file)
                print("Copied file " + f + " from " + os.path.basename(table2_path) + " to the merged table")

def identical_file_resolver_crawl(table1_path, table2_path, file_path_in_1, file_path_in_2, target_worker_folder, table_name, d, f):

    # print the file contents
    with open(file_path_in_1, "r", encoding="ISO-8859-1") as f:
        file1_contents = f.readlines()
    with open(file_path_in_2, "r", encoding="ISO-8859-1") as f:
        file2_contents = f.readlines()
    # print("File 1 contents: ", file1_contents)
    # print("File 2 contents: ", file2_contents)

    # print how similar the files are to each other
    similarity_score = difflib.SequenceMatcher(None, file1_contents, file2_contents).ratio()
    # print("Similarity: ", similarity_score)

    if similarity_score < 0.7:
        print("Warning: Similarity score is < 0.7")
        print("Similarity score was: ", similarity_score)
        num_conflicting_pages_under_70_similarity += 1

    # Copy the file which has the most recent timestamp
    if os.path.getmtime(file_path_in_1) > os.path.getmtime(file_path_in_2):
        file = os.path.basename(file_path_in_1)
        target_file_path = target_worker_folder + "/" + table_name + "/" + d + "/" + file
        target_file = os.path.join(target_file_path)
        if not os.path.exists(target_file):
            shutil.copy(file_path_in_1, target_file)
            print("Copied file " + file + " from " + os.path.basename(table1_path) + " to the merged table because it has the most recent timestamp")
    else:
        file = os.path.basename(file_path_in_2)
        target_file_path = target_worker_folder + "/" + table_name + "/" + d + "/" + file
        target_file = os.path.join(target_file_path)
        if not os.path.exists(target_file):
            shutil.copy(file_path_in_2, target_file)
            print("Copied file " + file + " from " + os.path.basename(table2_path) + " to the merged table because it has the most recent timestamp")

def parseFileContents(file_contents):
    # EX: acgoqkfofgrieoyecofgvaqmimpipiggcqxijocg rank 19 0.15099369888172004
    pageranks = {}
    for line in file_contents:
        line = line.strip().split()
        pageranks[line[0]] = float(line[3])
    
    return pageranks

def identical_file_resolver_pagerank(table1_path, table2_path, file_path_in_1, file_path_in_2, target_worker_folder, table_name, d, f):
    # EX: acgoqkfofgrieoyecofgvaqmimpipiggcqxijocg rank 19 0.15099369888172004

    # TODO:
    # Parse each file
    # Combine their ranks: SUM them together
    # Write new rank to file

    # Get the file contents of file_path_in_1
    with open(file_path_in_1, "r") as f:
        file1_contents = f.readlines()
        
    # Get the file contents of file_path_in_2
    with open(file_path_in_2, "r") as f:
        file2_contents = f.readlines()

    # Parse the file contents
    pageranks = parseFileContents(file1_contents)
    pageranks2 = parseFileContents(file2_contents)

    url1 = pageranks[0]
    url2 = pageranks2[0]
    rank = "rank"
    length1 = pageranks[2]
    length2 = pageranks2[2]
    new_rank1 = pageranks[3]
    new_rank2 = pageranks2[3]

    # combine the two newranks by parsing string as double and adding them together
    new_rank = float(new_rank1) + float(new_rank2)

    # set new_length to be the length of the new_rank as a string
    new_length = str(len(str(new_rank)))

    if url1 != url2:
        print("Failed to merge file because the URLs in the file don't match despite having the same KEY")
        return
    
    # Write the new rank to the file
    with open(os.path.join(target_worker_folder, table_name, d, f), "w") as f:
        f.write(f"{url1} {rank} {new_length} {new_rank}")
        print(f"Combined pageranks for {url1} and wrote to the merged table")

def get_table_size(table_path):
    """
    Get the size of the table
    :param table_path: Path to the table folder.
        EX: /path/to/table

    :return: Size of the table in bytes
    """
    table_size = 0
    for root, dirs, files in os.walk(table_path):
        for file in files:
            file_path = os.path.join(root, file)
            table_size += os.path.getsize(file_path)
    return table_size

if __name__ == "__main__":
    import argparse

    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Merge two final tables into one.")
    parser.add_argument("table1_path", help="Path to the first table folder.")
    parser.add_argument("table2_path", help="Path to the second table folder.")
    parser.add_argument("merged_table_path", help="Path where the merged table will be stored.")
    parser.add_argument("table_name", help="Name of the table to merge (e.g., 'pt-crawl').")

    args = parser.parse_args()

    num_conflicting_pages_under_70_similarity = 0

    if args.table_name == "pt-crawl":
        merge_final_tables(args.table1_path, args.table2_path, args.merged_table_path, args.table_name, identical_file_resolver_crawl)
    elif args.table_name == "pt-pagerank":
        merge_final_tables(args.table1_path, args.table2_path, args.merged_table_path, args.table_name, identical_file_resolver_pagerank)
        # print("Failed to merge: pt-pagerank not implemented")
    else:
        print("Failed to merge: invalid table name")

    # find the size of the input table
    print("Size of table 1: ", get_table_size(args.table1_path))
    print("Size of table 2: ", get_table_size(args.table2_path))

    # find the size of the output table
    print("Size of merged table: ", get_table_size(args.merged_table_path))
