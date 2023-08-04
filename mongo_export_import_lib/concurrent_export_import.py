from mongodb_unload_load_tools import export_mongodb_collection, import_mongodb_collection, run_cmd
import shutil


def remove_files_or_directories(file_path):
    try:
        shutil.rmtree(file_path)
    except OSError as e:
        print(f"Error: {e.strerror}")


def export_and_import_mongodb_collection(db_name, collection_name, export_dir, import_collection_name, batch_size, mongo_uri, config_path, username, dump_file_type, fields):
    print("=========== EXPORT START ===========")
    export_files = export_mongodb_collection(db_name, collection_name, export_dir, batch_size, mongo_uri, config_path, username, dump_file_type, fields)
    print("=========== EXPORT END =============")
    print("\n\n")

    # delete an existing table to be imported 
    #import pdb
    #pdb.set_trace()
    print(f"Deleting collection {import_collection_name}, if it already exists.")
    run_cmd([
        'mongosh',
        f'{mongo_uri}/admin',
        '--quiet',
        '--eval',
        f'db.getSiblingDB("{db_name}").{import_collection_name}.remove({{}})'
    ])

    print("=========== IMPORT START ===========")
    import_mongodb_collection(export_files, db_name, import_collection_name, config_path, username, dump_file_type, fields)
    print("=========== IMPORT END =============")


if __name__ == '__main__':
    DB_NAME = 'sample_mflix'
    COLLECTION_NAME = 'my_favourite_movies'
    #COLLECTION_NAME = 'movies'
    EXPORT_DIR = './_export'
    IMPORT_COLLECTION_NAME = 'imported_my_favourite_movies'
    #IMPORT_COLLECTION_NAME = 'csv_movies'
    BATCH_SIZE = 500000
    CONFIG_PATH = "/home/delphix/mongod_creds.conf"
    MONGO_URI = "mongodb://dlpxadmin:delphix@mkk-prod-src.dcol1.delphix.com:58270"
    USERNAME = "dlpxadmin"
    #DUMP_FILE_TYPE = "json"
    DUMP_FILE_TYPE = "csv"
    FIELDS = ['plot', 'genres', 'runtime', 'cast', 'num_mflix_comments', 'title', 'fullplot', 'countries', 'released', 'directors', 'rated', 'awards', 'lastupdated', 'year', 'imdb', 'type', 'tomatoes', 'poster', 'languages', 'writers', 'metacritic']

    print("Cleaning up directory {EXPORT_DIR} before proceeding.")
    remove_files_or_directories(EXPORT_DIR)

    export_and_import_mongodb_collection(
        DB_NAME,
        COLLECTION_NAME,
        EXPORT_DIR,
        IMPORT_COLLECTION_NAME,
        BATCH_SIZE,
        MONGO_URI,
        CONFIG_PATH,
        USERNAME,
        DUMP_FILE_TYPE,
        FIELDS,
    )
