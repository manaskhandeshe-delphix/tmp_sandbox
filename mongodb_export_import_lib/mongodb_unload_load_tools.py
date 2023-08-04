import os
import subprocess
from multiprocessing import Process
import time



def timer_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        elapsed_time_in_seconds = end_time - start_time
        minutes, seconds = divmod(elapsed_time_in_seconds, 60)
        print(f"{func.__name__} took {minutes:02f}m:{seconds:02f}s to run.")
        return result
    return wrapper


def run_cmd(cmd, check=True):
    """
    Run a shell command as a subprocess.

    Args:
        cmd (list): The command to run, as a list of strings.
        check (bool, optional): Whether to raise an exception if the command fails. Defaults to True.

    Returns:
        CompletedProcess: A subprocess.CompletedProcess object representing the completed process.
    """
    print(f'Running command: {" ".join(cmd)}')
    #result = subprocess.run(cmd, capture_output=True, check=check)
    #print(f'Stdout: {result.stdout.decode().strip()}')
    #print(f'Stderr: {result.stderr.decode().strip()}')
    #return result
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    stdout_raw = ''
    for line in process.stdout:
        print(line, end='')
        stdout_raw += line
    return stdout_raw.strip()


@timer_decorator
def export_mongodb_collection(db_name, collection_name, export_dir,
        batch_size, mongo_uri, config_path, username, dump_file_type='dump_file_type', fields=[]):
    os.makedirs(export_dir, exist_ok=True)

    #count = int(run_cmd([
    #    'mongosh',
    #    f'{mongo_uri}/admin',
    #    '--quiet',
    #    '--eval',
    #    f'db.getSiblingDB("{db_name}").{collection_name}.countDocuments()'
    #]).stdout.decode().strip())
    count = int(run_cmd([
        'mongosh',
        f'{mongo_uri}/admin',
        '--quiet',
        '--eval',
        f'db.getSiblingDB("{db_name}").{collection_name}.countDocuments()'
    ]).strip())

    if fields:
        fields_str = f'--fields={",".join(fields)}'
    else:
        fields_str = ""

    start = 0
    end = batch_size

    export_processes = []
    while end < count:
        filename = f'{export_dir}/{collection_name}-{start}-{end}.{dump_file_type}'
        export_command = ['mongoexport', '--config', config_path, '--db', db_name, '--collection', collection_name, '--type', dump_file_type, '--query', '{}', '--out', filename, '--limit', str(batch_size), '--skip', str(start), '--authenticationDatabase=admin', '--username', username, fields_str]
        #print("Executing [{}] in the background..".format(" ".join(export_command)))
        export_process = Process(target=run_cmd, args=(export_command, False))
        export_processes.append(export_process)
        export_process.start()
        start = end
        end += batch_size

    # export the last batch
    filename = f'{export_dir}/{collection_name}-{start}-{count}.{dump_file_type}'
    export_command = ['mongoexport', '--config', config_path, '--db', db_name, '--collection', collection_name, '--type', dump_file_type, '--query', '{}', '--out', filename, '--limit', str(count - start), '--skip', str(start), '--authenticationDatabase=admin', '--username', username, fields_str]
    export_process = Process(target=run_cmd, args=(export_command, False))
    export_processes.append(export_process)
    export_process.start()

    # wait for all background export jobs to finish
    print("Waiting for all the above commands to finish..")
    for process in export_processes:
        process.join()

    return [os.path.join(export_dir, filename) for filename in os.listdir(export_dir) if filename.endswith(f'.{dump_file_type}')]


@timer_decorator
def import_mongodb_collection(export_files, db_name, collection_name, config_path, username, dump_file_type='json', fields=[]):
    # import the exported files into a new collection
    import_commands = []

    if fields:
        fields_str = f'--fields={",".join(fields)}'
    else:
        fields_str = ""

    for filename in export_files:
        import_command = ['mongoimport', '--config', config_path, '--db', db_name, '--collection', collection_name, '--type', dump_file_type, '--file', filename, '--authenticationDatabase=admin', '--username', username, fields_str]
        import_commands.append(import_command)

    # run import commands sequentially
    for command in import_commands:
        run_cmd(command)
