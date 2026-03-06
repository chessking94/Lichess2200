import logging
import os
from pathlib import Path

import steps

from Utilities_Python import misc

CONFIG_FILE = os.path.join(Path(__file__).parents[1], 'config.json')


def main():
    script_name = Path(__file__).stem
    _ = misc.initiate_logging(script_name, CONFIG_FILE)

    pending_files = steps.files_to_process()

    if len(pending_files) == 0:
        logging.info('No files pending download')
        raise SystemExit

    for online_file in pending_files:
        dbExport = steps.DatabaseExport(online_file)
        dbExport.process_export()


if __name__ == '__main__':
    main()
