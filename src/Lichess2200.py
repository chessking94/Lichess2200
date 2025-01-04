import logging
import os
from pathlib import Path
import shutil

import steps

from Utilities_Python import misc, notifications

CONFIG_FILE = os.path.join(Path(__file__).parents[1], 'config.json')

# TODO: does it make sense to introduce a class for the download file?


def main():
    script_name = Path(__file__).stem
    _ = misc.initiate_logging(script_name, CONFIG_FILE)

    pending_files = steps.files_to_process()

    if len(pending_files) == 0:
        logging.info('No files pending download')
        raise SystemExit

    for online_file in pending_files:
        msg = f'Begin processing {online_file}'
        notifications.SendTelegramMessage(f'Lichess2200: {msg}')
        logging.info(msg)

        db_filename = os.path.splitext(online_file.split('/')[-1])[0]
        steps.write_log(db_filename)

        # download file
        logging.info('Download started')
        steps.write_log(db_filename, 'Download_Start', 'GETDATE()')
        file_root = misc.get_config('downloadRoot', CONFIG_FILE)
        file_name, file_path = steps.download_file(online_file, file_root)
        steps.write_log(db_filename, 'Download_End', 'GETDATE()')

        # decompress file
        logging.info('Decompression started')
        steps.write_log(db_filename, 'Decompression_Start', 'GETDATE()')
        extracted_file = steps.decompress(file_path, file_name)
        steps.write_log(db_filename, 'Decompression_End', 'GETDATE()')

        yyyy = extracted_file[26:30]
        mm = extracted_file[31:33]

        # create error log
        logging.info('Error log started')
        steps.write_log(db_filename, 'ErrorLog_Start', 'GETDATE()')
        steps.errorlog(file_path, extracted_file, yyyy, mm)
        steps.write_log(db_filename, 'ErrorLog_End', 'GETDATE()')

        # log start of 2200 processing
        steps.write_log(db_filename, '[2200_Start]', 'GETDATE()')

        # update correspondence game TimeControl tag
        logging.info('Correspondence TimeControl tag update started')
        upd_name = steps.update_timecontrol(file_path, extracted_file, yyyy, mm)

        os.remove(os.path.join(file_path, extracted_file))  # file no longer needed

        # extract only 2200+ rating games
        logging.info('2200+ game extract started')
        pgn_name = steps.extract2200(file_path, upd_name, yyyy, mm)

        # fix date tag if file is earlier than 201804
        if int(yyyy + mm) <= 201803:
            logging.info(f'Filedate = {yyyy}{mm}')
            logging.info('Date tag update started')
            new_pgn_name_2 = steps.fix_datetag(file_path, pgn_name, yyyy, mm, False)
            curr_name = new_pgn_name_2
            os.remove(os.path.join(file_path, pgn_name))
        else:
            curr_name = pgn_name

        # separate into time control files
        logging.info('Splitting into time-control files started')
        tc_files = steps.split_timecontrol(file_path, curr_name, yyyy, mm)
        steps.write_log(db_filename, '[2200_End]', 'GETDATE()')
        os.remove(os.path.join(file_path, curr_name))

        # split pgn into corr games
        logging.info('Complete correspondence game extract started')
        steps.write_log(db_filename, 'Corr_Start', 'GETDATE()')
        corr_name = steps.extractcorr(file_path, upd_name, yyyy, mm)
        os.remove(os.path.join(file_path, upd_name))

        # fix date tag if file is earlier than 201804
        new_name = f'lichess_correspondence_{yyyy}{mm}.pgn'
        if int(yyyy + mm) <= 201803:
            logging.info(f'Filedate = {yyyy}{mm}')
            logging.info('Corr Date tag update started')
            _ = steps.fix_datetag(file_path, corr_name, yyyy, mm, True)
            # do not need to set new_name since it's for creating a file, which steps.fix_datetag does for us
            os.remove(os.path.join(file_path, corr_name))
        else:
            os.rename(os.path.join(file_path, corr_name), os.path.join(file_path, new_name))

        logging.info('Review for ongoing correspondence games started')
        completed_file, ctr = steps.ongoing_corr(file_path, new_name)
        tc_files.append(completed_file)
        os.remove(os.path.join(file_path, new_name))
        logging.info(f'Total of {ctr} ongoing correspondence games')
        steps.write_log(db_filename, 'Corr_End', 'GETDATE()')

        # 2200 game counts
        logging.info('Counting games started')
        search_text = '[Event "'
        for tcf in tc_files:
            ct = 0
            if os.path.isfile(os.path.join(file_path, tcf)):
                with open(os.path.join(file_path, tcf), 'r') as ff:
                    for line in ff:
                        if search_text in line:
                            ct = ct + 1

            tc = os.path.splitext(tcf)[0].split('_')[-1]
            if tc in ['Bullet', 'Blitz', 'Rapid', 'Classical']:
                steps.write_log(db_filename, f'{tc}_2200', ct)
            else:
                steps.write_log(db_filename, 'Corr_All', ct)

        # review for recently completed correspondence games
        token_value = os.getenv('LichessAPIToken')
        game_url = 'https://lichess.org/api/games/export/_ids'
        dload_path = os.path.join(file_root, 'temp')
        dest_path = file_path

        logging.info('Review for recently completed correspondence games started')
        steps.completed_corr_pending(token_value, game_url)

        # download newly completed games and delete table records if download succeeds
        logging.info('Download for recently completed correspondence games started')
        running_total = steps.completed_corr_download(token_value, game_url, dload_path)
        steps.write_log(db_filename, 'Corr_Additional', running_total)

        # verify files were downloaded before continuing
        if running_total == 0:
            logging.warning('No recently completed correspondence games found')
            compcorr_name = None
        else:
            pass
            # merge newly downloaded pgns
            logging.info('Merge of recently completed correspondence games started')
            merge_name = steps.merge_files(dload_path)

            # update TimeControl tag
            logging.info('Recently completed correspondence TimeControl tag update started')
            upd_name = steps.update_timecontrol(dload_path, merge_name, yyyy, mm)

            # sort game file
            logging.info('Sorting of recently completed correspondence games started')
            compcorr_name = steps.sort_gamefile(dload_path, upd_name)

            # clean up
            dir_files = [f for f in os.listdir(dload_path) if os.path.isfile(os.path.join(dload_path, f))]
            for filename in dir_files:
                if filename != compcorr_name:
                    fname_relpath = os.path.join(dload_path, filename)
                    os.remove(fname_relpath)

            if not os.path.isdir(dest_path):
                os.mkdir(dest_path)
            if os.path.normpath(os.getcwd()) != os.path.normpath(dest_path):
                os.chdir(dest_path)
            old_name = os.path.join(dload_path, compcorr_name)
            new_name = os.path.join(dest_path, compcorr_name)
            os.rename(old_name, new_name)

        # create 2200+ corr file for database analysis
        logging.info('2200+ corr game file started')
        steps.extract2200corr(file_path, dload_path, completed_file, compcorr_name)

        if os.path.normpath(os.getcwd()) != os.path.normpath(file_root):
            os.chdir(file_root)
        shutil.rmtree(dload_path)

        # extract first 2000 games from bullet and blitz files for database analysis
        logging.info('2000 game extract of bullet and blitz games started')
        steps.extractbulletblitz(file_path, tc_files, 2000)

        # move files from processing directory to final location
        final_root = misc.get_config('finalRoot', CONFIG_FILE)
        final_path = os.path.join(final_root, yyyy)
        if not os.path.isdir(final_path):
            os.mkdir(final_path)

        all_files = [f for f in os.listdir(file_path) if os.path.isfile(os.path.join(file_path, f))]
        move_error = False
        for file in all_files:
            try:
                shutil.move(os.path.join(file_path, file), os.path.join(final_path, file))
            except Exception as e:
                logging.error(f"Unable to move file {file} to {final_path}: {e}")
                move_error = True

        if not move_error:
            if os.path.normpath(os.getcwd()) == os.path.normpath(file_path):
                os.chdir('..')
            shutil.rmtree(file_path)

        msg = f'End processing {online_file}'
        notifications.SendTelegramMessage(f'Lichess2200: {msg}')
        logging.info(msg)


if __name__ == '__main__':
    main()
