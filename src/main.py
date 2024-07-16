import datetime as dt
import logging
import os
import shutil
import sys

import func
import steps


def main():
    pending_files = steps.files_to_process()

    if len(pending_files) == 0:
        print('No files to process')
        raise SystemExit

    for online_file in pending_files:
        dte = dt.datetime.now().strftime('%Y%m%d%H%M%S')
        scr_nm = os.path.splitext(os.path.basename(__file__))[0]
        log_path = func.get_config(os.path.dirname(os.path.dirname(__file__)), 'logPath')
        if not os.path.isdir(log_path):
            os.mkdir(log_path)
        log_name = scr_nm + '_' + dte + '.log'
        log_full = os.path.join(log_path, log_name)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s\t%(funcName)s\t%(levelname)s\t%(message)s',
            handlers=[
                logging.FileHandler(log_full),
                logging.StreamHandler(sys.stdout)
            ]
        )

        log_file = func.get_config(os.path.dirname(os.path.dirname(__file__)), 'logFile')

        # download file
        logging.info('Process started')
        logging.info('Download started')
        db_filename = os.path.splitext(online_file.split('/')[-1])[0]
        steps.write_log(db_filename, 'Download_Start', 'GETDATE()')
        file_root = func.get_config(os.path.dirname(os.path.dirname(__file__)), 'fileRoot')
        file_name, file_path = steps.download_file(online_file, file_root)
        steps.write_log(db_filename)
        logging.info('Download ended')
        steps.write_log(db_filename, 'Download_End', 'GETDATE()')

        # decompress file
        logging.info('Decompression started')
        steps.write_log(db_filename, 'Decompression_Start', 'GETDATE()')
        extracted_file = steps.decompress(file_path, file_name)
        logging.info('Decompression ended')
        steps.write_log(db_filename, 'Decompression_End', 'GETDATE()')

        yyyy = extracted_file[26:30]
        mm = extracted_file[31:33]

        # create error log
        logging.info('Error log started')
        steps.write_log(db_filename, 'ErrorLog_Start', 'GETDATE()')
        steps.errorlog(file_path, extracted_file, yyyy, mm)
        logging.info('Error log ended')
        steps.write_log(db_filename, 'ErrorLog_End', 'GETDATE()')

        steps.write_log(db_filename, '[2200_Start]', 'GETDATE()')

        # update correspondence game TimeControl tag
        logging.info('Correspondence TimeControl tag update started')
        upd_name = steps.update_timecontrol(file_path, extracted_file, yyyy, mm)
        logging.info('Correspondence TimeControl tag update ended')

        # extract only 2200+ rating games
        logging.info('2200+ game extract started')
        pgn_name = steps.extract2200(file_path, upd_name, yyyy, mm)
        logging.info('2200+ game extract ended')

        # fix date tag if file is earlier than 201804
        bad_dates = False
        if int(yyyy + mm) <= 201803:
            bad_dates = True
            logging.info(f'Filedate is {yyyy}{mm}, Date tag update started')
            new_pgn_name_2 = steps.fix_datetag(file_path, pgn_name, yyyy, mm, False)
            curr_name = new_pgn_name_2
            logging.info(f'Filedate is {yyyy}{mm}, Date tag update ended')
        else:
            curr_name = pgn_name

        # separate into time control files
        tc_files = steps.split_timecontrol(file_path, curr_name, yyyy, mm)
        steps.write_log(db_filename, '[2200_End]', 'GETDATE()')

        # split pgn into corr games
        steps.write_log(db_filename, 'Corr_Start', 'GETDATE()')
        logging.info('Complete correspondence game extract started')
        corr_name = steps.extractcorr(file_path, upd_name, yyyy, mm)
        logging.info('Complete correspondence game extract ended')

        # fix date tag if file is earlier than 201804
        new_name = f'lichess_correspondence_{yyyy}{mm}.pgn'
        if int(yyyy + mm) <= 201803:
            logging.info(f'Filedate is {yyyy}{mm}, Corr Date tag update started')
            steps.fix_datetag(file_path, corr_name, yyyy, mm, True)
            logging.info(f'Filedate is {yyyy}{mm}, Corr Date tag update ended')
        else:
            os.rename(os.path.join(file_path, corr_name), os.path.join(file_path, new_name))

        logging.info('Review for ongoing correspondence games started')
        completed_file, ctr = steps.ongoing_corr(file_path, new_name)
        tc_files.append(completed_file)
        logging.info(f'Total of {ctr} ongoing correspondence games')
        logging.info('Review for ongoing correspondence games ended')
        steps.write_log(db_filename, 'Corr_End', 'GETDATE()')

        # clean up old files
        os.remove(os.path.join(file_path, extracted_file))
        os.remove(os.path.join(file_path, upd_name))
        os.remove(os.path.join(file_path, pgn_name))
        os.remove(os.path.join(file_path, new_name))
        if bad_dates:
            os.remove(os.path.join(file_path, new_pgn_name_2))
            os.remove(os.path.join(file_path, corr_name))

        # write to log file
        logging.info('Counting games started')
        # 2200 game counts
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

        logging.info('Counting games ended')

        # review for recently completed correspondence games
        token_value = func.get_conf('LichessAPIToken')
        game_url = 'https://lichess.org/api/games/export/_ids'
        dload_path = os.path.join(file_root, 'temp')
        dest_path = file_path

        logging.info('Review for recently completed correspondence games started')
        steps.completed_corr_pending(token_value, game_url)
        logging.info('Review for recently completed correspondence games ended')

        # download newly completed games and delete table records if download succeeds
        logging.info('Download for recently completed correspondence games started')
        running_total = steps.completed_corr_download(token_value, game_url, dload_path)
        with open(log_file, 'a') as f:
            f.write(str(running_total) + '\n')  # count of newly completed games
        logging.info('Download for recently completed correspondence games ended')
        steps.write_log(db_filename, 'Corr_Additional', running_total)

        # verify files were downloaded before continuing
        if running_total == 0:
            logging.warning('No recently completed correspondence games found, process ended')
            raise SystemExit

        # merge newly downloaded pgns
        logging.info('Merge of recently completed correspondence games started')
        merge_name = steps.merge_files(dload_path)
        logging.info('Merge of recently completed correspondence games ended')

        # update TimeControl tag
        logging.info('Recently completed correspondence TimeControl tag update started')
        upd_name = steps.update_timecontrol(dload_path, merge_name, yyyy, mm)
        logging.info('Recently completed correspondence TimeControl tag update ended')

        # sort game file
        logging.info('Sorting of recently completed correspondence games started')
        compcorr_name = steps.sort_gamefile(dload_path, upd_name)
        logging.info('Sorting of recently completed correspondence games ended')

        # clean up
        dir_files = [f for f in os.listdir(dload_path) if os.path.isfile(os.path.join(dload_path, f))]
        for filename in dir_files:
            if filename != compcorr_name:
                fname_relpath = os.path.join(dload_path, filename)
                os.remove(fname_relpath)

        if not os.path.isdir(dest_path):
            os.mkdir(dest_path)
        if os.getcwd != dest_path:
            os.chdir(dest_path)
        old_name = os.path.join(dload_path, compcorr_name)
        new_name = os.path.join(dest_path, compcorr_name)
        os.rename(old_name, new_name)

        # extract first 2000 games from bullet and blitz files for database analysis
        logging.info('2000 game extract of bullet and blitz games started')
        steps.extractbulletblitz(file_path, tc_files, 2000)
        logging.info('2000 game extract of bullet and blitz games ended')

        # create 2200+ corr file for database analysis
        logging.info('2200+ corr game file started')
        steps.extract2200corr(file_path, dload_path, completed_file, compcorr_name)
        logging.info('2200+ corr game file ended')

        if os.getcwd != file_root:
            os.chdir(file_root)
        shutil.rmtree(dload_path)

        logging.info('Process ended')


if __name__ == '__main__':
    main()
