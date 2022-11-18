import datetime as dt
import logging
import os
import sys

import func
import steps

# TODO: Verify Corr_Additional game count log write is correct after next run


def main():
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

    logging.info('Process started')
    log_file = func.get_config(os.path.dirname(os.path.dirname(__file__)), 'logFile')
    file_path = func.get_config(os.path.dirname(os.path.dirname(__file__)), 'filePath')
    file_name = func.get_config(os.path.dirname(os.path.dirname(__file__)), 'fileName')

    start_date = dt.datetime.now().strftime('%Y-%m-%d')
    decomp_start = ''
    decomp_end = ''
    error_start = ''
    error_end = ''
    start_2200 = ''
    end_2200 = ''
    start_corr = ''
    end_corr = ''

    # decompress file
    decomp_start = dt.datetime.now().strftime('%H:%M:%S')
    logging.info('Decompression started')
    steps.decompress(file_path, file_name)
    decomp_end = dt.datetime.now().strftime('%H:%M:%S')
    logging.info('Decompression ended')
    extracted_file = file_name.replace('.bz2', '')

    yyyy = extracted_file[26:30]
    mm = extracted_file[31:33]

    # create error log
    error_start = dt.datetime.now().strftime('%H:%M:%S')
    logging.info('Error log started')
    steps.errorlog(file_path, extracted_file, yyyy, mm)
    error_end = dt.datetime.now().strftime('%H:%M:%S')
    logging.info('Error log ended')

    start_2200 = dt.datetime.now().strftime('%H:%M:%S')

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
    end_2200 = dt.datetime.now().strftime('%H:%M:%S')

    # split pgn into corr games
    start_corr = dt.datetime.now().strftime('%H:%M:%S')
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
    completed_file, ctr, comp_ct = steps.ongoing_corr(file_path, new_name)
    tc_files.append(completed_file)
    logging.info(f'Total of {ctr} ongoing correspondence games')
    logging.info('Review for ongoing correspondence games ended')

    end_corr = dt.datetime.now().strftime('%H:%M:%S')

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
    with open(log_file, 'a') as f:
        # timings
        f.write(extracted_file + '\t' + start_date + '\t')
        f.write(decomp_start + '\t' + decomp_end + '\t')
        f.write(error_start + '\t' + error_end + '\t')
        f.write(start_2200 + '\t' + end_2200 + '\t')
        f.write(start_corr + '\t' + end_corr + '\t')

        # 2200 game counts
        search_text = '[Event "'
        for tcf in tc_files:
            ct = 0
            if os.path.isfile(os.path.join(file_path, tcf)):
                with open(os.path.join(file_path, tcf), 'r') as ff:
                    for line in ff:
                        if search_text in line:
                            ct = ct + 1
            f.write(str(ct) + '\t')

        # corr count
        f.write(str(comp_ct) + '\t')
    logging.info('Counting games ended')

    # review for recently completed correspondence games
    token_value = func.get_conf('LichessAPIToken')
    game_url = 'https://lichess.org/api/games/export/_ids'
    yr = dt.datetime.now().strftime('%Y')
    root_path = r'D:\eehunt\LONGTERM\Chess\LichessPGN'
    dload_path = os.path.join(root_path, 'temp')
    dest_path = os.path.join(root_path, yr)

    logging.info('Review for recently completed correspondence games started')
    steps.completed_corr_pending(token_value, game_url)
    logging.info('Review for recently completed correspondence games ended')

    # download newly completed games and delete table records if download succeeds
    logging.info('Download for recently completed correspondence games started')
    running_total = steps.completed_corr_download(token_value, game_url, dload_path)
    with open(log_file, 'a') as f:
        f.write(str(running_total) + '\n')  # count of newly completed games
    logging.info('Download for recently completed correspondence games ended')

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

    os.rmdir(dload_path)

    logging.info('Process ended')


if __name__ == '__main__':
    main()
