import datetime as dt
import fileinput
import json
import logging
import os
import sys
import time

import chess.pgn
import pandas as pd
import pyodbc as sql
import requests

def get_conf(key):
    fname = r'C:\Users\eehunt\Repository\confidential.json'
    with open(fname, 'r') as t:
        key_data = json.load(t)
    val = key_data.get(key)
    return val

def format_date(game_text, tag):
    tag_text = game_text.headers.get(tag)
    if tag_text is None:
        tag_text = game_text.headers.get('Date')

    return tag_text
    
def format_result(game_text, tag):
    tag_text = game_text.headers.get(tag)
    # res_len = 3
    res = None

    if tag_text is not None:
        if tag_text == '1-0':
            res = '1.0'
        elif tag_text == '0-1':
            res = '0.0'
        elif tag_text == '1/2-1/2':
            res = '0.5'
    
    return res

def format_source_id(game_text, tag):
    tag_text = game_text.headers.get(tag)
    site_id = tag_text.split('/')[-1] if tag_text is not None else None
    
    return site_id

def main():
    # initiate
    dte = dt.datetime.now().strftime('%Y%m%d%H%M%S')
    scr_nm = os.path.splitext(os.path.basename(__file__))[0]
    log_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'logs')
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
    log_file = r'D:\eehunt\LONGTERM\Chess\LichessPGN\LichessPGN_Log.txt'
    file_path = r'D:\eehunt\LONGTERM\Chess\LichessPGN\2022'
    file_name = 'lichess_db_standard_rated_2022-08.pgn.bz2'

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
    cmd_text = '7z e ' +  file_name
    if os.getcwd != file_path:
        os.chdir(file_path)
    os.system('cmd /C ' + cmd_text)
    decomp_end = dt.datetime.now().strftime('%H:%M:%S')
    logging.info('Decompression ended')
    extracted_file = file_name.replace('.bz2', '')
    
    yyyy = extracted_file[26:30]
    mm = extracted_file[31:33]

    # create error log
    error_start = dt.datetime.now().strftime('%H:%M:%S')
    logging.info('Error log started')
    error_file = 'lichess_' + yyyy + mm + '_errors.log'
    cmd_text = 'pgn-extract --quiet -r -l' + error_file + ' ' + extracted_file
    if os.getcwd != file_path:
        os.chdir(file_path)
    os.system('cmd /C ' + cmd_text)
    error_end = dt.datetime.now().strftime('%H:%M:%S')
    logging.info('Error log ended')

    start_2200 = dt.datetime.now().strftime('%H:%M:%S')

    # update correspondence game TimeControl tag
    logging.info('Correspondence TimeControl tag update started')
    upd_name = 'lichess_tc_updated_' + yyyy + mm + '.pgn'
    ofile = os.path.join(file_path, extracted_file)
    nfile = os.path.join(file_path, upd_name)
    searchExp = '[TimeControl "-"]\n'
    replaceExp = '[TimeControl "1/86400"]\n'
    wfile = open(nfile, 'w')
    for line in fileinput.input(ofile, inplace=1):
        if searchExp in line:
            line = line.replace(searchExp, replaceExp)
        wfile.write(line)
    wfile.close()
    logging.info('Correspondence TimeControl tag update ended')

    # extract only 2200+ rating games
    logging.info('2200+ game extract started')
    tag_file = r'C:\Users\eehunt\Repository\Lichess2200\LichessPgnTags.txt'
    pgn_name = 'lichess2200all_' + yyyy + mm + '.pgn'
    cmd_text = 'pgn-extract -N -V -D -pl2 -t"' + tag_file + '" --quiet --fixresulttags --fixtagstrings --nosetuptags --output ' + pgn_name + ' ' + upd_name
    if os.getcwd() != file_path:
        os.chdir(file_path)
    os.system('cmd /C ' + cmd_text)
    logging.info('2200+ game extract ended')

    # fix date tag if file is earlier than 201804
    bad_dates = 0
    if int(yyyy + mm) <= 201803:
        logging.info(f'Filedate is {yyyy}{mm}, Date tag update started')
        bad_dates = 1
        new_pgn_name_2 = 'lichess2200allfixed_' + yyyy + mm + '.pgn'
        nfile2 = os.path.join(file_path, new_pgn_name_2)
        searchExp1 = '[Date "????.??.??"]\n'
        replaceExp1 = ''
        searchExp2 = '[UTCDate'
        replaceExp2 = '[Date'

        # DO IT
        wfile = open(nfile2, 'w')
        for line in fileinput.input(os.path.join(file_path, pgn_name), inplace=1):
            if searchExp1 in line:
                line = line.replace(searchExp1, replaceExp1)
            elif searchExp2 in line:
                line = line.replace(searchExp2, replaceExp2)
            wfile.write(line)
        wfile.close()
        logging.info(f'Filedate is {yyyy}{mm}, Date tag update ended')
    
    if bad_dates:
        curr_name = new_pgn_name_2
    else:
        curr_name = pgn_name

    # separate into time control files
    i = 0
    tc_files = []
    tc_options = ['Bullet', 'Blitz', 'Rapid', 'Classical']
    tc_min_list = ['60', '180', '601', '1800']
    tc_max_list = ['179', '600', '1799', '86399']
    for tc_type in tc_options:
        logging.info(f'{tc_type} extract started')
        tc_min = tc_min_list[i]
        tc_max = tc_max_list[i]
        new_tc_name = 'lichess2200_' + yyyy + mm + '_' + tc_type + '.pgn'

        # create time control tag files
        tc_tag_file_min = 'TimeControlTagMin.txt'
        tc_tag_file_min_full = os.path.join(file_path, tc_tag_file_min)
        tc_txt = 'TimeControl >= "' + tc_min + '"'
        with open(tc_tag_file_min_full, 'w') as mn:
            mn.write(tc_txt)
        
        tc_tag_file_max = 'TimeControlTagMax.txt'
        tc_tag_file_max_full = os.path.join(file_path, tc_tag_file_max)
        tc_txt = 'TimeControl <= "' + tc_max + '"'
        with open(tc_tag_file_max_full, 'w') as mx:
            mx.write(tc_txt)
    
        # filter min time control
        tmp_file = 'temp' + tc_type + '_' + curr_name
        cmd_text = 'pgn-extract --quiet -t' + tc_tag_file_min + ' --output ' + tmp_file + ' ' + curr_name
        if os.getcwd != file_path:
            os.chdir(file_path)
        os.system('cmd /C ' + cmd_text)

        # filter max time control
        cmd_text = 'pgn-extract --quiet -t' + tc_tag_file_max + ' --output ' + new_tc_name + ' ' + tmp_file
        if os.getcwd != file_path:
            os.chdir(file_path)
        os.system('cmd /C ' + cmd_text)

        tc_files.append(new_tc_name)
        i = i + 1
        os.remove(os.path.join(file_path, tmp_file))
        os.remove(tc_tag_file_min_full)
        os.remove(tc_tag_file_max_full)
        logging.info(f'{tc_type} extract ended')

    end_2200 = dt.datetime.now().strftime('%H:%M:%S')

    # split pgn into corr games
    start_corr = dt.datetime.now().strftime('%H:%M:%S')
    logging.info('Complete correspondence game extract started')
    tag_file = r'C:\Users\eehunt\Repository\Lichess2200\LichessCorrTag.txt'
    minply = '6'
    corr_name = 'lichess_correspondence_orig_' + yyyy + mm + '.pgn'
    cmd_text = 'pgn-extract -N -V -D -s -pl' + minply + ' -t"' + tag_file + '" --quiet --fixresulttags --fixtagstrings --nosetuptags -o' + corr_name + ' ' + upd_name
    if os.getcwd() != file_path:
        os.chdir(file_path)
    os.system('cmd /C ' + cmd_text)
    logging.info('Complete correspondence game extract ended')

    # fix date tag if file is earlier than 201804
    new_name = 'lichess_correspondence_' + yyyy + mm + '.pgn'
    bad_dates = 0
    if int(yyyy + mm) <= 201803:
        logging.info(f'Filedate is {yyyy}{mm}, Date tag update started')
        bad_dates = 1
        curr_name = os.path.join(file_path, corr_name)
        nfile3 = os.path.join(file_path, new_name)
        searchExp1 = '[Date "????.??.??"]\n'
        replaceExp1 = ''
        searchExp2 = '[UTCDate'
        replaceExp2 = '[Date'

        wfile = open(nfile3, 'w')
        for line in fileinput.input(curr_name, inplace=1):
            if searchExp1 in line:
                line = line.replace(searchExp1, replaceExp1)
            elif searchExp2 in line:
                line = line.replace(searchExp2, replaceExp2)
            wfile.write(line)
        wfile.close()
        logging.info(f'Filedate is {yyyy}{mm}, Date tag update ended')
    
    if not bad_dates:
        os.rename(os.path.join(file_path, corr_name), os.path.join(file_path, new_name))

    logging.info('Review for ongoing correspondence games started')
    conn_str = get_conf('SqlServerConnectionStringTrusted')
    conn = sql.connect(conn_str)
    csr = conn.cursor()

    full_pgn = os.path.join(file_path, new_name)
    completed_file = f'{os.path.splitext(new_name)[0]}_Completed.pgn'
    completed_full = os.path.join(file_path, completed_file)
    comp_ct = 0
    with open(full_pgn, 'r') as pgn:
        ctr = 0
        game_text = chess.pgn.read_game(pgn)
        while game_text is not None:
            result = format_result(game_text, 'Result')
            if result is None:
                gameid = format_source_id(game_text, 'Site')
                qry_text = f"SELECT GameID FROM OngoingLichessCorr WHERE GameID = '{gameid}'"
                gmlist = pd.read_sql(qry_text, conn).values.tolist()
                gm_ct = len(gmlist)
                sql_cmd = ''
                if gm_ct == 0:
                    sql_cmd = f"INSERT INTO OngoingLichessCorr (GameID, Filename, Download, Inactive) VALUES ('{gameid}', '{new_name}', 0, 0)"
                if sql_cmd != '':
                    logging.debug(sql_cmd)
                    csr.execute(sql_cmd)
                    conn.commit()
                    ctr = ctr + 1
            else:
                comp_ct = comp_ct + 1
                with open(completed_full, 'a', encoding='utf-8') as f:
                    f.write(str(game_text) + '\n\n')

            game_text = chess.pgn.read_game(pgn)
    
    tc_files.append(completed_file)
    conn.close()
    logging.info(f'Total of {ctr} ongoing correspondence games')
    logging.info('Review for ongoing correspondence games ended')

    end_corr = dt.datetime.now().strftime('%H:%M:%S')

    # clean up old files
    os.remove(os.path.join(file_path, extracted_file))
    os.remove(os.path.join(file_path, upd_name))
    os.remove(os.path.join(file_path, pgn_name))
    os.remove(os.path.join(file_path, new_name))
    if bad_dates:
        os.remove(nfile2)
        os.remove(os.path.join(file_path, corr_name))

    # write to log file
    logging.info('Counting games started')
    with open(log_file, 'a') as f:
        # timings
        f.write(extracted_file + '\t')
        f.write(start_date + '\t')
        f.write(decomp_start + '\t')
        f.write(decomp_end + '\t')
        f.write(error_start + '\t')
        f.write(error_end + '\t')
        f.write(start_2200 + '\t')
        f.write(end_2200 + '\t')
        f.write(start_corr + '\t')
        f.write(end_corr)

        # 2200 game counts
        search_text = '[Event "'
        for tcf in tc_files:
            ct = 0
            if os.path.isfile(os.path.join(file_path, tcf)):
                with open(os.path.join(file_path, tcf), 'r') as ff:
                    for line in ff:
                        if search_text in line:
                            ct = ct + 1
            f.write('\t' + str(ct))
        
        # corr count
        f.write('\t' + str(comp_ct))
        # f.write('\n')
    logging.info('Counting games ended')

    # review for recently completed correspondence games
    conn = sql.connect(conn_str)
    csr = conn.cursor()
    token_value = get_conf('LichessAPIToken')
    game_url = 'https://lichess.org/api/games/export/_ids'
    hdr_json = {'Authorization': 'Bearer ' + token_value, 'Accept': 'application/x-ndjson'}
    hdr_pgn = {'Authorization': 'Bearer ' + token_value, 'Accept': 'application/x-chess-pgn'}
    completed_status = ['aborted', 'mate', 'resign', 'stalemate', 'timeout', 'draw', 'outoftime', 'cheat']
    yr = dt.datetime.now().strftime('%Y')
    curr_unix = int(time.time()*1000)
    root_path = r'D:\eehunt\LONGTERM\Chess\LichessPGN'
    dload_path = os.path.join(root_path, 'temp')
    dest_path = os.path.join(root_path, yr)

    # update database table
    logging.info('Review for recently completed correspondence games started')
    game_qry = """
SELECT TOP 300
GameID
FROM OngoingLichessCorr
WHERE (Inactive = 0 AND (LastReviewed IS NULL OR DATEDIFF(DAY, LastReviewed, GETDATE()) >= 7))
OR (Inactive = 1 AND DATEDIFF(DAY, LastReviewed, GETDATE()) >= 90)
    """
    game_rec = pd.read_sql(game_qry, conn).values.tolist()
    game_list = [j for sub in game_rec for j in sub]
    game_ct = len(game_list)
    running_total = 0
    while game_ct > 0:
        game_join = ','.join(game_list)
        cde = 429
        ct = 0
        while cde != 200:
            with requests.post(game_url, headers=hdr_json, data=game_join) as resp:
                cde = resp.status_code
                if cde == 200:
                    c = resp.content.decode('utf-8')
                    games = [json.loads(s) for s in c.split('\n')[:-1]]
                    if len(games) > 0:
                        for g in games:
                            upd_qry = ''
                            game_id = g['id']
                            curr_status = g['status']
                            last_move = g['lastMoveAt']
                            if curr_status in completed_status:
                                upd_qry = f"UPDATE OngoingLichessCorr SET Download = 1, LastMoveAtUnix = {last_move}, LastReviewed = GETDATE() WHERE GameID = '{game_id}'"
                            else:
                                # if no move has been played in at least 30 days, game is still considered ongoing. update to inactive
                                if curr_unix - last_move > 2592000000:
                                    inact = '1'
                                else:
                                    inact = '0'
                                upd_qry = f"UPDATE OngoingLichessCorr SET LastMoveAtUnix = {last_move}, LastReviewed = GETDATE(), Inactive = {inact} WHERE GameID = '{game_id}'"

                            if upd_qry != '':
                                logging.debug(upd_qry)
                                csr.execute(upd_qry)
                                conn.commit()
                    else:
                        # this would only happen for gameid's that don't exist in Lichess for some reason. no idea how but it did happen
                        for gm in game_list:
                            logging.warning(f'GameID {gm} does not exist, marking as inactive')
                            upd_qry = f"UPDATE OngoingLichessCorr SET LastReviewed = GETDATE(), Inactive = 1 WHERE GameID = '{gm}'"
                            if upd_qry != '':
                                logging.debug(upd_qry)
                                csr.execute(upd_qry)
                                conn.commit()

                else:
                    ct = ct + 1
                    if cde == 429:
                        logging.warning('API returned 429, waiting 65 seconds before trying again')
                        time.sleep(65)

                if ct == 5: # exit ability to avoid infinite loop
                    logging.critical('API rejected 5 consecutive times, terminating script!')
                    quit()

        running_total = running_total + game_ct
        logging.info(f'Games reviewed so far: {running_total}')
        game_rec = pd.read_sql(game_qry, conn).values.tolist()
        game_list = [j for sub in game_rec for j in sub]
        game_ct = len(game_list)
    logging.info('Review for recently completed correspondence games ended')

    # download newly completed games and delete table records if download succeeds
    logging.info('Download for recently completed correspondence games started')
    if not os.path.isdir(dload_path):
        os.mkdir(dload_path)
    total_qry = "SELECT COUNT(GameID) FROM OngoingLichessCorr WHERE Download = 1"
    total_rec = pd.read_sql(total_qry, conn).values.tolist()
    total_dl = int(total_rec[0][0])
    dl_qry = "SELECT TOP 300 GameID FROM OngoingLichessCorr WHERE Download = 1 ORDER BY GameID"
    dl_delete = f"DELETE FROM OngoingLichessCorr WHERE GameID IN ({dl_qry})"
    dl_rec = pd.read_sql(dl_qry, conn).values.tolist()
    dl_list = [j for sub in dl_rec for j in sub]
    dl_ct = len(dl_list)
    ctr = 1
    running_total = 0
    while dl_ct > 0:
        dl_join = ','.join(dl_list)
        dload_name = f'lichess_newlycomplete_{dte}_{ctr}.pgn'
        dload_file = os.path.join(dload_path, dload_name)
        cde = 429
        ct = 0
        while cde != 200:
            with requests.post(game_url, headers=hdr_pgn, data=dl_join, stream=True) as resp:
                cde = resp.status_code
                if cde == 200:
                    with open(dload_file, 'wb') as f:
                        for chunk in resp.iter_content(chunk_size=8196):
                            f.write(chunk)
                    
                    logging.debug(dl_delete)
                    csr.execute(dl_delete)
                    conn.commit()
                else:
                    ct = ct + 1
                    if cde == 429:
                        logging.warning('API returned 429, waiting 65 seconds before trying again')
                        time.sleep(65)

                if ct == 5: # exit ability to avoid infinite loop
                    logging.critical('API rejected 5 consecutive times, terminating script!')
                    quit()

        running_total = running_total + dl_ct
        logging.info(f'Currently downloaded {running_total} of {total_dl} games')
        dl_rec = pd.read_sql(dl_qry, conn).values.tolist()
        dl_list = [j for sub in dl_rec for j in sub]
        dl_ct = len(dl_list)
        ctr = ctr + 1
    
    conn.close()
    with open(log_file, 'a') as f:
        f.write('\t' + str(running_total) + '\n') # count of newly completed games
    logging.info('Download for recently completed correspondence games ended')

    # verify files were downloaded before continuing
    file_list = [f for f in os.listdir(dload_path) if os.path.isfile(os.path.join(dload_path, f))]
    if len(file_list) == 0:
        logging.warning('No recently completed correspondence games found, process ended')
        quit()

    # merge newly downloaded pgns
    logging.info('Merge of recently completed correspondence games started')
    merge_name = 'newlycomplete_merge.pgn'
    cmd_text = 'copy /B *.pgn ' + merge_name + ' >nul'
    if os.getcwd != dload_path:
        os.chdir(dload_path)
    os.system('cmd /C ' + cmd_text)
    logging.info('Merge of recently completed correspondence games ended')

    # sort game file
    logging.info('Sorting of recently completed correspondence games started')
    idx = []
    game_date = []
    game_text = []
    gm_idx = 0 
    with open(os.path.join(dload_path, merge_name), mode='r', encoding='utf-8', errors='replace') as pgn:
        gm_txt = chess.pgn.read_game(pgn)
        while gm_txt is not None:
            idx.append(gm_idx)
            game_date.append(gm_txt.headers['UTCDate'])
            game_text.append(gm_txt)
            gm_txt = chess.pgn.read_game(pgn)
            gm_idx = gm_idx + 1

    idx_sort = [x for _, x in sorted(zip(game_date, idx))]
    sort_name = f'lichess_correspondence_{dte}_NewlyCompleted.pgn'
    with open(os.path.join(dload_path, sort_name), 'w', encoding = 'utf-8') as sort_file:
        for i in idx_sort:
            sort_file.write(str(game_text[i]) + '\n\n')
    logging.info('Sorting of recently completed correspondence games ended')

    # clean up
    dir_files = [f for f in os.listdir(dload_path) if os.path.isfile(os.path.join(dload_path, f))]
    for filename in dir_files:
        if filename != sort_name:
            fname_relpath = os.path.join(dload_path, filename)
            os.remove(fname_relpath)
    
    if not os.path.isdir(dest_path):
        os.mkdir(dest_path)
    if os.getcwd != dest_path:
        os.chdir(dest_path)
    old_name = os.path.join(dload_path, sort_name)
    new_name = os.path.join(dest_path, sort_name)
    os.rename(old_name, new_name)
    os.rmdir(dload_path)

    logging.info('Process ended')


if __name__ == '__main__':
    main()
