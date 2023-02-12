import datetime as dt
import fileinput
import json
import logging
import os
import shutil
import time

import chess.pgn
import pandas as pd
import pyodbc as sql
import requests

import format as fmt
import func


def completed_corr_download(token_value, game_url, dload_path):
    hdr_pgn = {'Authorization': 'Bearer ' + token_value, 'Accept': 'application/x-chess-pgn'}
    if not os.path.isdir(dload_path):
        os.mkdir(dload_path)
    dte = dt.datetime.now().strftime('%Y%m%d%H%M%S')

    conn_str = func.get_conf('SqlServerConnectionStringTrusted')
    conn = sql.connect(conn_str)
    csr = conn.cursor()
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

                if ct == 5:  # exit ability to avoid infinite loop
                    logging.critical('API rejected 5 consecutive times, terminating script!')
                    raise SystemExit

        running_total = running_total + dl_ct
        logging.info(f'Currently downloaded {running_total} of {total_dl} games')
        dl_rec = pd.read_sql(dl_qry, conn).values.tolist()
        dl_list = [j for sub in dl_rec for j in sub]
        dl_ct = len(dl_list)
        ctr = ctr + 1
    conn.close()
    return total_dl


def completed_corr_pending(token_value, game_url):
    conn_str = func.get_conf('SqlServerConnectionStringTrusted')
    hdr_json = {'Authorization': 'Bearer ' + token_value, 'Accept': 'application/x-ndjson'}
    completed_status = ['aborted', 'mate', 'resign', 'stalemate', 'timeout', 'draw', 'outoftime', 'cheat']
    curr_unix = int(time.time()*1000)
    conn = sql.connect(conn_str)
    csr = conn.cursor()

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
                                upd_qry = 'UPDATE OngoingLichessCorr '
                                upd_qry = upd_qry + f'SET Download = 1, LastMoveAtUnix = {last_move}, LastReviewed = GETDATE() '
                                upd_qry = upd_qry + f"WHERE GameID = '{game_id}'"
                            else:
                                # if no move has been played in at least 30 days, game is still considered ongoing. update to inactive
                                if curr_unix - last_move > 2592000000:
                                    inact = '1'
                                else:
                                    inact = '0'
                                upd_qry = 'UPDATE OngoingLichessCorr '
                                upd_qry = upd_qry + f'SET LastMoveAtUnix = {last_move}, LastReviewed = GETDATE(), Inactive = {inact} '
                                upd_qry = upd_qry + f"WHERE GameID = '{game_id}'"

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

                if ct == 5:  # exit ability to avoid infinite loop
                    logging.critical('API rejected 5 consecutive times, terminating script!')
                    raise SystemExit

        running_total = running_total + game_ct
        logging.info(f'Games reviewed so far: {running_total}')
        game_rec = pd.read_sql(game_qry, conn).values.tolist()
        game_list = [j for sub in game_rec for j in sub]
        game_ct = len(game_list)
    conn.close()
    return running_total


def decompress(file_path, file_name):
    # https://github.com/facebook/zstd/releases/download/v1.4.4/zstd-v1.4.4-win64.zip
    # lichess switched from bz2 to zst after 202210 file
    new_file_name = file_name.replace('.zst', '')
    cmd_text = f'zstd -d {file_name} -o {new_file_name}'
    logging.debug(cmd_text)
    if os.getcwd != file_path:
        os.chdir(file_path)
    os.system('cmd /C ' + cmd_text)
    return new_file_name


def errorlog(file_path, file_name, y, m):
    error_file = f'lichess_{y}{m}_errors.log'
    cmd_text = f'pgn-extract --quiet -r -l{error_file} {file_name}'
    logging.debug(cmd_text)
    if os.getcwd != file_path:
        os.chdir(file_path)
    os.system('cmd /C ' + cmd_text)


def extract2200(file_path, file_name, y, m):
    pgn_tag_name = 'LichessPgnTags.txt'
    pgn_tag_file = os.path.join(file_path, pgn_tag_name)
    with open(pgn_tag_file, 'w') as tf:
        tf.write('WhiteElo >= "2200"')
        tf.write('\n')
        tf.write('BlackElo >= "2200"')

    pgn_name = f'lichess2200all_{y}{m}.pgn'
    cmd_text = f'pgn-extract -N -V -D -pl2 -t"{pgn_tag_file}" --quiet --fixresulttags --fixtagstrings --nosetuptags --output {pgn_name} {file_name}'
    logging.debug(cmd_text)
    if os.getcwd() != file_path:
        os.chdir(file_path)
    os.system('cmd /C ' + cmd_text)
    os.remove(os.path.join(file_path, pgn_tag_name))
    return pgn_name


def extract2200corr(file_path, temp_path, monthly_file, complete_file):
    shutil.copy(os.path.join(file_path, monthly_file), temp_path)
    shutil.copy(os.path.join(file_path, complete_file), temp_path)
    merge_name = merge_files(temp_path)
    yyyy = monthly_file[23:27]
    mm = monthly_file[27:29]
    filter_name = extract2200(temp_path, merge_name, yyyy, mm)
    final_name = f'lichess2200_{yyyy}{mm}_Correspondence.pgn'
    old_name = os.path.join(temp_path, filter_name)
    new_name = os.path.join(file_path, final_name)
    os.rename(old_name, new_name)


def extractbulletblitz(file_path, tc_files, limit):
    for file in tc_files:
        if 'bullet' in file.lower() or 'blitz' in file.lower():
            lim_name = os.path.splitext(file)[0] + f'_{limit}' + '.pgn'
            cmd_text = f'pgn-extract --quiet --gamelimit {limit} --output {lim_name} {file}'
            logging.debug(cmd_text)
            if os.getcwd != file_path:
                os.chdir(file_path)
            os.system('cmd /C ' + cmd_text)


def extractcorr(file_path, file_name, y, m):
    corr_tag_name = 'LichessCorrTag.txt'
    corr_tag_file = os.path.join(file_path, corr_tag_name)
    tag_txt = 'TimeControl >= "86400"'
    with open(corr_tag_file, 'w') as tf:
        tf.write(tag_txt)

    minply = '6'
    corr_name = f'lichess_correspondence_orig_{y}{m}.pgn'
    cmd_text = f'pgn-extract -N -V -D -s -pl{minply} -t"{corr_tag_file}" --quiet --fixresulttags --fixtagstrings --nosetuptags -o{corr_name} {file_name}'
    logging.debug(cmd_text)
    if os.getcwd() != file_path:
        os.chdir(file_path)
    os.system('cmd /C ' + cmd_text)
    os.remove(os.path.join(file_path, corr_tag_name))
    return corr_name


def fix_datetag(file_path, file_name, y, m, corrflag):
    if corrflag:
        new_name = f'lichess_correspondence_{y}{m}.pgn'
    else:
        new_name = f'lichess2200allfixed_{y}{m}.pgn'
    nfile = os.path.join(file_path, new_name)
    searchExp1 = '[Date "????.??.??"]\n'
    replaceExp1 = ''
    searchExp2 = '[UTCDate'
    replaceExp2 = '[Date'

    wfile = open(nfile, 'w', encoding='utf-8')
    for line in fileinput.input(os.path.join(file_path, file_name), inplace=1, openhook=fileinput.hook_encoded('utf-8')):
        if searchExp1 in line:
            line = line.replace(searchExp1, replaceExp1)
        elif searchExp2 in line:
            line = line.replace(searchExp2, replaceExp2)
        wfile.write(line)
    wfile.close()
    return new_name


def merge_files(file_path):
    merge_name = 'mergedgamefile.pgn'
    cmd_text = 'copy /B *.pgn ' + merge_name + ' >nul'
    if os.getcwd != file_path:
        os.chdir(file_path)
    os.system('cmd /C ' + cmd_text)
    return merge_name


def ongoing_corr(file_path, file_name):
    conn_str = func.get_conf('SqlServerConnectionStringTrusted')
    conn = sql.connect(conn_str)
    csr = conn.cursor()

    full_pgn = os.path.join(file_path, file_name)
    completed_file = f'{os.path.splitext(file_name)[0]}_Completed.pgn'
    completed_full = os.path.join(file_path, completed_file)
    with open(full_pgn, 'r', encoding='utf-8') as pgn:
        ctr = 0
        game_text = chess.pgn.read_game(pgn)
        while game_text is not None:
            result = fmt.format_result(game_text, 'Result')
            if result is None:
                gameid = fmt.format_source_id(game_text, 'Site')
                qry_text = f"SELECT GameID FROM OngoingLichessCorr WHERE GameID = '{gameid}'"
                gmlist = pd.read_sql(qry_text, conn).values.tolist()
                gm_ct = len(gmlist)
                sql_cmd = ''
                if gm_ct == 0:
                    sql_cmd = 'INSERT INTO OngoingLichessCorr (GameID, Filename, Download, Inactive) '
                    sql_cmd = sql_cmd + f"VALUES ('{gameid}', '{file_name}', 0, 0)"
                if sql_cmd != '':
                    logging.debug(sql_cmd)
                    csr.execute(sql_cmd)
                    conn.commit()
                    ctr = ctr + 1
            else:
                with open(completed_full, 'a', encoding='utf-8') as f:
                    f.write(str(game_text) + '\n\n')

            game_text = chess.pgn.read_game(pgn)
    conn.close()
    return completed_file, ctr


def sort_gamefile(file_path, file_name):
    dte = dt.datetime.now().strftime('%Y%m%d%H%M%S')
    idx = []
    game_date = []
    game_text = []
    gm_idx = 0
    with open(os.path.join(file_path, file_name), mode='r', encoding='utf-8', errors='replace') as pgn:
        gm_txt = chess.pgn.read_game(pgn)
        while gm_txt is not None:
            idx.append(gm_idx)
            game_date.append(gm_txt.headers['UTCDate'])
            game_text.append(gm_txt)
            gm_txt = chess.pgn.read_game(pgn)
            gm_idx = gm_idx + 1

    idx_sort = [x for _, x in sorted(zip(game_date, idx))]
    sort_name = f'lichess_correspondence_{dte}_NewlyCompleted.pgn'
    with open(os.path.join(file_path, sort_name), 'w', encoding='utf-8') as sort_file:
        for i in idx_sort:
            sort_file.write(str(game_text[i]) + '\n\n')
    return sort_name


def split_timecontrol(file_path, file_name, y, m):
    i = 0
    tc_files = []
    tc_options = ['Bullet', 'Blitz', 'Rapid', 'Classical']
    tc_min_list = ['60', '180', '601', '1800']
    tc_max_list = ['179', '600', '1799', '86399']
    for tc_type in tc_options:
        logging.info(f'{tc_type} extract started')
        tc_min = tc_min_list[i]
        tc_max = tc_max_list[i]
        new_tc_name = f'lichess2200_{y}{m}_{tc_type}.pgn'

        # create time control tag files
        tc_tag_file_min = 'TimeControlTagMin.txt'
        tc_tag_file_min_full = os.path.join(file_path, tc_tag_file_min)
        tc_txt = f'TimeControl >= "{tc_min}"'
        with open(tc_tag_file_min_full, 'w') as mn:
            mn.write(tc_txt)

        tc_tag_file_max = 'TimeControlTagMax.txt'
        tc_tag_file_max_full = os.path.join(file_path, tc_tag_file_max)
        tc_txt = f'TimeControl <= "{tc_max}"'
        with open(tc_tag_file_max_full, 'w') as mx:
            mx.write(tc_txt)

        # filter min time control
        tmp_file = f'temp{tc_type}_{file_name}'
        cmd_text = f'pgn-extract --quiet -t{tc_tag_file_min} --output {tmp_file} {file_name}'
        logging.debug(cmd_text)
        if os.getcwd != file_path:
            os.chdir(file_path)
        os.system('cmd /C ' + cmd_text)

        # filter max time control
        cmd_text = f'pgn-extract --quiet -t{tc_tag_file_max} --output {new_tc_name} {tmp_file}'
        logging.debug(cmd_text)
        if os.getcwd != file_path:
            os.chdir(file_path)
        os.system('cmd /C ' + cmd_text)

        tc_files.append(new_tc_name)
        i = i + 1
        os.remove(os.path.join(file_path, tmp_file))
        os.remove(tc_tag_file_min_full)
        os.remove(tc_tag_file_max_full)
        logging.info(f'{tc_type} extract ended')
    return tc_files


def update_timecontrol(file_path, file_name, y, m):
    upd_name = f'lichess_tc_updated_{y}{m}.pgn'
    ofile = os.path.join(file_path, file_name)
    nfile = os.path.join(file_path, upd_name)
    searchExp = '[TimeControl "-"]\n'
    replaceExp = '[TimeControl "1/86400"]\n'
    wfile = open(nfile, 'w', encoding='utf-8')
    for line in fileinput.input(ofile, inplace=1):
        if searchExp in line:
            line = line.replace(searchExp, replaceExp)
        wfile.write(line)
    wfile.close()
    return upd_name
