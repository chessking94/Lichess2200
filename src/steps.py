import datetime as dt
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
import time

import chess.pgn
import pandas as pd
import pyodbc as sql
import requests
import sqlalchemy as sa

from Utilities_Python import misc, notifications

CONFIG_FILE = os.path.join(Path(__file__).parents[1], 'config.json')


def files_to_process():
    dloads = {}  # this will be a dictionary with the key being the .pgn file, and the value is the .zst
    dload_url = 'https://database.lichess.org/standard/list.txt'
    with requests.get(dload_url, stream=True) as resp:
        if resp.status_code != 200:
            logging.warning(f'Unable to complete request to {dload_url}! Request returned code {resp.status_code}')
        else:
            for line in resp.iter_lines():
                dload_name = line.decode('utf-8')
                proc_name = dload_name.split('/')[-1].replace('.zst', '')
                dloads[proc_name] = dload_name

    files_to_download = []
    for f in dloads:
        conn_str = os.getenv('ConnectionStringOdbcRelease')
        conn = sql.connect(conn_str)
        csr = conn.cursor()
        qry = 'SELECT COUNT(Filename) FROM ChessWarehouse.dbo.LichessDatabase WHERE Filename = ?'
        csr.execute(qry, f)
        if csr.fetchone()[0] == 0:
            files_to_download.append(dloads[f])
        conn.close()

    return sorted(files_to_download)


class DatabaseExport:
    def __init__(self, url):
        self.url = url
        self.archive_file = self.url.split('/')[-1]
        self.pgn_file = os.path.splitext(self.archive_file)[0]  # strip off the outermost extension
        self.yyyy = self.pgn_file[26:30]
        self.mm = self.pgn_file[31:33]

        self.conn_str = os.getenv('ConnectionStringOdbcRelease')

        self.download_root = misc.get_config('downloadRoot', CONFIG_FILE)
        self.download_path = os.path.join(self.download_root, dt.datetime.now().strftime('%Y%m%d-%H%M%S'))

        self.files_to_keep = []
        self.analysis_files = []

        self.pgn_file_updated_tc = None
        self.timecontrol_files = []
        self.pgn_file_corr_completed = None
        self.final_archive = f'lichess2200_{self.yyyy}{self.mm}.7z'

    def process_export(self):
        self._initialize()
        self._download()
        self._decompress()
        self._create_error_log()
        self._extract_2200_games()
        self._extract_corr_games()
        self._count_games()
        self._recently_completed_corr()
        self._extractbulletblitz()
        self._queue_analysis()
        self._cleanup()

    def _initialize(self):
        msg = f'Begin processing {self.archive_file}'
        notifications.SendTelegramMessage(f'Lichess2200: {msg}')
        logging.info(msg)
        self._write_log()

    def _download(self):
        logging.info('Download started')
        self._write_log('Download_Start', 'GETDATE()')

        if not os.path.isdir(self.download_path):
            os.mkdir(self.download_path)

        with requests.get(self.url, stream=True) as resp:
            if resp.status_code != 200:
                logging.critical(f'Unable to complete download to {self.url}! Request returned code {resp.status_code}')
                raise SystemExit
            else:
                with open(os.path.join(self.download_path, self.archive_file), 'wb') as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        if chunk:  # filter out keep-alive chunks
                            f.write(chunk)

        self._write_log('Download_End', 'GETDATE()')

    def _decompress(self):
        # https://github.com/facebook/zstd/releases/download/v1.4.4/zstd-v1.4.4-win64.zip
        logging.info('Decompression started')
        self._write_log('Decompression_Start', 'GETDATE()')

        cmd_text = f'zstd -d {self.archive_file} -o {self.pgn_file}'
        result = subprocess.run(cmd_text, cwd=self.download_path, capture_output=True, text=True)
        if result.returncode != 0:
            logging.critical(f'Error extracting archive: {result.stderr}')
            raise SystemExit

        self._write_log('Decompression_End', 'GETDATE()')

    def _create_error_log(self):
        logging.info('Error log started')
        self._write_log('ErrorLog_Start', 'GETDATE()')

        error_file = f'lichess_{self.yyyy}{self.mm}_errors.log'
        cmd_text = f'pgn-extract --quiet -r -l{error_file} {self.pgn_file}'
        logging.debug(cmd_text)
        result = subprocess.run(cmd_text, cwd=self.download_path, capture_output=True, text=True)
        if result.returncode != 0:
            logging.critical(f'Error extracting error log: {result.stderr}')
            raise SystemExit

        if os.path.getsize(os.path.join(self.download_path, error_file)) > 0:
            self.files_to_keep.append(error_file)
        else:
            os.remove(os.path.join(self.download_path, error_file))

        self._write_log('ErrorLog_End', 'GETDATE()')

    def _extract_2200_games(self):
        self._write_log('[2200_Start]', 'GETDATE()')

        # update correspondence game TimeControl tag
        logging.info('Correspondence TimeControl tag update started')
        self.pgn_file_updated_tc = self._update_corr_timecontrol(os.path.join(self.download_path, self.pgn_file))
        os.remove(os.path.join(self.download_path, self.pgn_file))  # file no longer needed

        # extract only 2200+ rating games
        logging.info('2200+ game extract started')
        pgn_tag_name = 'LichessPgnTags.txt'
        with open(os.path.join(self.download_path, pgn_tag_name), 'w') as tf:
            tf.write('WhiteElo >= "2200"')
            tf.write('\n')
            tf.write('BlackElo >= "2200"')

        pgn_file_2200_all = f'lichess2200all_{self.yyyy}{self.mm}.pgn'
        cmd_text = f'pgn-extract -N -V -D -pl2 -t"{pgn_tag_name}" --quiet --fixresulttags --fixtagstrings --nosetuptags --output {pgn_file_2200_all} {self.pgn_file_updated_tc}'
        logging.debug(cmd_text)
        result = subprocess.run(cmd_text, cwd=self.download_path, capture_output=True, text=True)
        if result.returncode != 0:
            logging.critical(f'Error extracting 2200 games: {result.stderr}')
            raise SystemExit
        os.remove(os.path.join(self.download_path, pgn_tag_name))

        pgn_file_2000_all_fixedtags = self._fix_datetags(os.path.join(self.download_path, pgn_file_2200_all), False)

        # separate into time control files
        logging.info('Splitting into time-control files started')
        self.timecontrol_files = self._split_timecontrol(os.path.join(self.download_path, pgn_file_2000_all_fixedtags))
        self.files_to_keep.extend(self.timecontrol_files)
        self._write_log('[2200_End]', 'GETDATE()')
        os.remove(os.path.join(self.download_path, pgn_file_2000_all_fixedtags))

    def _extract_corr_games(self):
        logging.info('Complete correspondence game extract started')
        self._write_log('Corr_Start', 'GETDATE()')

        corr_tag_name = 'LichessCorrTag.txt'
        tag_txt = 'TimeControl >= "86400"'
        with open(os.path.join(self.download_path, corr_tag_name), 'w') as tf:
            tf.write(tag_txt)

        minply = '6'
        pgn_file_corr_all_temp = f'lichess_correspondence_orig_{self.yyyy}{self.mm}.pgn'
        cmd_text = f'pgn-extract -N -V -D -s -pl{minply} -t"{corr_tag_name}" --quiet --fixresulttags --fixtagstrings --nosetuptags -o{pgn_file_corr_all_temp} {self.pgn_file_updated_tc}'
        logging.debug(cmd_text)
        result = subprocess.run(cmd_text, cwd=self.download_path, capture_output=True, text=True)
        if result.returncode != 0:
            logging.critical(f'Error extracting correspondence games: {result.stderr}')
            raise SystemExit
        os.remove(os.path.join(self.download_path, corr_tag_name))
        os.remove(os.path.join(self.download_path, self.pgn_file_updated_tc))

        pgn_file_corr_all = self._fix_datetags(os.path.join(self.download_path, pgn_file_corr_all_temp), True)

        logging.info('Review for ongoing correspondence games started')

        connection_url = sa.engine.URL.create(
            drivername='mssql+pyodbc',
            query={'odbc_connect': self.conn_str}
        )
        qryengine = sa.create_engine(connection_url)
        conn = sql.connect(self.conn_str)
        csr = conn.cursor()

        completed_file = f'{os.path.splitext(pgn_file_corr_all)[0]}_Completed.pgn'
        self.pgn_file_corr_completed = os.path.join(self.download_path, completed_file)
        ctr = 0
        with open(os.path.join(self.download_path, pgn_file_corr_all), 'r', encoding='utf-8') as pgn:
            game_text = chess.pgn.read_game(pgn)
            while game_text is not None:
                result = self._format_result(game_text, 'Result')
                if result is None:
                    gameid = self._format_source_id(game_text, 'Site')
                    qry_text = f"SELECT GameID FROM ChessWarehouse.dbo.OngoingLichessCorr WHERE GameID = '{gameid}'"
                    gmlist = pd.read_sql(qry_text, qryengine).values.tolist()
                    gm_ct = len(gmlist)
                    sql_cmd = ''
                    if gm_ct == 0:
                        sql_cmd = 'INSERT INTO ChessWarehouse.dbo.OngoingLichessCorr (GameID, Filename, Download, Inactive) '
                        sql_cmd = sql_cmd + f"VALUES ('{gameid}', '{pgn_file_corr_all}', 0, 0)"
                    if sql_cmd != '':
                        logging.debug(sql_cmd)
                        csr.execute(sql_cmd)
                        conn.commit()
                        ctr = ctr + 1
                else:
                    with open(self.pgn_file_corr_completed, 'a', encoding='utf-8') as f:
                        f.write(str(game_text) + '\n\n')

                game_text = chess.pgn.read_game(pgn)

        conn.close()
        qryengine.dispose()

        self.timecontrol_files.append(completed_file)
        self.files_to_keep.append(completed_file)
        os.remove(os.path.join(self.download_path, pgn_file_corr_all))

        logging.info(f'Total of {ctr} ongoing correspondence games')
        self._write_log('Corr_End', 'GETDATE()')

    def _count_games(self):
        logging.info('Counting games started')
        search_text = '[Event "'
        for tcf in self.timecontrol_files:
            ct = 0
            if os.path.isfile(os.path.join(self.download_path, tcf)):
                with open(os.path.join(self.download_path, tcf), 'r') as ff:
                    for line in ff:
                        if search_text in line:
                            ct = ct + 1

            tc = os.path.splitext(tcf)[0].split('_')[-1]
            if tc in ['Bullet', 'Blitz', 'Rapid', 'Classical']:
                self._write_log(f'{tc}_2200', ct)
            else:
                self._write_log('Corr_All', ct)

    def _recently_completed_corr(self):
        # review for recently completed correspondence games
        token_value = os.getenv('LichessAPIToken')
        game_url = 'https://lichess.org/api/games/export/_ids'
        dload_path = os.path.join(self.download_root, 'temp')
        if not os.path.isdir(dload_path):
            os.mkdir(dload_path)
        dest_path = self.download_path

        logging.info('Review for recently completed correspondence games started')
        self._completed_corr_pending(token_value, game_url)

        # download newly completed games and delete table records if download succeeds
        logging.info('Download for recently completed correspondence games started')
        running_total = self._completed_corr_download(token_value, game_url, dload_path)
        self._write_log('Corr_Additional', running_total)

        # verify files were downloaded before continuing
        if running_total == 0:
            logging.warning('No recently completed correspondence games found')
            compcorr_name = None
        else:
            # merge newly downloaded pgns
            logging.info('Merge of recently completed correspondence games started')
            merge_name = self._merge_files(dload_path)

            # update TimeControl and date tags
            logging.info('Recently completed correspondence TimeControl tag update started')
            upd_name = self._update_corr_timecontrol(os.path.join(dload_path, merge_name))

            fixed_name = self._fix_datetags(os.path.join(dload_path, upd_name), True)

            # sort game file
            logging.info('Sorting of recently completed correspondence games started')
            compcorr_name = self._sort_gamefile(dload_path, fixed_name)
            self.files_to_keep.append(compcorr_name)

            # clean up
            dir_files = [f for f in os.listdir(dload_path) if os.path.isfile(os.path.join(dload_path, f))]
            for filename in dir_files:
                if filename != compcorr_name:
                    fname_relpath = os.path.join(dload_path, filename)
                    os.remove(fname_relpath)

            if not os.path.isdir(dest_path):
                os.mkdir(dest_path)
            old_name = os.path.join(dload_path, compcorr_name)
            new_name = os.path.join(dest_path, compcorr_name)
            os.rename(old_name, new_name)

        # create 2200+ corr file for database analysis
        logging.info('2200+ corr game file started')
        self._extract2200corr(self.download_path, dload_path, self.pgn_file_corr_completed, compcorr_name)

        # remove the temp directory - change the working directory first just in case
        if os.path.normpath(os.getcwd()) == os.path.normpath(dload_path):
            os.chdir('..')
        shutil.rmtree(dload_path)

    def _extractbulletblitz(self):
        logging.info('2000 game extract of bullet and blitz games started')
        limit = 2000
        for file in self.timecontrol_files:
            if 'bullet' in file.lower() or 'blitz' in file.lower():
                lim_name = os.path.splitext(file)[0] + f'_{limit}' + '.pgn'
                cmd_text = f'pgn-extract --quiet --gamelimit {limit} --output {lim_name} {file}'
                logging.debug(cmd_text)
                result = subprocess.run(cmd_text, cwd=self.download_path, capture_output=True, text=True)
                if result.returncode != 0:
                    logging.critical(f'Error extracting partial bullet/blitz games: {result.stderr}')
                    raise SystemExit
                self.files_to_keep.append(lim_name)
                self.analysis_files.append(lim_name)

    def _queue_analysis(self):
        # copy files in self.files_to_keep to temp directory
        temp_path = os.path.join(self.download_root, 'analysis')
        if not os.path.isdir(temp_path):
            os.mkdir(temp_path)

        for file in self.analysis_files:
            shutil.move(os.path.join(self.download_path, file), os.path.join(temp_path, file))

        merge_name = self._merge_files(temp_path)

        # move new archive to expected directory
        analysis_dir = misc.get_config('analysisDir', CONFIG_FILE)
        if not os.path.exists(analysis_dir):
            os.mkdir(analysis_dir)
        analysis_name = f'lichess2200_{self.yyyy}{self.mm}_Analysis.pgn'
        shutil.move(os.path.join(temp_path, merge_name), os.path.join(analysis_dir, analysis_name))

        if os.path.normpath(os.getcwd()) == os.path.normpath(temp_path):
            os.chdir('..')
        shutil.rmtree(temp_path)

    def _cleanup(self):
        self._create_zip()

        # move files from processing directory to final location
        final_root = misc.get_config('finalRoot', CONFIG_FILE)
        final_path = os.path.join(final_root, self.yyyy)
        if not os.path.isdir(final_path):
            os.mkdir(final_path)

        all_files = [self.archive_file, self.final_archive]
        move_error = False
        for file in all_files:
            try:
                shutil.move(os.path.join(self.download_path, file), os.path.join(final_path, file))
            except Exception as e:
                logging.error(f"Unable to move file {file} to {final_path}: {e}")
                move_error = True

        if not move_error:
            # remove the temp directory - change the working directory first just in case
            if os.path.normpath(os.getcwd()) == os.path.normpath(self.download_path):
                os.chdir('..')
            shutil.rmtree(self.download_path)

        msg = f'End processing {self.archive_file}'
        notifications.SendTelegramMessage(f'Lichess2200: {msg}')
        logging.info(msg)

    def _write_log(self, field_name=None, field_value=None):
        conn = sql.connect(self.conn_str)
        csr = conn.cursor()
        if field_name is None:
            qry = f"INSERT INTO ChessWarehouse.dbo.LichessDatabase (Filename) VALUES ('{self.pgn_file}')"
        else:
            qry = f"UPDATE ChessWarehouse.dbo.LichessDatabase SET {field_name} = {field_value} WHERE Filename = '{self.pgn_file}'"

        csr.execute(qry)
        csr.commit()
        conn.close()

    def _update_corr_timecontrol(self, input_file: str) -> str:
        upd_name = f'lichess_tc_updated_{self.yyyy}{self.mm}.pgn'
        output_file = os.path.join(os.path.dirname(input_file), upd_name)
        replacements = {
            '[TimeControl "-"]\n': '[TimeControl "1/86400"]\n',
            '[TimeControl "1 day per move"]\n': '[TimeControl "1/86400"]\n',
            '[TimeControl "2 days per move"]\n': '[TimeControl "1/172800"]\n',
            '[TimeControl "3 days per move"]\n': '[TimeControl "1/259200"]\n',
            '[TimeControl "4 days per move"]\n': '[TimeControl "1/345600"]\n',
            '[TimeControl "5 days per move"]\n': '[TimeControl "1/432000"]\n',
            '[TimeControl "6 days per move"]\n': '[TimeControl "1/518400"]\n',
            '[TimeControl "7 days per move"]\n': '[TimeControl "1/604800"]\n',
            '[TimeControl "8 days per move"]\n': '[TimeControl "1/691200"]\n',
            '[TimeControl "9 days per move"]\n': '[TimeControl "1/777600"]\n',
            '[TimeControl "10 days per move"]\n': '[TimeControl "1/864000"]\n',
            '[TimeControl "11 days per move"]\n': '[TimeControl "1/950400"]\n',
            '[TimeControl "12 days per move"]\n': '[TimeControl "1/1036800"]\n',
            '[TimeControl "13 days per move"]\n': '[TimeControl "1/1123200"]\n',
            '[TimeControl "14 days per move"]\n': '[TimeControl "1/1209600"]\n'
        }

        with open(input_file, 'r', encoding='utf-8') as inFile:
            with open(output_file, 'w', encoding='utf-8') as outFile:
                for line in inFile:
                    for key in replacements:
                        if key in line:
                            line = line.replace(key, replacements[key])
                            break
                    outFile.write(line)

        return upd_name

    def _fix_datetags(self, input_file: str, is_corr: bool) -> str:
        if is_corr:
            new_name = f'lichess_{self.yyyy}{self.mm}_Correspondence.pgn'
        else:
            new_name = f'lichess2200allfixed_{self.yyyy}{self.mm}.pgn'

        # fix date tag if file is earlier than 201804
        if int(self.yyyy + self.mm) <= 201803:
            logging.info(f'Filedate = {self.yyyy}{self.mm}')
            logging.info('Date tag update started')

            output_file = os.path.join(os.path.dirname(input_file), new_name)
            searchExp1 = '[Date "????.??.??"]\n'
            replaceExp1 = ''
            searchExp2 = '[UTCDate'
            replaceExp2 = '[Date'

            with open(input_file, 'r', encoding='utf-8') as inFile:
                with open(output_file, 'w', encoding='utf-8') as outFile:
                    for line in inFile:
                        if searchExp1 in line:
                            line = line.replace(searchExp1, replaceExp1)
                        elif searchExp2 in line:
                            line = line.replace(searchExp2, replaceExp2)
                        outFile.write(line)
        else:
            os.rename(input_file, os.path.join(os.path.dirname(input_file), new_name))

        if os.path.exists(input_file):
            os.remove(input_file)

        return new_name

    def _split_timecontrol(self, input_file: str) -> list[str]:
        i = 0
        tc_files = []
        tc_options = ['Bullet', 'Blitz', 'Rapid', 'Classical']
        tc_min_list = ['60', '180', '601', '1800']
        tc_max_list = ['179', '600', '1799', '86399']
        for tc_type in tc_options:
            logging.info(f'{tc_type} extract started')
            tc_min = tc_min_list[i]
            tc_max = tc_max_list[i]
            new_tc_name = f'lichess2200_{self.yyyy}{self.mm}_{tc_type}.pgn'

            # create time control tag files
            tc_tag_file_min = 'TimeControlTagMin.txt'
            tc_tag_file_min_full = os.path.join(os.path.dirname(input_file), tc_tag_file_min)
            tc_txt = f'TimeControl >= "{tc_min}"'
            with open(tc_tag_file_min_full, 'w') as mn:
                mn.write(tc_txt)

            tc_tag_file_max = 'TimeControlTagMax.txt'
            tc_tag_file_max_full = os.path.join(os.path.dirname(input_file), tc_tag_file_max)
            tc_txt = f'TimeControl <= "{tc_max}"'
            with open(tc_tag_file_max_full, 'w') as mx:
                mx.write(tc_txt)

            # filter min time control
            tmp_file = f'temp{tc_type}_{os.path.basename(input_file)}'
            cmd_text = f'pgn-extract --quiet -t{tc_tag_file_min} --output {tmp_file} {os.path.basename(input_file)}'
            logging.debug(cmd_text)
            result = subprocess.run(cmd_text, cwd=os.path.dirname(input_file), capture_output=True, text=True)
            if result.returncode != 0:
                logging.critical(f'Error extracting minimum time control games: {result.stderr}')
                raise SystemExit

            # filter max time control
            cmd_text = f'pgn-extract --quiet -t{tc_tag_file_max} --output {new_tc_name} {tmp_file}'
            logging.debug(cmd_text)
            result = subprocess.run(cmd_text, cwd=os.path.dirname(input_file), capture_output=True, text=True)
            if result.returncode != 0:
                logging.critical(f'Error extracting maximum time control games: {result.stderr}')
                raise SystemExit

            if tc_type in ['Rapid', 'Classical']:
                self.analysis_files.append(new_tc_name)

            tc_files.append(new_tc_name)
            i = i + 1
            os.remove(os.path.join(os.path.dirname(input_file), tmp_file))
            os.remove(tc_tag_file_min_full)
            os.remove(tc_tag_file_max_full)

        return tc_files

    def _completed_corr_pending(self, token_value: str, game_url: str):
        hdr_json = {'Authorization': 'Bearer ' + token_value, 'Accept': 'application/x-ndjson'}
        completed_status = ['aborted', 'mate', 'resign', 'stalemate', 'timeout', 'draw', 'outoftime', 'cheat']
        curr_unix = int(time.time()*1000)

        connection_url = sa.engine.URL.create(
            drivername='mssql+pyodbc',
            query={'odbc_connect': self.conn_str}
        )
        qryengine = sa.create_engine(connection_url)
        conn = sql.connect(self.conn_str)
        csr = conn.cursor()

        # update database table
        game_qry = """
SELECT TOP 300 GameID
FROM ChessWarehouse.dbo.OngoingLichessCorr
WHERE (Inactive = 0 AND (LastReviewed IS NULL OR DATEDIFF(DAY, LastReviewed, GETDATE()) >= 7))
OR (Inactive = 1 AND DATEDIFF(DAY, LastReviewed, GETDATE()) >= 90)
        """
        game_rec = pd.read_sql(game_qry, qryengine).values.tolist()
        game_list = [j for sub in game_rec for j in sub]
        game_ct = len(game_list)
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
                                    upd_qry = 'UPDATE ChessWarehouse.dbo.OngoingLichessCorr '
                                    upd_qry = upd_qry + f'SET Download = 1, LastMoveAtUnix = {last_move}, LastReviewed = GETDATE() '
                                    upd_qry = upd_qry + f"WHERE GameID = '{game_id}'"
                                else:
                                    # if no move has been played in at least 30 days, game is still considered ongoing. update to inactive
                                    if curr_unix - last_move > 2592000000:
                                        inact = '1'
                                    else:
                                        inact = '0'
                                    upd_qry = 'UPDATE ChessWarehouse.dbo.OngoingLichessCorr '
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
                                upd_qry = f"UPDATE ChessWarehouse.dbo.OngoingLichessCorr SET LastReviewed = GETDATE(), Inactive = 1 WHERE GameID = '{gm}'"
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

            game_rec = pd.read_sql(game_qry, qryengine).values.tolist()
            game_list = [j for sub in game_rec for j in sub]
            game_ct = len(game_list)

        conn.close()
        qryengine.dispose()

    def _completed_corr_download(self, token_value: str, game_url: str, dload_path: str) -> int:
        hdr_pgn = {'Authorization': 'Bearer ' + token_value, 'Accept': 'application/x-chess-pgn'}
        dte = dt.datetime.now().strftime('%Y%m%d%H%M%S')

        connection_url = sa.engine.URL.create(
            drivername='mssql+pyodbc',
            query={'odbc_connect': self.conn_str}
        )
        qryengine = sa.create_engine(connection_url)
        conn = sql.connect(self.conn_str)
        csr = conn.cursor()

        total_qry = 'SELECT COUNT(GameID) FROM ChessWarehouse.dbo.OngoingLichessCorr WHERE Download = 1'
        total_rec = pd.read_sql(total_qry, qryengine).values.tolist()
        total_dl = int(total_rec[0][0])
        dl_qry = 'SELECT TOP 300 GameID FROM ChessWarehouse.dbo.OngoingLichessCorr WHERE Download = 1 ORDER BY GameID'
        dl_delete = f'DELETE FROM ChessWarehouse.dbo.OngoingLichessCorr WHERE GameID IN ({dl_qry})'
        dl_rec = pd.read_sql(dl_qry, qryengine).values.tolist()
        dl_list = [j for sub in dl_rec for j in sub]
        dl_ct = len(dl_list)
        ctr = 1
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

            dl_rec = pd.read_sql(dl_qry, qryengine).values.tolist()
            dl_list = [j for sub in dl_rec for j in sub]
            dl_ct = len(dl_list)
            ctr = ctr + 1

        conn.close()
        qryengine.dispose()

        return total_dl

    def _format_result(self, game_text: chess.pgn.Game, tag: str) -> str:
        tag_text = game_text.headers.get(tag)
        res = None
        if tag_text is not None:
            if tag_text == '1-0':
                res = '1.0'
            elif tag_text == '0-1':
                res = '0.0'
            elif tag_text == '1/2-1/2':
                res = '0.5'
        return res

    def _format_source_id(self, game_text: chess.pgn.Game, tag: str) -> str:
        tag_text = game_text.headers.get(tag)
        site_id = tag_text.split('/')[-1] if tag_text is not None else None
        return site_id

    def _merge_files(self, file_path: str) -> str:
        merge_name = 'mergedgamefile.pgn'
        cmd_text = 'copy /B *.pgn ' + merge_name + ' >nul'
        result = subprocess.run(cmd_text, cwd=file_path, capture_output=True, text=True)
        if result.returncode != 0:
            logging.critical(f'Error merging game files: {result.stderr}')
            raise SystemExit

        return merge_name

    def _sort_gamefile(self, file_path: str, file_name: str) -> str:
        dte = dt.datetime.now().strftime('%Y%m%d%H%M%S')
        idx = []
        game_date = []
        game_text = []
        gm_idx = 0
        with open(os.path.join(file_path, file_name), mode='r', encoding='utf-8', errors='replace') as pgn:
            gm_txt = chess.pgn.read_game(pgn)
            while gm_txt is not None:
                idx.append(gm_idx)
                try:
                    dt_val = gm_txt.headers['UTCDate']
                except KeyError:
                    dt_val = gm_txt.headers['Date']
                game_date.append(dt_val)
                game_text.append(gm_txt)
                gm_txt = chess.pgn.read_game(pgn)
                gm_idx = gm_idx + 1

        idx_sort = [x for _, x in sorted(zip(game_date, idx))]
        sort_name = f'lichess_Correspondence_{dte}_NewlyCompleted.pgn'
        with open(os.path.join(file_path, sort_name), 'w', encoding='utf-8') as sort_file:
            for i in idx_sort:
                sort_file.write(str(game_text[i]) + '\n\n')

        return sort_name

    def _extract2200corr(self, file_path: str, temp_path: str, monthly_file: str, complete_file: str):
        shutil.copy(os.path.join(file_path, monthly_file), temp_path)
        if complete_file is not None:
            # this would happen when no recently completed correspondence game were found
            shutil.copy(os.path.join(file_path, complete_file), temp_path)

        merge_name = self._merge_files(temp_path)

        pgn_tag_name = 'LichessPgnTags.txt'
        pgn_tag_file = os.path.join(temp_path, pgn_tag_name)
        with open(pgn_tag_file, 'w') as tf:
            tf.write('WhiteElo >= "2200"')
            tf.write('\n')
            tf.write('BlackElo >= "2200"')

        pgn_name = f'lichess2200all_{self.yyyy}{self.mm}.pgn'
        cmd_text = f'pgn-extract -N -V -D -pl2 -t"{pgn_tag_file}" --quiet --fixresulttags --fixtagstrings --nosetuptags --output {pgn_name} {merge_name}'
        logging.debug(cmd_text)
        result = subprocess.run(cmd_text, cwd=temp_path, capture_output=True, text=True)
        if result.returncode != 0:
            logging.critical(f'Error extracting 2200 correspondence games: {result.stderr}')
            raise SystemExit
        os.remove(os.path.join(temp_path, pgn_tag_name))

        final_name = f'lichess2200_{self.yyyy}{self.mm}_Correspondence.pgn'
        old_name = os.path.join(temp_path, pgn_name)
        new_name = os.path.join(file_path, final_name)
        os.rename(old_name, new_name)
        self.files_to_keep.append(final_name)
        self.analysis_files.append(final_name)

    def _create_zip(self):
        # copy files in self.files_to_keep to temp directory
        temp_path = os.path.join(self.download_root, 'temparchive')
        if not os.path.isdir(temp_path):
            os.mkdir(temp_path)

        for file in self.files_to_keep:
            shutil.move(os.path.join(self.download_path, file), os.path.join(temp_path, file))

        # compress files in temp directory
        cmd = [
            r'C:\Program Files\7-Zip\7z.exe',  # expectation is a standard 7-Zip install
            'a',  # Add to archive
            '-t7z',  # archive type
            '-mx=9',  # maximum compression
            '-m0=lzma2',  # compression method
            '-mfb=64',  # number of fast bytes
            '-md=32m',  # dictionary size
            '-ms=on',  # solid mode
            os.path.join(temp_path, self.final_archive)
        ]

        cmd.extend([f for f in self.files_to_keep])  # add all files to compress
        result = subprocess.run(cmd, cwd=temp_path, capture_output=True, text=True)
        if result.returncode != 0:
            logging.critical(f'Error archiving final results: {result.stderr}')
            raise SystemExit

        # move new archive to expected directory
        shutil.move(os.path.join(temp_path, self.final_archive), os.path.join(self.download_path, self.final_archive))

        if os.path.normpath(os.getcwd()) == os.path.normpath(temp_path):
            os.chdir('..')
        shutil.rmtree(temp_path)
