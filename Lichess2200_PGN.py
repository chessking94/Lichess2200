import os
import fileinput
import datetime as dt

def main():
    start_time = dt.datetime.now().strftime('%H:%M:%S')
    print("Process started at " + start_time)
    log_file = r'D:\eehunt\LONGTERM\Chess\LichessPGN\Summary_Log.txt'
    file_path = r'D:\eehunt\LONGTERM\Chess\LichessPGN\2022'
    f_arr = ['lichess_db_standard_rated_2022-04.pgn.bz2']
    for file_name in f_arr:
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
        print('Decompression started at ' + decomp_start)
        cmd_text = '7z e ' +  file_name
        if os.getcwd != file_path:
            os.chdir(file_path)
        os.system('cmd /C ' + cmd_text)
        decomp_end = dt.datetime.now().strftime('%H:%M:%S')
        print('Decompression ended at ' + decomp_end)
        extracted_file = file_name.replace('.bz2', '')
        
        yyyy = extracted_file[26:30]
        mm = extracted_file[31:33]

        # create error log
        error_start = dt.datetime.now().strftime('%H:%M:%S')
        print('Error log creation started at ' + error_start)
        error_file = 'lichess_' + yyyy + mm + '_errors.log'
        cmd_text = 'pgn-extract --quiet -r -l' + error_file + ' ' + extracted_file
        if os.getcwd != file_path:
            os.chdir(file_path)
        os.system('cmd /C ' + cmd_text)
        error_end = dt.datetime.now().strftime('%H:%M:%S')
        print('Error log creation ended at ' + error_end)

        start_2200 = dt.datetime.now().strftime('%H:%M:%S')
        print('Lichess2200 pgn creation started at ' + start_2200)

        # update correspondence game TimeControl tag
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

        # extract only 2200+ rating games
        tag_file = r'C:\Users\eehunt\Repository\Lichess2200\LichessPgnTags.txt'
        pgn_name = 'lichess2200all_' + yyyy + mm + '.pgn'
        cmd_text = 'pgn-extract -C -N -V -D -pl2 -t"' + tag_file + '" --quiet --fixresulttags --fixtagstrings --nosetuptags --output ' + pgn_name + ' ' + upd_name
        if os.getcwd() != file_path:
            os.chdir(file_path)
        os.system('cmd /C ' + cmd_text)

        # fix date tag if file is earlier than 201804
        bad_dates = 0
        if int(yyyy + mm) <= 201803:
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
        
        if bad_dates:
            curr_name = new_pgn_name_2
        else:
            curr_name = pgn_name

        # separate into time control files
        i = 0
        tc_files = []
        tc_options = ['Bullet', 'Blitz', 'Rapid', 'Classical', 'Correspondence']
        tc_min_list = ['60', '180', '601', '1800', '86400']
        tc_max_list = ['179', '600', '1799', '86399', '1209600']
        for tc_type in tc_options:
            tc_min = tc_min_list[i]
            tc_max = tc_max_list[i]
            new_tc_name = 'lichess2200_' + yyyy + mm + '_' + tc_type + '.pgn'
            tc_files.append(new_tc_name)

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

            i = i + 1
            os.remove(os.path.join(file_path, tmp_file))
            os.remove(tc_tag_file_min_full)
            os.remove(tc_tag_file_max_full)

        end_2200 = dt.datetime.now().strftime('%H:%M:%S')
        print('Lichess2200 pgn creation ended at ' + end_2200)

        start_corr = dt.datetime.now().strftime('%H:%M:%S')
        print('LichessCorr pgn creation started at ' + start_corr)

        # split pgn into corr games
        tag_file = r'C:\Users\eehunt\Repository\Lichess2200\LichessCorrTag.txt'
        minply = '6'
        corr_name = 'lichess_correspondence_orig_' + yyyy + mm + '.pgn'
        cmd_text = 'pgn-extract -C -N -V -D -s -pl' + minply + ' -t"' + tag_file + '" --quiet --fixresulttags --fixtagstrings --nosetuptags -o' + corr_name + ' ' + upd_name
        if os.getcwd() != file_path:
            os.chdir(file_path)
        os.system('cmd /C ' + cmd_text)

        # fix date tag if file is earlier than 201804
        new_name = 'lichess_correspondence_' + yyyy + mm + '.pgn'
        bad_dates = 0
        if int(yyyy + mm) <= 201803:
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
        
        end_corr = dt.datetime.now().strftime('%H:%M:%S')
        print('LichessCorr pgn creation ended at ' + end_corr)

        # clean up old files
        os.remove(os.path.join(file_path, extracted_file))
        os.remove(os.path.join(file_path, upd_name))
        if bad_dates:
            os.remove(nfile2)
            os.remove(os.path.join(file_path, corr_name))
        else:
            os.rename(os.path.join(file_path, corr_name), os.path.join(file_path, new_name))

        # write to log file
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
                with open(os.path.join(file_path, tcf), 'r') as ff:
                    for line in ff:
                        if search_text in line:
                            ct = ct + 1
                    f.write('\t' + str(ct))
            
            # corr game count
            ct = 0
            with open(os.path.join(file_path, new_name), 'r') as fff:
                for line in fff:
                    if search_text in line:
                        ct = ct + 1
                f.write('\t' + str(ct))

            f.write('\n')

        """
        # split file into smaller 2m game files
        cmd_text = 'pgn-extract --quiet -#2000000,' + str(yyyy) + str(mm) + '01 ' + pgn_name
        if os.getcwd() != file_path:
            os.chdir(file_path)
        os.system('cmd /C ' + cmd_text)
        """


if __name__ == '__main__':
    main()