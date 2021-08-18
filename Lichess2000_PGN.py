import os
import fileinput
import datetime as dt

def main():
    start_time = dt.datetime.now().strftime('%H:%M:%S')
    print("Process started at " + start_time)
    log_file = r'D:\eehunt\LONGTERM\Chess\LichessPGN\File_Processing_Log.txt'
    file_path = r'D:\eehunt\LONGTERM\Chess\LichessPGN\2021'
    file_name = 'lichess_db_standard_rated_2021-07.pgn.bz2'

    # set default values, if I want to mess around and only skip certain steps
    start_date = dt.datetime.now().strftime('%Y-%m-%d')
    decomp_start = ''
    decomp_end = ''
    error_start = ''
    error_end = ''
    pgn_start = ''
    pgn_end = ''

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

    # if file is already extracted, specify here
    #extracted_file = 'lichess_db_standard_rated_2021-03.pgn'
    
    # create error log
    error_start = dt.datetime.now().strftime('%H:%M:%S')
    print('Error log creation started at ' + error_start)
    yyyy = extracted_file[26:30]
    mm = extracted_file[31:33]
    error_file = 'lichess_' + yyyy + mm + '_errors.log'
    cmd_text = 'pgn-extract --quiet -r -l' + error_file + ' ' + extracted_file
    if os.getcwd != file_path:
        os.chdir(file_path)
    os.system('cmd /C ' + cmd_text)
    error_end = dt.datetime.now().strftime('%H:%M:%S')
    print('Error log creation ended at ' + error_end)

    # create 2000+ rating file
    pgn_start = dt.datetime.now().strftime('%H:%M:%S')
    print('Lichess2000 pgn creation started at ' + pgn_start)
    tag_file = r'C:\Users\eehunt\Repository\Lichess2000\LichessPgnTags.txt'
    pgn_name = 'lichess2000all_' + yyyy + mm + '.pgn'
    cmd_text = 'pgn-extract -C -N -V -D -pl2 -t"' + tag_file + '" --quiet --fixresulttags --fixtagstrings --nosetuptags --output ' + pgn_name + ' ' + extracted_file
    if os.getcwd() != file_path:
        os.chdir(file_path)
    os.system('cmd /C ' + cmd_text)

    # update correspondence game TimeControl tag
    new_pgn_name = 'lichess2000fixed_' + yyyy + mm + '.pgn'
    ofile = os.path.join(file_path, pgn_name)
    nfile = os.path.join(file_path, new_pgn_name)
    searchExp = '[TimeControl "-"]\n'
    replaceExp = '[TimeControl "1/86400"]\n'
    wfile = open(nfile, 'w')
    for line in fileinput.input(ofile, inplace=1):
        if searchExp in line:
            line = line.replace(searchExp, replaceExp)
        wfile.write(line)
    wfile.close()

    # rerun pgn-extract to leave all non-bullet games
    tag_file = r'C:\Users\eehunt\Repository\Lichess2000\LichessNoBulletTag.txt'
    pgn_name = 'lichess2000_' + yyyy + mm + '.pgn'
    cmd_text = 'pgn-extract -t"' + tag_file + '" --quiet --output ' + pgn_name + ' ' + new_pgn_name
    if os.getcwd() != file_path:
        os.chdir(file_path)
    os.system('cmd /C ' + cmd_text)
    pgn_end = dt.datetime.now().strftime('%H:%M:%S')
    print('Lichess2000 pgn creation ended at ' + pgn_end)

    # delete old files
    fname_relpath = os.path.join(file_path, extracted_file)
    os.remove(fname_relpath)
    os.remove(ofile)
    os.remove(nfile)

    # write to log files
    # timing
    with open(log_file, 'a') as f:
        f.write(extracted_file + '\t' + start_date + '\t' + decomp_start + '\t' + decomp_end + '\t' + error_start + '\t' + error_end + '\t' + pgn_start + '\t' + pgn_end +'\n')
    f.close()

    # game count
    log_counts = r'D:\eehunt\LONGTERM\Chess\LichessPGN\Game_Counts.txt'
    search_text = '[Event "'
    ct = 0
    with open(os.path.join(file_path, pgn_name), 'r') as f:
        for line in f:
            if search_text in line:
                ct = ct + 1
        with open(log_counts, 'a') as lf:
            lf.write(pgn_name + '\t' + str(ct) + '\n')
        lf.close()
    f.close()

    end_time = dt.datetime.now().strftime('%H:%M:%S')
    print("Process ended at " + end_time)

if __name__ == '__main__':
    main()