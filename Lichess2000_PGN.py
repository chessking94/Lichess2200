import os
import datetime as dt

def main():
    start_time = dt.datetime.now().strftime('%H:%M:%S')
    print("Process started at " + start_time)
    file_path = r'D:\eehunt\LONGTERM\Chess\LichessPGN\2021'
    file_name = 'lichess_db_standard_rated_2021-07.pgn.bz2'

    # decompress file
    dte1 = dt.datetime.now().strftime('%H:%M:%S')
    print('Decompression started at ' + dte1)
    cmd_text = '7z e ' +  file_name
    os.chdir(file_path)
    os.system('cmd /C ' + cmd_text)
    dte2 = dt.datetime.now().strftime('%H:%M:%S')
    print('Decompression ended at ' + dte1)
    extracted_file = file_name.replace('.bz2', '')

    # create error log
    dte1 = dt.datetime.now().strftime('%H:%M:%S')
    print('Error log creation started at ' + dte1)
    yyyy = extracted_file[26:30]
    mm = extracted_file[31:33]
    error_file = 'lichess_' + yyyy + mm + '_errors.log'
    cmd_text = 'pgn-extract --quiet -r -l' + error_file + ' ' + extracted_file
    os.system('cmd /C ' + cmd_text)
    dte2 = dt.datetime.now().strftime('%H:%M:%S')
    print('Error log creation ended at ' + dte2)

    # create lichess2000 file
    dte1 = dt.datetime.now().strftime('%H:%M:%S')
    print('Lichess2000 pgn creation started at ' + dte1)
    tag_file = r'C:\Users\eehunt\Documents\Chess\Scripts\LichessPgnTags.txt'
    pgn_name = 'lichess2000_' + yyyy + mm + '.pgn'
    cmd_text = 'pgn-extract -C -N -V -D -pl2 -t"' + tag_file + '" --quiet --fixresulttags --fixtagstrings --nosetuptags --output ' + pgn_name + ' ' + extracted_file
    os.system('cmd /C ' + cmd_text)
    dte2 = dt.datetime.now().strftime('%H:%M:%S')
    print('Lichess2000 pgn ended started at ' + dte2)

    # delete old files
    fname_relpath = os.path.join(file_path, extracted_file)
    os.remove(fname_relpath)

    end_time = dt.datetime.now().strftime('%H:%M:%S')
    print("Process ended at " + end_time)

if __name__ == '__main__':
    main()