import fileinput
import os

def main():
    # ENTER THE PATH AND ORIGINAL FILE NAME
    base_path = r'D:\eehunt\LONGTERM\Chess\LichessPGN\2018'
    fname = ['lichess2000_201801','lichess2000_201802','lichess2000_201803']

    for i in fname:
        # PATH CONCATENATION AND SEARCH/REPLACE CRITERIA
        ofile = os.path.join(base_path, i) + '.pgn'
        nfile = os.path.join(base_path, i + '_fixed') + '.pgn'
        searchExp1 = '[Date "????.??.??"]\n'
        replaceExp1 = ''
        searchExp2 = '[UTCDate'
        replaceExp2 = '[Date'

        # DO IT
        wfile = open(nfile, 'w')
        for line in fileinput.input(ofile, inplace=1):
            if searchExp1 in line:
                line = line.replace(searchExp1, replaceExp1)
            elif searchExp2 in line:
                line = line.replace(searchExp2, replaceExp2)
            wfile.write(line)
        wfile.close()
        
        print('Done with ' + i + '.pgn')

if __name__ == '__main__':
    main()