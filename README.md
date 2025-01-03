# Lichess2200
This process will download, parse, and return files related to the monthly game dumps provided by Lichess.

## Dependencies
- `pgn-extract`, which can be downloaded from [https://www.cs.kent.ac.uk/people/staff/djb/pgn-extract](url).
- SQL Server instance as defined in [https://github.com/chessking94/db_ChessWarehouse](url), specifically `dbo.LichessDatabase` and `dbo.OngoingLichessCorr`.

## Extract Criteria
- No variations, comments, or other annotations. Move times *are* included.
- No duplicate games.
- A minimum of 1 ply played by both White and Black.
- Both the White and Black elo ratings must be a minimum of 2200.
- Inconsistent results (i.e. result shows a draw but the last move was checkmate) will be corrected.
- Improperly formatted tag strings will be corrected, if possible.
- Games are restricted to standard chess only.


## Monthly Files Returned
1. `lichess_db_standard_rated_YYYY-MM.pgn.zst`: The original `.zst` file downloaded from [database.lichess.org](url).
2. `lichess_YYYYMM_errors.log`: The output of `pgn-extract -r`.
3. `lichess2200_YYYYMM_Bullet.pgn`: All games in the bullet time control for players of ratings over 2200.
4. `lichess2200_YYYYMM_Blitz.pgn`: All games in the blitz time control for players of ratings over 2200.
5. `lichess2200_YYYYMM_Rapid.pgn`: All games in the rapid time control for players of ratings over 2200.
6. `lichess2200_YYYYMM_Classical.pgn`: All games in the classical time control for players of ratings over 2200.
7. `lichess2200_YYYYMM_Correspondence.pgn`: All games in the correspondence time control for players of ratings over 2200.
8. `lichess_correspondence_YYYYMM_Completed.pgn`: All completed games in the correspondence time control, regardless of rating.

### Additional files
There are three additional files generated for my personal use.

9. `lichess_correspondence_yyyymmddHHMMSS_NewlyCompleted.pgn`: All games from prior months that were completed since the last run of this process. Reviews games logged to `dbo.OngoingLichessCorr`. Unlike other files, the timestamp in the filename is the runtime rather than the year/month of the downloaded file.
10. `lichess2200_YYYYMM_Bullet_2000.pgn`: A 2000 game sampling of the `lichess2200_YYYYMM_Bullet.pgn` file, for separate analysis purposes.
11. `lichess2200_YYYYMM_Blitz_2000.pgn`: A 2000 game sampling of the `lichess2200_YYYYMM_Blitz.pgn` file, for separate analysis purposes.

## Further Notes
- The original Lichess database downloads through 201803 did not include the required pgn "Date" tag. It did, however, include a "UTCDate" tag. This tag is natively converted into a "Date" tag instead.
