# Lichess2200

This process takes a predetermined path and entered filename of a monthly Lichess database file and will process it to the below specifications:

1. No variations, comments, or other annotations. This includes move times.
2. No duplicate games.
3. A minimum of 1 ply played by both White and Black.
4. Both the White and Black elo ratings must be a minimum of 2200.
5. Inconsistent results (i.e. result shows a draw but the last move was checkmate) will be corrected.
6. Improperly formatted tag strings will be corrected, if possible.
7. Games are restricted to standard chess only.
8. A separate file containing all correspondence games, regardless of rating, will also be created.

Note: The original Lichess database downloads through 201803 were missing the "Date" tag. The "UTCDate" tag can be converted to "Date" instead.

Game files can be found at https://database.lichess.org/ and the free tool pgn-extract (https://www.cs.kent.ac.uk/people/staff/djb/pgn-extract/) is used for game processing. Additionally, the free software 7-Zip (https://www.7-zip.org/) is also used.