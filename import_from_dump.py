import pandas as pd
from credentials import *

if __name__ == "__main__":
	this_moment = (datetime.now(time_zone) - timedelta(days=2)).strftime("%m_%d_%Y")
	remote_dump = 'https://files.tmdb.org/p/exports/movie_ids_{}.json.gz'.format(this_moment)
	print("Try to download {} first".format(remote_dump))

	# 1. Φόρτωση - Το pandas διαβάζει απευθείας το .gz
	df = pd.read_json(remote_dump, lines=True)

	# 2. Refactoring με χρήση Method Chaining για καθαρότητα
	df = (df
	      .rename(columns={'id': 'link'})
	      .assign(film=None, page=None, created_at=this_moment, updated_at=this_moment)
	      .drop(columns=['adult', 'video'], errors='ignore')
	      .set_index('link')
	      .sort_index()                      # Sorting index για ταχύτητα στις μελλοντικές αναζητήσεις
	      )

	# 3. Αναδιάταξη στηλών
	# Τις ορίζουμε ρητά για να είμαστε σίγουροι για το format
	df = df[['film', 'page', 'original_title', 'popularity']]

	# 4. Αποθήκευση & Έλεγχος
	# df.to_parquet('movies_processed.parquet') # Η καλύτερη επιλογή για 1.5M γραμμές
	print("Συνολικές γραμμές: {}".format(df.shape[0]))

	df.to_csv('movie_file.csv.gz')

	print(f"Το DataFrame φορτώθηκε με {len(df):,} γραμμές.")
