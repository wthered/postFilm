# Code pasted from https://alyssaq.github.io/2015/20150426-simple-movie-recommender-using-svd/
# todo: See results from https://antoinevastel.com/machine%20learning/python/2016/02/14/svd-recommender-system.html
import numpy as np
import pandas as pd

data_names = ['user_id', 'movie_id', 'rating', 'time']
data = pd.read_csv('ml-1m/ratings.dat', names=['user_id', 'movie_id', 'rating', 'time'], engine='python', delimiter='::')

# for (i, row) in data.iterrows():
# 	print(row)

movie_names = ['movie_id', 'title', 'genre']
movie_data = pd.read_csv('ml-1m/movies.dat', names=['movie_id', 'title', 'genre'], engine='python', delimiter='::')

ratings_shape = (np.max(data.movie_id.values), np.max(data.user_id.values))
ratings_mat = np.ndarray(shape=(np.max(data.movie_id.values), np.max(data.user_id.values)), dtype=np.uint8)
ratings_mat[data.movie_id.values - 1, data.user_id.values - 1] = data.rating.values

normalised_mat = ratings_mat - np.asarray([(np.mean(ratings_mat, 1))]).T

# 4) Compute SVD


A = normalised_mat.T / np.sqrt(ratings_mat.shape[0] - 1)
U, S, V = np.linalg.svd(A)


# 5) Calculate cosine similarity, sort by most similar and return the top N.

# data, movie_id, top_n=10
def top_cosine_similarity(data, movie_id, top_many=10):
	# Movie id starts from 1
	index = movie_id - 1
	movie_row = data[index, :]
	magnitude = np.sqrt(np.einsum('ij, ij -> i', data, data))
	similarity = np.dot(movie_row, data.T) / (magnitude[index] * magnitude)
	sort_indexes = np.argsort(-similarity)
	return sort_indexes[:top_many]


# Helper function to print top N similar movies
# movie_data, movie_id, top_indexes
def print_similar_movies(movie_data, movie_id, top_indexes):
	print('Recommendations for {0}: \n'.format(
		movie_data[movie_data.movie_id == movie_id].title.values[0]))
	for similar_id in top_indexes + 1:
		print('{}\t{}'.format(similar_id,movie_data[movie_data.movie_id == similar_id].title.values[0]))


k = 50
movie_id = 1  # Grab an id from movies.dat
top_n = 10

sliced = V.T[:, :k]  # representative data
indexes = top_cosine_similarity(sliced, movie_id, top_n)  # sliced, movie_id, top_n)
# print('{0}'.format(indexes))
print_similar_movies(movie_data, movie_id, indexes)  # movie_data, movie_id, indexes
