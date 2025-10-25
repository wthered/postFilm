import time

import requests

from credentials import *


def should_keep_line(line: str) -> bool:
	"""
	Defines the condition for keeping a line.

	If this function returns TRUE, the line is KEPT (not removed).
	If this function returns FALSE, the line is REMOVED (not kept).

	*** CHANGE THE LOGIC BELOW TO MATCH YOUR CONDITION ***
	"""
	response = requests.get(line, headers=http_headers).json()
	print("Keep line: {}".format('id' in response))
	time.sleep(1)

	# Default: Keep all other lines
	return 'id' in response


def filter_existing_file(filename: str):
	"""
	Reads the existing file, filters lines based on a condition, and overwrites the file.
	"""
	try:
		# 1. READ ALL LINES INTO MEMORY
		with open(filename, 'r') as f:
			# We use readlines() to get a list of all lines
			lines = f.readlines()

		print(f"File '{filename}' read successfully. Total lines found: {len(lines)}")

		# 2. FILTER THE LINES
		# Create a new list containing only the lines that satisfy the should_keep_line condition
		# The line will be kept if should_keep_line(line) returns True.
		filtered_lines = [line for line in lines if should_keep_line(line)]

		lines_removed = len(lines) - len(filtered_lines)

		# Check if any changes were made before writing
		if lines_removed == 0:
			print("No lines matched the removal condition. File left unchanged.")
			return

		print(f"Filtering complete. Lines kept: {len(filtered_lines)}. Lines removed: {lines_removed}")

		# 3. WRITE THE FILTERED LIST BACK TO THE FILE (Overwriting the original)
		with open(filename, 'w') as f:
			f.writelines(filtered_lines)

		print(f"File '{filename}' has been successfully overwritten with filtered content.")

	except FileNotFoundError:
		print(f"Error: The file '{filename}' was not found. Please ensure it exists in the same directory.")
	except Exception as e:
		print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
	# Ensure you modify the should_keep_line function above with your actual condition
	filter_existing_file('links_not_found.txt')
