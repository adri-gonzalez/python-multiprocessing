# % % timeit -n1 -r1

# location of this file
# f = ("/project/shared/biohpc_training/large_txt.bin")
f = "/Users/adriangonzalez/Documents/github-repository/python-multiprocessing/02_deep_multiprocessing/large_text.txt"


# Create a Python 'generator'
# Reads in large binary file in 256k chunks
# Processes each chunk and tallies the word count
def read_in_chunks(file_object, chunk_size=256 * 1024):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 256k."""
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data


dogs = 0
cats = 0
boys = 0
girls = 0

# Open the file, read in chunks sequentially, and count the number of word instances
with open(f, "rb") as fin:
    for chunk in read_in_chunks(fin):
        line_list = chunk.decode('latin-1').strip().split()

        dogs += line_list.count("dog")
        cats += line_list.count("cat")
        boys += line_list.count("boy")
        girls += line_list.count("girl")

print(dogs, cats, boys, girls)
