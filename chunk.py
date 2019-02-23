import jsonlines
import math
import sys
from tqdm import tqdm

# Source path to file from CLI args.
SOURCE_PATH = sys.argv[1]
# Chunk output directory.
OUT_DIR = './data'
# Chunk size.
CHUNK_SIZE = 100000
# Optional limit after which to abort chunking.
LIMIT = None

# Create current writer for chunked segment.
def create_current_writer(i):
    chunk_no = math.floor(i / CHUNK_SIZE)
    output_filename = OUT_DIR + '/ocdata.chunk' + str(chunk_no) + '.jsonl'
    current_writer = jsonlines.open(output_filename, mode='w')
    return current_writer

print("Chunking data from input: " + SOURCE_PATH)

i = 0
current_writer = create_current_writer(i)
reader = jsonlines.open(SOURCE_PATH)
for obj in tqdm(reader):
    if ((i != 0) and (i % CHUNK_SIZE == 0)):
        current_writer.close()
        current_writer = create_current_writer(i)
    # Stop on limit if any.
    if (LIMIT != None and i >= LIMIT):
        break
    # Add line number.
    obj['_line'] = i
    current_writer.write(obj)
    i = i + 1
    
reader.close()
current_writer.close()
print("Done chunking " + str(i) + " records.")