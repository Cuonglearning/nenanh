from PIL import Image
from bitarray import bitarray
import pickle
import time
import psycopg2

def decode_data(encoded_data, huffman_codes):
    reverse_codebook = {v: k for k, v in huffman_codes.items()}
    decoded_data = []
    buffer = ''
    for bit in encoded_data.to01():  # Convert bitarray to string of '0's and '1's
        buffer += bit
        if buffer in reverse_codebook:
            decoded_data.append(reverse_codebook[buffer])
            buffer = ''
    return bytes(decoded_data)

def huffman_decompress(output_file, image_name, conn):
    start_time = time.time()

    # Retrieve compressed data from database using image name
    with conn.cursor() as cur:
        cur.execute("""
            SELECT image_name, compressed_data, width, height
            FROM huffman_compress
            WHERE image_name = %s
        """, (image_name,))
        result = cur.fetchone()
        if not result:
            print(f"No data found for image name '{image_name}'")
            return
        
        _, compressed_data, width, height = result

    # Unpack compressed data
    huffman_codes, encoded_data = pickle.loads(compressed_data)
    encoded_bitarray = bitarray()
    encoded_bitarray.frombytes(encoded_data)

    # Decode data
    decoded_data = decode_data(encoded_bitarray, huffman_codes)

    # Convert back to image and save
    img = Image.frombytes("L", (width, height), decoded_data)
    img.save(output_file)

    end_time = time.time()
    decompression_time = end_time - start_time
    
    print(f"Image '{image_name}' decompressed and saved to '{output_file}' in {decompression_time:.2f} seconds")

if __name__ == "__main__":
    conn = psycopg2.connect(
        host="localhost",
        database="AnhVienTham",
        user="postgres",
        password="khongnho"
    )

    try:
        output_file = r"decompress\decompressed_image.TIF"
        image_name = "LC08_L2SP_125052_20170214_20200905_02_T1_SR_B1.TIF"  # Tên ảnh để truy vấn
        huffman_decompress(output_file, image_name, conn)
    finally:
        conn.close()
