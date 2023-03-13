import numisk as nm

with nm.Chunk("./test_path/money/0001", "r") as chunk:
    print(chunk)