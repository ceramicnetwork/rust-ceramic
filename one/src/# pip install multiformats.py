# pip install multiformats
# pip install cbor2
import multiformats
import cbor2
import sys

# # usage
# > cat something.car | python3 car_view.py

# CAR file format
# | length | Header |
# | length | CID | block |
# ...
# | length | CID | block |

car_bytes =  sys.stdin.buffer.read() # read the car file from stdin
length, _, rest = multiformats.varint.decode_raw(car_bytes) # file starts with the header length varint
head = cbor2.loads(rest[:length])  # read the header CBOR object {'roots': [CID], 'version': 1}
rest = rest[length:]  # move up the pointer
print(head)

while rest:
    # | length | CID | block |
    length, _, rest = multiformats.varint.decode_raw(rest) # read the block length varint
    cid_and_block = rest[:length]
    cid = multiformats.CID.decode(cid_and_block[:36])  # CID first 36 bytes
    block = cid_and_block[36:] # block rest bytes
    obj = cbor2.loads(block) if cid.codec.name == "dag-cbor" else block
    rest = rest[length:]  # move up the pointer
    print(cid.encode(base="base16"), obj) 