use std::io::Read;

use cid::Cid;

use super::util::{ld_read, read_node};
use crate::{header::CarHeader, Error};

/// Reads CAR files that are in a BufReader
#[derive(Debug)]
pub struct CarReader<R> {
    reader: R,
    header: CarHeader,
    buffer: Vec<u8>,
}

impl<R> CarReader<R>
where
    R: Read + Send + Unpin,
{
    /// Creates a new CarReader and parses the CarHeader
    pub fn new(mut reader: R) -> Result<Self, Error> {
        let mut buffer = Vec::new();

        match ld_read(&mut reader, &mut buffer)? {
            Some(buf) => {
                let header = CarHeader::decode(buf)?;

                Ok(CarReader {
                    reader,
                    header,
                    buffer,
                })
            }
            None => Err(Error::Parsing(
                "failed to parse uvarint for header".to_string(),
            )),
        }
    }

    /// Returns the header of this car file.
    pub fn header(&self) -> &CarHeader {
        &self.header
    }

    /// Returns the next IPLD Block in the buffer
    pub fn next_block(&mut self) -> Result<Option<(Cid, Vec<u8>)>, Error> {
        read_node(&mut self.reader, &mut self.buffer)
    }
}

impl<R> Iterator for CarReader<R>
where
    R: Read + Send + Unpin,
{
    type Item = Result<(Cid, Vec<u8>), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        read_node(&mut self.reader, &mut self.buffer).transpose()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use cid::Cid;
    use ipld_core::{codec::Codec, ipld::Ipld};
    use multihash_codetable::{Code, MultihashDigest};
    use serde_ipld_dagcbor::codec::DagCborCodec;

    use crate::sync::*;

    #[test]
    fn car_write_read() {
        let digest_test = Code::Sha2_256.digest(b"test");
        let cid_test = Cid::new_v1(<DagCborCodec as Codec<Ipld>>::CODE, digest_test);

        let digest_foo = Code::Sha2_256.digest(b"foo");
        let cid_foo = Cid::new_v1(<DagCborCodec as Codec<Ipld>>::CODE, digest_foo);

        let header = CarHeader::V1(CarHeaderV1::from(vec![cid_foo]));

        let mut buffer = Vec::new();
        let mut writer = CarWriter::new(header, &mut buffer);
        writer.write(cid_test, b"test").unwrap();
        writer.write(cid_foo, b"foo").unwrap();
        writer.finish().unwrap();

        let reader = Cursor::new(&buffer);
        let car_reader = CarReader::new(reader).unwrap();
        let files: Vec<_> = car_reader
            .into_iter()
            .collect::<Result<_, Error>>()
            .unwrap();

        assert_eq!(files.len(), 2);
        assert_eq!(files[0].0, cid_test);
        assert_eq!(files[0].1, b"test");
        assert_eq!(files[1].0, cid_foo);
        assert_eq!(files[1].1, b"foo");
    }
}
