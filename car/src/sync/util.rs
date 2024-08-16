use std::io::Read;

use cid::Cid;
use integer_encoding::VarIntReader;

use crate::Error;

/// Maximum size that is used for single node.
pub(crate) const MAX_ALLOC: usize = 4 * 1024 * 1024;

pub(crate) fn ld_read<R>(mut reader: R, buf: &mut Vec<u8>) -> Result<Option<&[u8]>, Error>
where
    R: Read + Send + Unpin,
{
    let length: usize = match VarIntReader::read_varint(&mut reader) {
        Ok(len) => len,
        Err(e) => {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(None);
            }
            return Err(Error::Parsing(e.to_string()));
        }
    };

    if length > MAX_ALLOC {
        return Err(Error::LdReadTooLarge(length));
    }
    if length > buf.len() {
        buf.resize(length, 0);
    }

    reader
        .read_exact(&mut buf[..length])
        .map_err(|e| Error::Parsing(e.to_string()))?;

    Ok(Some(&buf[..length]))
}

pub(crate) fn read_node<R>(
    buf_reader: &mut R,
    buf: &mut Vec<u8>,
) -> Result<Option<(Cid, Vec<u8>)>, Error>
where
    R: Read + Send + Unpin,
{
    if let Some(buf) = ld_read(buf_reader, buf)? {
        let mut cursor = std::io::Cursor::new(buf);
        let c = Cid::read_bytes(&mut cursor)?;
        let pos = cursor.position() as usize;

        return Ok(Some((c, buf[pos..].to_vec())));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use integer_encoding::VarIntWriter;

    use super::*;

    fn ld_write<'a, W>(writer: &mut W, bytes: &[u8]) -> Result<(), Error>
    where
        W: Write + Send + Unpin,
    {
        writer.write_varint(bytes.len())?;
        writer.write_all(bytes)?;
        writer.flush()?;
        Ok(())
    }

    #[test]
    fn ld_read_write_good() {
        let mut buffer = Vec::<u8>::new();
        ld_write(&mut buffer, b"test bytes").unwrap();
        let reader = std::io::Cursor::new(buffer);

        let mut buffer = vec![1u8; 1024];
        let read = ld_read(reader, &mut buffer).unwrap().unwrap();
        assert_eq!(read, b"test bytes");
    }

    #[test]
    fn ld_read_write_fail() {
        let mut buffer = Vec::<u8>::new();
        let size = MAX_ALLOC + 1;
        ld_write(&mut buffer, &vec![2u8; size]).unwrap();
        let reader = std::io::Cursor::new(buffer);

        let mut buffer = vec![1u8; 1024];
        let read = ld_read(reader, &mut buffer);
        assert!(matches!(read, Err(Error::LdReadTooLarge(_))));
    }
}
