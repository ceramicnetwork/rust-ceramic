use std::borrow::Cow;

use cid::Cid;
use sqlx::{
    encode::IsNull,
    error::BoxDynError,
    sqlite::{SqliteArgumentValue, SqliteRow, SqliteTypeInfo, SqliteValueRef},
    Decode, Encode, FromRow, Row, Sqlite, Type,
};

#[derive(Debug)]
pub struct CidBlob(pub(crate) Cid);

impl Type<Sqlite> for CidBlob {
    fn type_info() -> SqliteTypeInfo {
        <&[u8] as Type<Sqlite>>::type_info()
    }

    fn compatible(ty: &SqliteTypeInfo) -> bool {
        <&[u8] as Type<Sqlite>>::compatible(ty)
    }
}

impl<'q> Encode<'q, Sqlite> for CidBlob {
    fn encode(self, args: &mut Vec<SqliteArgumentValue<'q>>) -> IsNull {
        args.push(SqliteArgumentValue::Blob(Cow::Owned(self.0.to_bytes())));

        IsNull::No
    }

    fn encode_by_ref(&self, args: &mut Vec<SqliteArgumentValue<'q>>) -> IsNull {
        args.push(SqliteArgumentValue::Blob(Cow::Owned(self.0.to_bytes())));

        IsNull::No
    }
}

impl<'r> Decode<'r, Sqlite> for CidBlob {
    fn decode(value: SqliteValueRef<'r>) -> Result<Self, BoxDynError> {
        let v: &[u8] = <&[u8] as Decode<Sqlite>>::decode(value)?;
        let cid = Cid::try_from(v)?;
        Ok(CidBlob(cid))
    }
}

impl FromRow<'_, SqliteRow> for CidBlob {
    fn from_row(row: &SqliteRow) -> std::result::Result<CidBlob, sqlx::Error> {
        let v: Vec<u8> = row.get(0);
        let cid = Cid::try_from(v.as_slice()).map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
        Ok(CidBlob(cid))
    }
}
