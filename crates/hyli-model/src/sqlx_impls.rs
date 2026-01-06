use crate::utils::TimestampMs;
use crate::{ConsensusProposalHash, LaneId, TxHash, TxId};
use crate::{ContractName, DataProposalHash};
use sqlx::Row;

impl sqlx::Type<sqlx::Postgres> for TimestampMs {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        sqlx::postgres::PgTypeInfo::with_name("TIMESTAMP")
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for TimestampMs {
    fn decode(
        value: sqlx::postgres::PgValueRef<'r>,
    ) -> std::result::Result<
        TimestampMs,
        std::boxed::Box<dyn std::error::Error + std::marker::Send + std::marker::Sync + 'static>,
    > {
        use sqlx::types::chrono::NaiveDateTime;

        let timestamp = <NaiveDateTime as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        let millis = timestamp.and_utc().timestamp_millis() as u128;
        Ok(TimestampMs(millis))
    }
}

impl sqlx::Type<sqlx::Postgres> for ContractName {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <String as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}
impl sqlx::Encode<'_, sqlx::Postgres> for ContractName {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> std::result::Result<
        sqlx::encode::IsNull,
        std::boxed::Box<dyn std::error::Error + std::marker::Send + std::marker::Sync + 'static>,
    > {
        <String as sqlx::Encode<sqlx::Postgres>>::encode_by_ref(&self.0, buf)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for ContractName {
    fn decode(
        value: sqlx::postgres::PgValueRef<'r>,
    ) -> std::result::Result<
        ContractName,
        std::boxed::Box<dyn std::error::Error + std::marker::Send + std::marker::Sync + 'static>,
    > {
        let inner = <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        Ok(ContractName(inner))
    }
}

impl sqlx::Type<sqlx::Postgres> for TxHash {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <String as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}
impl sqlx::Encode<'_, sqlx::Postgres> for TxHash {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> std::result::Result<
        sqlx::encode::IsNull,
        std::boxed::Box<dyn std::error::Error + std::marker::Send + std::marker::Sync + 'static>,
    > {
        <String as sqlx::Encode<sqlx::Postgres>>::encode_by_ref(&self.0, buf)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for TxHash {
    fn decode(
        value: sqlx::postgres::PgValueRef<'r>,
    ) -> std::result::Result<
        TxHash,
        std::boxed::Box<dyn std::error::Error + std::marker::Send + std::marker::Sync + 'static>,
    > {
        let inner = <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        Ok(TxHash(inner))
    }
}

impl sqlx::Type<sqlx::Postgres> for LaneId {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <String as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}
impl sqlx::Encode<'_, sqlx::Postgres> for LaneId {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> std::result::Result<
        sqlx::encode::IsNull,
        std::boxed::Box<dyn std::error::Error + std::marker::Send + std::marker::Sync + 'static>,
    > {
        <String as sqlx::Encode<sqlx::Postgres>>::encode_by_ref(&self.to_string(), buf)
    }
}
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for LaneId {
    fn decode(
        value: sqlx::postgres::PgValueRef<'r>,
    ) -> std::result::Result<
        LaneId,
        std::boxed::Box<dyn std::error::Error + std::marker::Send + std::marker::Sync + 'static>,
    > {
        let inner = <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        LaneId::parse(&inner)
            .map_err(std::boxed::Box::<dyn std::error::Error + Send + Sync>::from)
    }
}

impl sqlx::Type<sqlx::Postgres> for ConsensusProposalHash {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <String as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}

impl sqlx::Encode<'_, sqlx::Postgres> for ConsensusProposalHash {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> std::result::Result<
        sqlx::encode::IsNull,
        std::boxed::Box<dyn std::error::Error + std::marker::Send + std::marker::Sync + 'static>,
    > {
        <String as sqlx::Encode<sqlx::Postgres>>::encode_by_ref(&self.0, buf)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for ConsensusProposalHash {
    fn decode(
        value: sqlx::postgres::PgValueRef<'r>,
    ) -> std::result::Result<
        ConsensusProposalHash,
        std::boxed::Box<dyn std::error::Error + std::marker::Send + std::marker::Sync + 'static>,
    > {
        let inner = <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        Ok(ConsensusProposalHash(inner))
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for TxId {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        let tx_hash: TxHash = row.try_get("tx_hash")?;
        let dp_hash: DataProposalHash = row.try_get("parent_dp_hash")?;
        Ok(TxId(dp_hash, tx_hash))
    }
}

impl sqlx::Type<sqlx::Postgres> for DataProposalHash {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <String as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}
impl sqlx::Encode<'_, sqlx::Postgres> for DataProposalHash {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> std::result::Result<
        sqlx::encode::IsNull,
        std::boxed::Box<dyn std::error::Error + std::marker::Send + std::marker::Sync + 'static>,
    > {
        <String as sqlx::Encode<sqlx::Postgres>>::encode_by_ref(&self.0, buf)
    }
}
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for DataProposalHash {
    fn decode(
        value: sqlx::postgres::PgValueRef<'r>,
    ) -> std::result::Result<
        DataProposalHash,
        std::boxed::Box<dyn std::error::Error + std::marker::Send + std::marker::Sync + 'static>,
    > {
        let inner = <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        Ok(DataProposalHash(inner))
    }
}
