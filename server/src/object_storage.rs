#![allow(dead_code)]

extern crate rmp_serde as rmps;

use std::{
    cmp::Ordering,
    collections::{
        HashMap,
        HashSet,
    },
    error::Error as StdError,
    fmt::Display,
    mem,
    path::Path,
    str::FromStr,
};

use refinery::Report;
use rmps::Serializer;
use rusqlite::{
    blob::ZeroBlob,
    named_params,
    params,
    types::{
        ToSqlOutput,
        Value,
    },
    Connection,
    DatabaseName,
    DropBehavior,
    OpenFlags,
    ToSql,
    Transaction,
};
use serde::{
    de::DeserializeOwned,
    Serialize,
};

mod embedded
{
    refinery::embed_migrations!("./sql_migrations");
}

#[derive(Debug)]
pub enum Error
{
    StorageNotFound
    {
        path: String,
    },
    StorageAlreadyExists
    {
        path: String,
    },
    ObjectNotFound
    {
        object_id: ObjectId,
    },
    ObjectKindNotFound
    {
        kind_code: String,
    },
    ObjectFieldNotExists
    {
        kind_code: String,
        field:     String,
    },
    ObjectFieldValueInvalidType
    {
        kind_code:     String,
        field:         String,
        expected_type: String,
        actual_type:   String,
    },
    ObjectIdParsingError
    {
        object_id_raw: String,
    },
    CyclicStrictLinkDetected
    {
        parent_id: ObjectId,
        child_id:  ObjectId,
    },
    LinkNotFound
    {
        parent_id: ObjectId,
        child_id:  ObjectId,
    },
    AlreadyLinked
    {
        parent_id: ObjectId,
        child_id:  ObjectId,
    },
    Other(Box<dyn StdError>),
}

impl PartialEq for Error
{
    fn eq(&self, other: &Self) -> bool
    {
        match (self, other) {
            (
                Self::StorageNotFound { path: l_path },
                Self::StorageNotFound { path: r_path },
            ) => l_path == r_path,
            (
                Self::ObjectNotFound {
                    object_id: l_object_id,
                },
                Self::ObjectNotFound {
                    object_id: r_object_id,
                },
            ) => l_object_id == r_object_id,
            (
                Self::ObjectKindNotFound {
                    kind_code: l_kind_code,
                },
                Self::ObjectKindNotFound {
                    kind_code: r_kind_code,
                },
            ) => l_kind_code == r_kind_code,
            (
                Self::ObjectFieldNotExists {
                    kind_code: l_kind_code,
                    field: l_field,
                },
                Self::ObjectFieldNotExists {
                    kind_code: r_kind_code,
                    field: r_field,
                },
            ) => l_kind_code == r_kind_code && l_field == r_field,
            (
                Self::ObjectFieldValueInvalidType {
                    kind_code: l_kind_code,
                    field: l_field,
                    expected_type: l_expected_type,
                    actual_type: l_actual_type,
                },
                Self::ObjectFieldValueInvalidType {
                    kind_code: r_kind_code,
                    field: r_field,
                    expected_type: r_expected_type,
                    actual_type: r_actual_type,
                },
            ) => {
                l_kind_code == r_kind_code
                    && l_field == r_field
                    && l_expected_type == r_expected_type
                    && l_actual_type == r_actual_type
            }
            (
                Self::ObjectIdParsingError {
                    object_id_raw: l_object_id_raw,
                },
                Self::ObjectIdParsingError {
                    object_id_raw: r_object_id_raw,
                },
            ) => l_object_id_raw == r_object_id_raw,
            (
                Self::CyclicStrictLinkDetected {
                    parent_id: l_parent_id,
                    child_id: l_child_id,
                },
                Self::CyclicStrictLinkDetected {
                    parent_id: r_parent_id,
                    child_id: r_child_id,
                },
            ) => l_parent_id == r_parent_id && l_child_id == r_child_id,
            (
                Self::LinkNotFound {
                    parent_id: l_parent_id,
                    child_id: l_child_id,
                },
                Self::LinkNotFound {
                    parent_id: r_parent_id,
                    child_id: r_child_id,
                },
            ) => l_parent_id == r_parent_id && l_child_id == r_child_id,
            (
                Self::AlreadyLinked {
                    parent_id: l_parent_id,
                    child_id: l_child_id,
                },
                Self::AlreadyLinked {
                    parent_id: r_parent_id,
                    child_id: r_child_id,
                },
            ) => l_parent_id == r_parent_id && l_child_id == r_child_id,
            (
                Self::StorageAlreadyExists { path: l_path },
                Self::StorageAlreadyExists { path: r_path },
            ) => l_path == r_path,
            (Self::Other(_), Self::Other(_)) => true,
            (_, _) => false,
        }
    }
}

impl Display for Error
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        match self {
            Error::StorageNotFound { path } => write!(f, "Storage not found by \"{}\".", path),
            Error::ObjectNotFound { object_id } => write!(f, "Object #{} not found in storage.", object_id),
            Error::ObjectKindNotFound { kind_code } => write!(f, "Object kind \"{}\" not found in storage.", kind_code),
            Error::AlreadyLinked { parent_id, child_id } => write!(f, "Link between #{} and #{} already exists.", parent_id, child_id),
            Error::ObjectFieldValueInvalidType { kind_code, field, expected_type, actual_type } =>
                write!(f, "Invalid value type for field \"{}\" of object kind \"{}\". Expected \"{}\" actual \"{}\"", field, kind_code, expected_type, actual_type),
            Error::ObjectFieldNotExists { kind_code, field } => write!(f, "Field \"{}\" not exists in object kind \"{}\".", field, kind_code),
            Error::ObjectIdParsingError { object_id_raw } => write!(f, "Error parsing ObjectId: {}", object_id_raw),
            Error::Other(error) => write!(f, "{}", error),
            Error::CyclicStrictLinkDetected { parent_id, child_id } => write!(f, "Cyclic strict link detected between {} and {}.", parent_id, child_id),
            Error::LinkNotFound { parent_id, child_id } => write!(f, "Link between {} and {} not found.", parent_id, child_id),
            Error::StorageAlreadyExists { path } => write!(f, "Storage file \"{}\" already exists.", path),
        }
    }
}

impl StdError for Error
{
    fn source(&self) -> Option<&(dyn StdError + 'static)>
    {
        None
    }
}

impl From<rusqlite::Error> for Error
{
    fn from(e: rusqlite::Error) -> Self
    {
        Error::Other(Box::new(e))
    }
}

impl From<refinery::Error> for Error
{
    fn from(e: refinery::Error) -> Self
    {
        Error::Other(Box::new(e))
    }
}

impl From<rmps::encode::Error> for Error
{
    fn from(e: rmps::encode::Error) -> Self
    {
        Error::Other(Box::new(e))
    }
}

impl From<rmps::decode::Error> for Error
{
    fn from(e: rmps::decode::Error) -> Self
    {
        Error::Other(Box::new(e))
    }
}

pub struct CreateOptions
{
    pub name:       String,
    pub custom_sql: Option<String>,
}

impl Default for CreateOptions
{
    fn default() -> Self
    {
        Self {
            name:       Default::default(),
            custom_sql: None,
        }
    }
}

pub struct Storage
{
    path: String,
}

impl Storage
{
    pub fn new(path: &str) -> Result<Self, Error>
    {
        if !path.eq(":memory:") && !Path::new(path).exists() {
            return Err(Error::StorageNotFound {
                path: path.to_owned(),
            });
        }
        Ok(Self {
            path: path.to_owned(),
        })
    }

    pub fn create(path: &str, options: CreateOptions) -> Result<Self, Error>
    {
        if Path::new(path).exists() {
            return Err(Error::StorageAlreadyExists {
                path: path.to_owned(),
            });
        }

        let open_flags =
            OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE;
        let mut connection = Connection::open_with_flags(path, open_flags)?;

        connection.execute_batch("PRAGMA journal_mode=WAL;")?;

        Self::apply_migrations_impl(&mut connection)?;

        if let Some(sql) = options.custom_sql {
            connection.execute_batch(sql.as_str())?;
        }

        connection.execute(
            "INSERT INTO properties(key, value) VALUES(?, ?)",
            ["name", options.name.as_str()],
        )?;
        drop(connection);

        Self::new(path)
    }

    /// Return schema migration status for this storage.
    ///
    /// * Ordering::Less - Schema need to migrate to last version
    /// * Ordering::Equal - Schema is up to date
    /// * Ordering::Greater - Storage schema migrated to version greater than available in current server version. \
    ///   This may be the reason for generating a version mismatch error
    pub fn schema_status(&self) -> Result<Ordering, Error>
    {
        let mut conn = self.start_session()?.connection;

        if conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'refinery_schema_version')", 
            [], |r| r.get::<usize, i64>(0))? == 0
        {
            return Ok( Ordering::Less )
        }

        let runner = embedded::migrations::runner();
        let max_apply = runner.get_last_applied_migration(&mut conn)?;
        if max_apply.is_none() {
            return Ok(Ordering::Less);
        }
        let max_apply = max_apply.unwrap();
        let max = runner.get_migrations().iter().max().unwrap();

        Ok(max_apply.cmp(max))
    }

    pub fn apply_migrations(&self) -> Result<Report, Error>
    {
        Self::apply_migrations_impl(&mut self.start_session()?.connection)
    }

    fn apply_migrations_impl(conn: &mut Connection) -> Result<Report, Error>
    {
        Ok(embedded::migrations::runner().run(conn)?)
    }

    pub fn get_property(&self, prop: &str) -> Result<Option<Value>, Error>
    {
        match self.start_session()?.connection.query_row(
            "SELECT value FROM properties WHERE key = ?1",
            params![prop],
            |r| r.get::<usize, Value>(0),
        ) {
            Ok(value) => Ok(Some(value)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn set_property(&self, prop: &str, value: Value) -> Result<(), Error>
    {
        self.start_session()?.connection.execute(
            "INSERT INTO properties(key, value) VALUES(?1, ?2) ON CONFLICT(key) DO UPDATE SET value = ?2;", params![prop, value])?;
        Ok(())
    }

    pub fn start_session(&self) -> Result<Session, Error>
    {
        let open_flags =
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let connection =
            Connection::open_with_flags(self.path.clone(), open_flags)?;

        Ok(Session::new(connection))
    }
}

#[derive(Debug)]
pub struct ObjectId
{
    pub kind: String,
    pub id:   i64,
}

unsafe impl Send for ObjectId {}
unsafe impl Sync for ObjectId {}

impl Display for ObjectId
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        write!(f, "{}", self.to_string())
    }
}

impl ObjectId
{
    pub fn new(kind: String, id: i64) -> Self
    {
        Self { kind, id }
    }

    pub fn to_string(&self) -> String
    {
        format!("{}#{}", self.kind, self.id)
    }
}

impl Into<String> for ObjectId
{
    fn into(self) -> String
    {
        self.to_string()
    }
}

impl Clone for ObjectId
{
    fn clone(&self) -> Self
    {
        Self {
            kind: self.kind.clone(),
            id:   self.id.clone(),
        }
    }
}

impl PartialEq for ObjectId
{
    fn eq(&self, other: &Self) -> bool
    {
        self.kind == other.kind && self.id == other.id
    }
}

impl ToSql for ObjectId
{
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>>
    {
        Ok(ToSqlOutput::Owned(Value::Text(self.to_string())))
    }
}

impl FromStr for ObjectId
{
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err>
    {
        if let Some(pos) = s.find('#') {
            if let Ok(id) = s[(pos + 1)..].parse::<i64>() {
                return Ok(Self::new(s[..pos].to_owned(), id));
            }
        }
        Err(Error::ObjectIdParsingError {
            object_id_raw: s.to_owned(),
        })
    }
}

#[derive(Debug)]
pub enum LinkType
{
    Free,
    Strict,
}

impl LinkType
{
    pub fn to_integer(&self) -> i64
    {
        match self {
            LinkType::Free => 0,
            LinkType::Strict => 1,
        }
    }
}

impl Into<i64> for LinkType
{
    fn into(self) -> i64
    {
        self.to_integer()
    }
}

impl From<i64> for LinkType
{
    fn from(link_type: i64) -> Self
    {
        match link_type {
            0 => LinkType::Free,
            1 => LinkType::Strict,
            _ => panic!(),
        }
    }
}

impl PartialEq for LinkType
{
    fn eq(&self, other: &Self) -> bool
    {
        core::mem::discriminant(self) == core::mem::discriminant(other)
    }
}

impl ToSql for LinkType
{
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>>
    {
        Ok(ToSqlOutput::Owned(Value::Integer(self.to_integer())))
    }
}

#[derive(Debug, Clone)]
pub enum FieldValue
{
    Text(String),
    Integer(i64),
    Real(f64),
    Blob(Vec<u8>),
}

impl PartialEq for FieldValue
{
    fn eq(&self, other: &Self) -> bool
    {
        match (self, other) {
            (Self::Text(l0), Self::Text(r0)) => l0 == r0,
            (Self::Integer(l0), Self::Integer(r0)) => l0 == r0,
            (Self::Real(l0), Self::Real(r0)) => l0 == r0,
            (Self::Blob(l0), Self::Blob(r0)) => l0 == r0,
            (_, _) => false,
        }
    }
}

impl From<i64> for FieldValue
{
    fn from(i: i64) -> Self
    {
        Self::Integer(i)
    }
}

impl From<f64> for FieldValue
{
    fn from(i: f64) -> Self
    {
        Self::Real(i)
    }
}

impl From<String> for FieldValue
{
    fn from(s: String) -> Self
    {
        Self::Text(s)
    }
}

impl From<&'static str> for FieldValue
{
    fn from(s: &'static str) -> Self
    {
        Self::Text(s.to_owned())
    }
}

impl From<bool> for FieldValue
{
    fn from(b: bool) -> Self
    {
        Self::Integer(b.into())
    }
}

impl FieldValue
{
    pub fn as_sql_value(&self) -> Value
    {
        match self {
            FieldValue::Integer(i) => Value::Integer(i.clone()),
            FieldValue::Real(f) => Value::Real(f.clone()),
            FieldValue::Text(s) => Value::Text(s.clone()),
            FieldValue::Blob(b) => Value::Blob(b.clone()),
        }
    }
}

impl ToSql for FieldValue
{
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>>
    {
        Ok(ToSqlOutput::Owned(self.as_sql_value()))
    }
}

#[derive(Clone)]
pub struct Fields
{
    seq: Vec<(String, FieldValue)>,
}

impl Fields
{
    pub fn new() -> Self
    {
        Self { seq: Vec::new() }
    }

    pub fn add(&mut self, name: &str, value: FieldValue) -> &mut Self
    {
        self.seq.push((name.to_owned(), value));
        self
    }

    pub fn names(&self) -> Vec<&String>
    {
        self.seq.iter().map(|(name, _)| name).collect()
    }

    pub fn values(&self) -> Vec<&FieldValue>
    {
        self.seq.iter().map(|(_, value)| value).collect()
    }

    pub fn into_entries(self) -> Vec<(String, FieldValue)>
    {
        self.seq
    }
}

pub struct Session
{
    connection: Connection,
}

impl Session
{
    pub fn new(conn: Connection) -> Self
    {
        Self { connection: conn }
    }

    pub fn touch_object(&mut self, kind: &str) -> Result<ObjectId, Error>
    {
        let tx = self.transaction()?;
        let id = tx.touch_object(kind)?;
        tx.commit()?;

        Ok(id)
    }

    pub fn object_set_fields(
        &mut self,
        id: &ObjectId,
        fields: Fields,
    ) -> Result<(), Error>
    {
        let tx = self.transaction()?;
        tx.object_set_fields(id, fields)?;
        tx.commit()?;

        Ok(())
    }

    pub fn object_exists(&mut self, id: &ObjectId) -> Result<bool, Error>
    {
        let tx = self.transaction()?;
        let exists = tx.object_exists(id)?;
        tx.commit()?;

        Ok(exists)
    }

    pub fn drop_objects(
        &mut self,
        ids: Vec<&ObjectId>,
    ) -> Result<Vec<ObjectId>, Error>
    {
        let tx = self.transaction()?;
        let deleted_ids = tx.drop_objects(ids)?;
        tx.commit()?;

        Ok(deleted_ids)
    }

    pub fn link_objects(
        &mut self,
        parent_id: &ObjectId,
        child_id: &ObjectId,
        link_type: LinkType,
    ) -> Result<i64, Error>
    {
        let tx = self.transaction()?;
        let link_id = tx.link_objects(parent_id, child_id, link_type)?;
        tx.commit()?;

        Ok(link_id)
    }

    pub fn get_link_by_id(
        &mut self,
        link_id: i64,
    ) -> Result<Option<Link>, Error>
    {
        let tx = self.transaction()?;
        let link = tx.get_link_by_id(link_id)?;
        tx.commit()?;

        Ok(link)
    }

    pub fn unlink_objects(
        &mut self,
        id0: &ObjectId,
        id1: &ObjectId,
    ) -> Result<(), Error>
    {
        let tx = self.transaction()?;
        tx.unlink_objects(id0, id1)?;
        tx.commit()?;

        Ok(())
    }

    pub fn link_objects_with_meta<M: Serialize + DeserializeOwned>(
        &mut self,
        parent_id: &ObjectId,
        child_id: &ObjectId,
        link_type: LinkType,
        meta: M,
    ) -> Result<i64, Error>
    {
        let tx = self.transaction()?;
        let link_id =
            tx.link_objects_with_meta(parent_id, child_id, link_type, meta)?;
        tx.commit()?;

        Ok(link_id)
    }

    pub fn set_link_meta<M: Serialize + DeserializeOwned>(
        &mut self,
        parent_id: &ObjectId,
        child_id: &ObjectId,
        meta: M,
    ) -> Result<(), Error>
    {
        let tx = self.transaction()?;
        tx.set_link_meta(parent_id, child_id, meta)?;
        tx.commit()?;

        Ok(())
    }

    pub fn set_raw_link_meta(
        &mut self,
        parent_id: &ObjectId,
        child_id: &ObjectId,
        meta: Vec<u8>,
    ) -> Result<(), Error>
    {
        let tx = self.transaction()?;
        tx.set_raw_link_meta(parent_id, child_id, meta)?;
        tx.commit()?;

        Ok(())
    }

    pub fn get_link_meta<M: Serialize + DeserializeOwned>(
        &mut self,
        parent_id: &ObjectId,
        child_id: &ObjectId,
    ) -> Result<Option<M>, Error>
    {
        let tx = self.transaction()?;
        let meta = tx.get_link_meta(parent_id, child_id)?;
        tx.commit()?;

        Ok(meta)
    }

    pub fn object_is_child_of(
        &mut self,
        parent_id: &ObjectId,
        child_id: &ObjectId,
    ) -> Result<bool, Error>
    {
        let tx = self.transaction()?;
        let is_child = tx.object_is_child_of(parent_id, child_id)?;
        tx.commit()?;

        Ok(is_child)
    }

    pub fn find_objects(
        &mut self,
        kind: &str,
        cond: Cond,
        params: &[(&str, &dyn ToSql)],
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<Object>, Error>
    {
        let tx = self.transaction()?;
        let objects = tx.find_objects(kind, cond, params, limit, offset)?;
        tx.commit()?;

        Ok(objects)
    }

    pub fn find_all_objects(&mut self, kind: &str)
        -> Result<Vec<Object>, Error>
    {
        let tx = self.transaction()?;
        let objects = tx.find_all_objects(kind)?;
        tx.commit()?;

        Ok(objects)
    }

    pub fn find_unique_object(
        &mut self,
        kind: &str,
        cond: Cond,
        params: &[(&str, &dyn ToSql)],
    ) -> Result<Option<Object>, Error>
    {
        let tx = self.transaction()?;
        let object = tx.find_unique_object(kind, cond, params)?;
        tx.commit()?;

        Ok(object)
    }

    pub fn find_object_by_id(
        &mut self,
        id: &ObjectId,
    ) -> Result<Option<Object>, Error>
    {
        let tx = self.transaction()?;
        let object = tx.find_object_by_id(id)?;
        tx.commit()?;

        Ok(object)
    }

    pub fn find_child_objects(
        &mut self,
        child_of: &ObjectId,
        kind: &str,
        cond: Cond,
        params: &[(&str, &dyn ToSql)],
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<(Object, Link)>, Error>
    {
        let tx = self.transaction()?;
        let objects =
            tx.find_child_objects(child_of, kind, cond, params, limit, offset)?;
        tx.commit()?;

        Ok(objects)
    }

    pub fn find_all_child_objects(
        &mut self,
        child_of: &ObjectId,
        kind: &str,
    ) -> Result<Vec<(Object, Link)>, Error>
    {
        let tx = self.transaction()?;
        let objects = tx.find_all_child_objects(child_of, kind)?;
        tx.commit()?;

        Ok(objects)
    }

    pub fn find_unique_child_object(
        &mut self,
        child_of: &ObjectId,
        kind: &str,
        cond: Cond,
        params: &[(&str, &dyn ToSql)],
    ) -> Result<Option<(Object, Link)>, Error>
    {
        let tx = self.transaction()?;
        let object =
            tx.find_unique_child_object(child_of, kind, cond, params)?;
        tx.commit()?;

        Ok(object)
    }

    pub fn transaction(&mut self) -> Result<SessionTransaction, Error>
    {
        let mut tx = self.connection.transaction()?;
        tx.set_drop_behavior(DropBehavior::Rollback);
        Ok(SessionTransaction::new(tx))
    }
}

pub struct SessionTransaction<'conn>
{
    tx: Transaction<'conn>,
}

impl<'conn> SessionTransaction<'conn>
{
    pub fn new(transaction: Transaction<'conn>) -> Self
    {
        SessionTransaction { tx: transaction }
    }

    pub fn commit(self) -> Result<(), Error>
    {
        self.tx.commit()?;
        Ok(())
    }
}

impl<'conn> SessionTransaction<'conn>
{
    pub fn touch_object(&self, kind: &str) -> Result<ObjectId, Error>
    {
        if !self.object_kind_exists(kind)? {
            return Err(Error::ObjectKindNotFound {
                kind_code: kind.to_owned(),
            });
        }
        self.touch_object_unchecked(kind)
    }

    pub fn object_set_fields(
        &self,
        id: &ObjectId,
        fields: Fields,
    ) -> Result<(), Error>
    {
        self.object_set_fields_unchecked(id, fields)
    }

    fn object_set_fields_unchecked(
        &self,
        id: &ObjectId,
        fields: Fields,
    ) -> Result<(), Error>
    {
        let fields_sql = fields
            .names()
            .iter()
            .enumerate()
            .map(|(indx, name)| format!("{} = ?{}", name, indx + 1))
            .collect::<Vec<String>>()
            .join(", ");
        let sql = format!(
            "UPDATE obj_{kind} SET {fields} WHERE id = {id};",
            kind = id.kind,
            id = id.id,
            fields = fields_sql
        );

        let mut stmt = self.tx.prepare(sql.as_str())?;
        for (indx, value) in fields.values().iter().enumerate() {
            stmt.raw_bind_parameter(indx + 1, value)?;
        }
        stmt.raw_execute()?;

        Ok(())
    }

    fn touch_object_unchecked(&self, kind: &str) -> Result<ObjectId, Error>
    {
        self.tx.execute_batch(
            format!("INSERT INTO obj_{kind}(id) VALUES(NULL);", kind = kind)
                .as_str(),
        )?;
        Ok(ObjectId::new(kind.to_owned(), self.tx.last_insert_rowid()))
    }

    fn object_kind_exists(&self, kind: &str) -> Result<bool, Error>
    {
        Ok(self.tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1);",
            params![format!("obj_{}", kind)],
            |r| r.get::<usize, i64>(0),
        )? > 0)
    }

    pub fn object_exists(&self, id: &ObjectId) -> Result<bool, Error>
    {
        if self.object_kind_exists(id.kind.as_str())? {
            if self.tx.query_row(
                format!(
                    "SELECT EXISTS(SELECT 1 FROM obj_{kind} WHERE id = ?1)",
                    kind = id.kind
                )
                .as_str(),
                params![id.id],
                |r| r.get::<usize, i64>(0),
            )? > 0
            {
                return Ok(true);
            }
        }

        Ok(false)
    }

    // Drop objects and all strict linked dependencies.
    pub fn drop_objects(
        &self,
        ids: Vec<&ObjectId>,
    ) -> Result<Vec<ObjectId>, Error>
    {
        if ids.len() == 0 {
            return Ok(Vec::new());
        }

        let object_ids = ids
            .iter()
            .map(|id| format!("('{}')", id.to_string()))
            .collect::<Vec<String>>()
            .join(",");

        let sql = format!(
            r#"
            WITH RECURSIVE slice(id) AS (
                VALUES {object_ids} UNION ALL
                SELECT r.id1 FROM relationship as r, slice WHERE r.id0 = slice.id AND r.type = 1
            ) SELECT id FROM slice;
        "#,
            object_ids = object_ids
        );
        drop(object_ids);

        let mut stmt = self.tx.prepare(sql.as_str())?;
        let ids_rows = stmt.query_map([], |r| r.get::<usize, String>(0))?;
        let mut ids_to_delete: Vec<ObjectId> = Vec::new();
        for id in ids_rows {
            let id = id?;
            ids_to_delete.push(id.parse()?);
        }

        let object_ids = ids
            .iter()
            .map(|id| format!("'{}'", id.to_string()))
            .collect::<Vec<String>>()
            .join(",");
        let sql = format!(
            "DELETE FROM relationship WHERE id0 IN({ids}) OR id1 IN ({ids})",
            ids = object_ids
        );

        self.tx.execute_batch(sql.as_str())?;

        let mut native_ids_grouped_by_kind: HashMap<String, HashSet<i64>> =
            HashMap::new();
        for id in ids_to_delete.clone() {
            if !native_ids_grouped_by_kind.contains_key(&id.kind) {
                native_ids_grouped_by_kind
                    .insert(id.kind.clone(), HashSet::new());
            }
            if let Some(set) = native_ids_grouped_by_kind.get_mut(&id.kind) {
                set.insert(id.id);
            }
        }

        let mut sql = String::new();
        for (kind, ids) in native_ids_grouped_by_kind.iter() {
            let ids = ids
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<String>>()
                .join(",");
            sql.push_str(
                format!(
                    "DELETE FROM obj_{kind} WHERE id IN ({ids});\n",
                    kind = kind,
                    ids = ids
                )
                .as_str(),
            );
        }

        self.tx.execute_batch(sql.as_str())?;

        Ok(ids_to_delete)
    }
}

// Relationship
impl<'conn> SessionTransaction<'conn>
{
    pub fn link_objects_with_meta<M: Serialize + DeserializeOwned>(
        &self,
        parent_id: &ObjectId,
        child_id: &ObjectId,
        link_type: LinkType,
        meta: M,
    ) -> Result<i64, Error>
    {
        let link_id = self.link_objects(parent_id, child_id, link_type)?;
        self.set_link_meta_by_link_id_unchecked(link_id, meta)?;

        Ok(link_id)
    }

    pub fn get_link_by_id(&self, link_id: i64) -> Result<Option<Link>, Error>
    {
        let mut stmt = self.tx.prepare(
            "SELECT id0, id1, type, meta FROM relationship WHERE id = ?1",
        )?;
        let mut result = stmt.query(params![link_id])?;
        match result.next()? {
            Some(row) => Ok(Some(Link {
                parent_id: row.get::<usize, String>(0)?.parse::<ObjectId>()?,
                child_id:  row.get::<usize, String>(1)?.parse::<ObjectId>()?,
                link_type: LinkType::from(row.get::<usize, i64>(2)?),
                meta:      row.get::<usize, Option<Vec<u8>>>(3)?,
            })),
            None => Ok(None),
        }
    }

    pub fn object_is_child_of(
        &self,
        parent_id: &ObjectId,
        child_id: &ObjectId,
    ) -> Result<bool, Error>
    {
        Ok(self.tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM relationship WHERE id0 = ?1 AND id1 = ?2);",
            params![parent_id, child_id],
            |r| r.get::<usize, i64>(0),
        )? > 0)
    }

    pub fn set_link_meta<M: Serialize + DeserializeOwned>(
        &self,
        parent_id: &ObjectId,
        child_id: &ObjectId,
        meta: M,
    ) -> Result<(), Error>
    {
        self.set_link_meta_by_link_id_unchecked(
            self.get_link_id_by_members(parent_id, child_id)?,
            meta,
        )
    }

    pub fn set_raw_link_meta(
        &self,
        parent_id: &ObjectId,
        child_id: &ObjectId,
        meta: Vec<u8>,
    ) -> Result<(), Error>
    {
        self.set_raw_link_meta_by_link_unchecked(
            self.get_link_id_by_members(parent_id, child_id)?,
            meta,
        )
    }

    pub fn get_link_meta<M: Serialize + DeserializeOwned>(
        &self,
        parent_id: &ObjectId,
        child_id: &ObjectId,
    ) -> Result<Option<M>, Error>
    {
        self.get_link_meta_by_link_id_unchecked(
            self.get_link_id_by_members(parent_id, child_id)?,
        )
    }

    fn get_link_meta_by_link_id_unchecked<M: Serialize + DeserializeOwned>(
        &self,
        link_id: i64,
    ) -> Result<Option<M>, Error>
    {
        match self.tx.query_row(
            "SELECT meta FROM relationship WHERE id = ?1",
            params![link_id],
            |r| r.get::<usize, Value>(0),
        )? {
            Value::Null => Ok(None),
            Value::Blob(buf) => Ok(rmps::from_slice(&buf)?),
            _ => panic!(),
        }
    }

    fn get_link_id_by_members(
        &self,
        parent_id: &ObjectId,
        child_id: &ObjectId,
    ) -> Result<i64, Error>
    {
        match self.tx.query_row(
            "SELECT id FROM relationship WHERE parent_id = ?1 AND child_id = ?2;",
            params![parent_id, child_id],
            |r| r.get::<usize, i64>(0),
        ) {
            Ok(link_id) => Ok(link_id),
            Err(rusqlite::Error::QueryReturnedNoRows) => Err(Error::LinkNotFound {
                parent_id: parent_id.clone(),
                child_id: child_id.clone(),
            }),
            Err(e) => Err(e.into()),
        }
    }

    fn set_link_meta_by_link_id_unchecked<M: Serialize + DeserializeOwned>(
        &self,
        link_id: i64,
        meta: M,
    ) -> Result<(), Error>
    {
        let mut buf: Vec<u8> = Vec::new();
        meta.serialize(&mut Serializer::new(&mut buf))?;

        self.set_raw_link_meta_by_link_unchecked(link_id, buf)
    }

    pub fn set_raw_link_meta_by_link_unchecked(
        &self,
        link_id: i64,
        meta: Vec<u8>,
    ) -> Result<(), Error>
    {
        self.tx.execute(
            "UPDATE relationship SET meta = ?1 WHERE id = ?2",
            params![ZeroBlob(meta.len() as i32), link_id],
        )?;

        let mut blob = self.tx.blob_open(
            DatabaseName::Main,
            "relationship",
            "meta",
            link_id,
            false,
        )?;
        blob.write_all_at(&meta[..], 0)?;
        blob.close()?;

        Ok(())
    }

    pub fn link_objects(
        &self,
        parent_id: &ObjectId,
        child_id: &ObjectId,
        link_type: LinkType,
    ) -> Result<i64, Error>
    {
        let mut sql = "SELECT  EXISTS(SELECT 1 FROM obj_".to_owned();
        sql.push_str(parent_id.kind.as_str());
        sql.push_str(
            " WHERE id = :native_parent_id LIMIT 1),
                    EXISTS(SELECT 1 FROM obj_",
        );
        sql.push_str(child_id.kind.as_str());
        sql.push_str(" WHERE id = :native_child_id LIMIT 1),
                    EXISTS(SELECT 1 FROM relationship WHERE (id0 = :parent_id AND id1 = :child_id) 
                        OR (id1 = :parent_id AND id0 = :child_id) LIMIT 1),
                    EXISTS(WITH RECURSIVE parents(id) AS (
                        VALUES(:parent_id) UNION ALL
                        SELECT r.id0 FROM relationship as r, parents WHERE r.id1 = parents.id AND r.type = 1
                    ) SELECT 1 FROM parents WHERE id = :child_id LIMIT 1)");
        let mut stmt = self.tx.prepare(sql.as_str())?;
        let mut result = stmt.query(named_params! {
            ":parent_id": parent_id,
            ":child_id": child_id,
            ":native_parent_id": parent_id.id,
            ":native_child_id": child_id.id
        })?;
        let row = result.next()?.unwrap();
        if row.get::<usize, i64>(0)? < 1 {
            return Err(Error::ObjectNotFound {
                object_id: parent_id.clone(),
            });
        } else if row.get::<usize, i64>(1)? < 1 {
            return Err(Error::ObjectNotFound {
                object_id: child_id.clone(),
            });
        } else if row.get::<usize, i64>(2)? >= 1 {
            return Err(Error::AlreadyLinked {
                parent_id: parent_id.clone(),
                child_id:  child_id.clone(),
            });
        } else if row.get::<usize, i64>(3)? >= 1
            && link_type == LinkType::Strict
        {
            return Err(Error::CyclicStrictLinkDetected {
                parent_id: parent_id.clone(),
                child_id:  child_id.clone(),
            });
        }

        self.tx.execute("INSERT INTO relationship(id0, id0_kind, id0_id, id1, id1_kind, id1_id, type) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7);", 
            params![parent_id,
                parent_id.kind,
                parent_id.id,
                child_id,
                child_id.kind,
                child_id.id,
                link_type])?;
        Ok(self.tx.last_insert_rowid())
    }

    pub fn unlink_objects(
        &self,
        id0: &ObjectId,
        id1: &ObjectId,
    ) -> Result<(), Error>
    {
        self.tx.execute(
            "DELETE FROM relationship WHERE (id0 = ?1 AND id1 = ?2) OR (id0 = ?2 AND id1 = ?1)",
            params![id0, id1],
        )?;
        Ok(())
    }
}

pub enum CondEntry
{
    Field(String),
    Value(FieldValue),
    NamedParam(String),
}

impl CondEntry
{
    fn to_string(self) -> String
    {
        match self {
            Self::Field(name) => format!("`{}`", name),
            Self::Value(field_value) => match field_value {
                FieldValue::Text(text) => {
                    format!("'{}'", text.replace("'", "''"))
                }
                FieldValue::Integer(int) => int.to_string(),
                FieldValue::Real(real) => real.to_string(),
                FieldValue::Blob(_) => {
                    panic!("Not allowed blob value in condition.")
                }
            },
            Self::NamedParam(name) => format!(":{}", name),
        }
    }
}

pub enum Op
{
    Eq,
    Neq,
    Gt,
    Gtq,
    Lt,
    Ltq,
}

impl Op
{
    fn to_string(self) -> String
    {
        match self {
            Self::Eq => "=",
            Self::Neq => "<>",
            Self::Gt => ">",
            Self::Gtq => ">=",
            Self::Lt => "<",
            Self::Ltq => "<=",
        }
        .to_owned()
    }
}

pub enum Cond
{
    And(Box<Cond>, Box<Cond>),
    Or(Box<Cond>, Box<Cond>),
    Common(CondEntry, Op, CondEntry),
    In(CondEntry, Vec<CondEntry>, bool),
    Like(CondEntry, String, bool),
    Empty,
}

impl Default for Cond
{
    fn default() -> Self
    {
        Self::Empty
    }
}

impl Cond
{
    fn to_string(self) -> String
    {
        match self {
            Self::And(l, r) => {
                format!("{} AND {}", l.to_string(), r.to_string())
            }
            Self::Or(l, r) => format!("{} OR {}", l.to_string(), r.to_string()),
            Self::Common(l, op, r) => {
                format!(
                    "{} {} {}",
                    l.to_string(),
                    op.to_string(),
                    r.to_string()
                )
            }
            Self::In(l, r, is_not) => format!(
                "{} {}IN ({})",
                l.to_string(),
                if is_not { "NOT " } else { "" },
                r.into_iter()
                    .map(|i| i.to_string())
                    .collect::<Vec<String>>()
                    .join(",")
            ),
            Self::Like(l, pat, is_not) => format!(
                "{} {}LIKE '{}'",
                l.to_string(),
                if is_not { "NOT " } else { "" },
                pat.replace("'", "''")
            ),
            Self::Empty => "1".to_owned(),
        }
    }
}

pub struct CondBuilder
{
    cond: Cond,
}

impl CondBuilder
{
    pub fn new_common(field: &str, op: Op, value: FieldValue) -> Self
    {
        Self {
            cond: Cond::Common(
                CondEntry::Field(field.to_owned()),
                op,
                CondEntry::Value(value),
            ),
        }
    }

    pub fn new_eq(field: &str, value: FieldValue) -> Self
    {
        Self::new_common(field, Op::Eq, value)
    }

    pub fn new_neq(field: &str, value: FieldValue) -> Self
    {
        Self::new_common(field, Op::Neq, value)
    }

    pub fn new_gt(field: &str, value: FieldValue) -> Self
    {
        Self::new_common(field, Op::Gt, value)
    }

    pub fn new_gtq(field: &str, value: FieldValue) -> Self
    {
        Self::new_common(field, Op::Gtq, value)
    }

    pub fn new_lt(field: &str, value: FieldValue) -> Self
    {
        Self::new_common(field, Op::Lt, value)
    }

    pub fn new_ltq(field: &str, value: FieldValue) -> Self
    {
        Self::new_common(field, Op::Ltq, value)
    }

    fn new_in_impl(field: &str, values: Vec<FieldValue>, is_not: bool) -> Self
    {
        Self {
            cond: Cond::In(
                CondEntry::Field(field.to_owned()),
                values.into_iter().map(|v| CondEntry::Value(v)).collect(),
                is_not,
            ),
        }
    }

    pub fn new_in(field: &str, values: Vec<FieldValue>) -> Self
    {
        Self::new_in_impl(field, values, false)
    }

    pub fn new_not_in(field: &str, values: Vec<FieldValue>) -> Self
    {
        Self::new_in_impl(field, values, true)
    }

    fn new_like_impl(field: &str, pattern: &str, is_not: bool) -> Self
    {
        Self {
            cond: Cond::Like(
                CondEntry::Field(field.to_owned()),
                pattern.to_owned(),
                is_not,
            ),
        }
    }

    pub fn new_like(field: &str, pattern: &str) -> Self
    {
        Self::new_like_impl(field, pattern, false)
    }

    pub fn new_not_like(field: &str, pattern: &str) -> Self
    {
        Self::new_like_impl(field, pattern, true)
    }

    pub fn and_common(
        &mut self,
        field: &str,
        op: Op,
        value: FieldValue,
    ) -> &mut Self
    {
        self.wrap_and(Cond::Common(
            CondEntry::Field(field.to_owned()),
            op,
            CondEntry::Value(value),
        ));
        self
    }

    pub fn and_eq(&mut self, field: &str, value: FieldValue) -> &mut Self
    {
        self.and_common(field, Op::Eq, value)
    }

    pub fn and_neq(&mut self, field: &str, value: FieldValue) -> &mut Self
    {
        self.and_common(field, Op::Neq, value)
    }

    pub fn and_gt(&mut self, field: &str, value: FieldValue) -> &mut Self
    {
        self.and_common(field, Op::Gt, value)
    }

    pub fn and_gtq(&mut self, field: &str, value: FieldValue) -> &mut Self
    {
        self.and_common(field, Op::Gtq, value)
    }

    pub fn and_lt(&mut self, field: &str, value: FieldValue) -> &mut Self
    {
        self.and_common(field, Op::Lt, value)
    }

    pub fn and_ltq(&mut self, field: &str, value: FieldValue) -> &mut Self
    {
        self.and_common(field, Op::Ltq, value)
    }

    pub fn or_common(
        &mut self,
        field: &str,
        op: Op,
        value: FieldValue,
    ) -> &mut Self
    {
        self.wrap_or(Cond::Common(
            CondEntry::Field(field.to_owned()),
            op,
            CondEntry::Value(value),
        ));
        self
    }

    pub fn or_eq(&mut self, field: &str, value: FieldValue) -> &mut Self
    {
        self.or_common(field, Op::Eq, value)
    }

    pub fn or_neq(&mut self, field: &str, value: FieldValue) -> &mut Self
    {
        self.or_common(field, Op::Neq, value)
    }

    pub fn or_gt(&mut self, field: &str, value: FieldValue) -> &mut Self
    {
        self.or_common(field, Op::Gt, value)
    }

    pub fn or_gtq(&mut self, field: &str, value: FieldValue) -> &mut Self
    {
        self.or_common(field, Op::Gtq, value)
    }

    pub fn or_lt(&mut self, field: &str, value: FieldValue) -> &mut Self
    {
        self.or_common(field, Op::Lt, value)
    }

    pub fn or_ltq(&mut self, field: &str, value: FieldValue) -> &mut Self
    {
        self.or_common(field, Op::Ltq, value)
    }

    fn and_in_impl(
        &mut self,
        field: &str,
        values: Vec<FieldValue>,
        is_not: bool,
    ) -> &mut Self
    {
        self.wrap_and(Cond::In(
            CondEntry::Field(field.to_owned()),
            values.into_iter().map(|v| CondEntry::Value(v)).collect(),
            is_not,
        ));
        self
    }

    pub fn and_in(&mut self, field: &str, values: Vec<FieldValue>)
        -> &mut Self
    {
        self.and_in_impl(field, values, false)
    }

    pub fn and_not_in(
        &mut self,
        field: &str,
        values: Vec<FieldValue>,
    ) -> &mut Self
    {
        self.and_in_impl(field, values, true)
    }

    fn or_in_impl(
        &mut self,
        field: &str,
        values: Vec<FieldValue>,
        is_not: bool,
    ) -> &mut Self
    {
        self.wrap_or(Cond::In(
            CondEntry::Field(field.to_owned()),
            values.into_iter().map(|v| CondEntry::Value(v)).collect(),
            is_not,
        ));
        self
    }

    pub fn or_in(&mut self, field: &str, values: Vec<FieldValue>) -> &mut Self
    {
        self.or_in_impl(field, values, false)
    }

    pub fn or_not_in(
        &mut self,
        field: &str,
        values: Vec<FieldValue>,
    ) -> &mut Self
    {
        self.or_in_impl(field, values, true)
    }

    fn and_like_impl(
        &mut self,
        field: &str,
        pattern: &str,
        is_not: bool,
    ) -> &mut Self
    {
        self.wrap_and(Cond::Like(
            CondEntry::Field(field.to_owned()),
            pattern.to_owned(),
            is_not,
        ));
        self
    }

    pub fn and_like(&mut self, field: &str, pattern: &str) -> &mut Self
    {
        self.and_like_impl(field, pattern, false)
    }

    pub fn and_not_like(&mut self, field: &str, pattern: &str) -> &mut Self
    {
        self.and_like_impl(field, pattern, true)
    }

    fn or_like_impl(
        &mut self,
        field: &str,
        pattern: &str,
        is_not: bool,
    ) -> &mut Self
    {
        self.wrap_or(Cond::Like(
            CondEntry::Field(field.to_owned()),
            pattern.to_owned(),
            is_not,
        ));
        self
    }

    pub fn or_like(&mut self, field: &str, pattern: &str) -> &mut Self
    {
        self.or_like_impl(field, pattern, false)
    }

    pub fn or_not_like(&mut self, field: &str, pattern: &str) -> &mut Self
    {
        self.or_like_impl(field, pattern, true)
    }

    fn wrap_and(&mut self, cond: Cond)
    {
        self.cond =
            Cond::And(Box::new(cond), Box::new(mem::take(&mut self.cond)))
    }

    fn wrap_or(&mut self, cond: Cond)
    {
        self.cond =
            Cond::Or(Box::new(cond), Box::new(mem::take(&mut self.cond)))
    }

    pub fn cond(self) -> Cond
    {
        self.cond
    }
}

#[derive(Debug)]
pub struct Object
{
    pub id:     ObjectId,
    pub fields: HashMap<String, Option<FieldValue>>,
}

#[derive(PartialEq, Debug)]
pub struct Link
{
    pub parent_id: ObjectId,
    pub child_id:  ObjectId,
    pub link_type: LinkType,
    pub meta:      Option<Vec<u8>>,
}

impl Link
{
    pub fn meta<T: Serialize + DeserializeOwned>(
        &self,
    ) -> Result<Option<T>, Error>
    {
        if let Some(meta) = &self.meta {
            return Ok(Some(rmps::from_slice(&meta[..])?));
        }
        Ok(None)
    }
}

// Finding objects
impl<'conn> SessionTransaction<'conn>
{
    pub fn find_objects(
        &self,
        kind: &str,
        cond: Cond,
        params: &[(&str, &dyn ToSql)],
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<Object>, Error>
    {
        Ok(self
            .find_objects_impl(kind, None, cond, params, limit, offset)?
            .into_iter()
            .map(|(object, _)| object)
            .collect())
    }

    pub fn find_all_objects(&self, kind: &str) -> Result<Vec<Object>, Error>
    {
        self.find_objects(kind, Cond::Empty, &[], None, None)
    }

    pub fn find_unique_object(
        &self,
        kind: &str,
        cond: Cond,
        params: &[(&str, &dyn ToSql)],
    ) -> Result<Option<Object>, Error>
    {
        Ok(self
            .find_objects(kind, cond, params, Some(1), None)?
            .into_iter()
            .nth(0))
    }

    pub fn find_object_by_id(
        &self,
        id: &ObjectId,
    ) -> Result<Option<Object>, Error>
    {
        self.find_unique_object(
            id.kind.as_str(),
            CondBuilder::new_eq("id", id.id.into()).cond(),
            &[],
        )
    }

    pub fn find_all_child_objects(
        &self,
        child_of: &ObjectId,
        kind: &str,
    ) -> Result<Vec<(Object, Link)>, Error>
    {
        self.find_child_objects(child_of, kind, Cond::Empty, &[], None, None)
    }

    pub fn find_child_objects(
        &self,
        child_of: &ObjectId,
        kind: &str,
        cond: Cond,
        params: &[(&str, &dyn ToSql)],
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<(Object, Link)>, Error>
    {
        Ok(self
            .find_objects_impl(
                kind,
                Some(child_of),
                cond,
                params,
                limit,
                offset,
            )?
            .into_iter()
            .map(|(object, link)| (object, link.unwrap()))
            .collect())
    }

    pub fn find_unique_child_object(
        &self,
        child_of: &ObjectId,
        kind: &str,
        cond: Cond,
        params: &[(&str, &dyn ToSql)],
    ) -> Result<Option<(Object, Link)>, Error>
    {
        Ok(self
            .find_child_objects(child_of, kind, cond, params, Some(1), None)?
            .into_iter()
            .nth(0))
    }

    fn find_objects_impl(
        &self,
        kind: &str,
        child_of: Option<&ObjectId>,
        cond: Cond,
        params: &[(&str, &dyn ToSql)],
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<(Object, Option<Link>)>, Error>
    {
        let mut columns_result = self.tx.prepare(
            format!("SELECT name FROM pragma_table_info('obj_{}')", kind)
                .as_str(),
        )?;
        let columns =
            columns_result.query_map([], |r| r.get::<usize, String>(0))?;

        let mut names: Vec<String> = Vec::new();
        for name in columns {
            names.push(name?);
        }

        let mut sql = format!("SELECT target.*");
        if child_of.is_some() {
            sql.push_str(
                ", relationship.id0, relationship.id1, relationship.type, relationship.meta",
            );
        }
        sql.push_str(format!(" FROM obj_{} AS target", kind).as_str());
        if let Some(child_of_into) = child_of.clone() {
            sql.push_str(
                format!(
                    " JOIN relationship ON id0 = '{}' AND id1_kind = '{}' AND id1_id = target.id",
                    child_of_into.to_string(),
                    kind,
                )
                .as_str(),
            );
        }
        sql.push_str(format!(" WHERE {}", cond.to_string()).as_str());
        if let Some(count) = limit {
            sql.push_str(format!(" LIMIT {}", count).as_str());
        }
        if let Some(number) = offset {
            sql.push_str(format!(" OFFSET {}", number).as_str());
        }

        #[cfg(debug_assertions)]
        println!("{}", sql);

        let mut stmt = self.tx.prepare(sql.as_str())?;
        let mut result = stmt.query(params)?;

        let mut objects: Vec<(Object, Option<Link>)> = Vec::new();
        while let Some(row) = result.next()? {
            let mut fields: HashMap<String, Option<FieldValue>> =
                HashMap::new();
            let id = row.get::<usize, i64>(0)?;
            for idx in 0..names.len() {
                let field = match row.get::<usize, Value>(idx)? {
                    Value::Null => None,
                    Value::Integer(i) => Some(FieldValue::Integer(i)),
                    Value::Real(f) => Some(FieldValue::Real(f)),
                    Value::Text(t) => Some(FieldValue::Text(t)),
                    Value::Blob(_) => panic!(),
                };
                fields.insert(names.get(idx).unwrap().clone(), field);
            }

            let object = Object {
                id: ObjectId::new(kind.to_owned(), id),
                fields,
            };
            let link = child_of.clone().map(|_| -> Result<Link, Error> {
                Ok(Link {
                    parent_id: row
                        .get::<usize, String>(names.len())?
                        .parse::<ObjectId>()?,
                    child_id:  row
                        .get::<usize, String>(names.len() + 1)?
                        .parse::<ObjectId>()?,
                    link_type: LinkType::from(
                        row.get::<usize, i64>(names.len() + 2)?,
                    ),
                    meta:      row
                        .get::<usize, Option<Vec<u8>>>(names.len() + 3)?,
                })
            });
            if let Some(result) = link {
                objects.push((object, Some(result?)));
            } else {
                objects.push((object, None));
            }
        }

        Ok(objects)
    }
}

#[cfg(test)]
mod tests
{
    use lazy_static::lazy_static;

    use super::{
        CreateOptions,
        Fields,
        Link,
        Session,
        Storage,
    };
    use crate::object_storage::{
        Cond,
        CondBuilder,
        Error,
        FieldValue,
    };

    extern crate tempdir;

    use random_string::generate;
    use tempdir::TempDir;

    extern crate test;
    use test::Bencher;

    lazy_static! {
        static ref TEMPSTORAGE: TempDir = TempDir::new("test_storage").unwrap();
    }

    fn temp_session() -> Session
    {
        temp_session_with_options(CreateOptions {
            name: "Тестовое хранилище".to_owned(),
            ..Default::default()
        })
    }

    fn temp_session_with_custom_sql(sql: &str) -> Session
    {
        temp_session_with_options(CreateOptions {
            custom_sql: Some(sql.to_owned()),
            ..Default::default()
        })
    }

    fn temp_session_with_options(options: CreateOptions) -> Session
    {
        let path = TEMPSTORAGE
            .path()
            .join(format!("{}.dbsqlite", generate(10, "0123456789abcdef")));
        Storage::create(path.as_os_str().to_str().unwrap(), options)
            .unwrap()
            .start_session()
            .unwrap()
    }

    #[test]
    fn test_creating_storage()
    {
        temp_session();
    }

    #[test]
    fn test_touch_object()
    {
        let mut session = temp_session_with_custom_sql(
            r#" CREATE TABLE obj_test(id INTEGER PRIMARY KEY); "#,
        );
        let object_id = session.touch_object("test").unwrap();

        assert_eq!(session.object_exists(&object_id).unwrap(), true);
    }

    #[test]
    fn test_basic_linking_objects()
    {
        let mut session = temp_session_with_custom_sql(
            r#"
            CREATE TABLE obj_parent(id INTEGER PRIMARY KEY);
            CREATE TABLE obj_child(id INTEGER PRIMARY KEY);
        "#,
        );

        let parent_id = session.touch_object("parent").unwrap();
        let child_id = session.touch_object("child").unwrap();
        session
            .link_objects(&parent_id, &child_id, super::LinkType::Free)
            .unwrap();

        assert_eq!(
            session.object_is_child_of(&parent_id, &child_id).unwrap(),
            true,
            "Objects linked."
        );

        let result = session.link_objects(
            &parent_id,
            &child_id,
            crate::object_storage::LinkType::Free,
        );
        assert_eq!(
            result,
            Err(Error::AlreadyLinked {
                parent_id,
                child_id
            })
        );
    }

    #[test]
    fn test_basic_unlinking_objects()
    {
        let mut session = temp_session_with_custom_sql(
            r#"
            CREATE TABLE obj_parent(id INTEGER PRIMARY KEY);
            CREATE TABLE obj_child(id INTEGER PRIMARY KEY);
        "#,
        );

        let parent_id = session.touch_object("parent").unwrap();
        let child_id = session.touch_object("child").unwrap();
        session
            .link_objects(&parent_id, &child_id, super::LinkType::Free)
            .unwrap();

        assert_eq!(
            session.object_is_child_of(&parent_id, &child_id).unwrap(),
            true,
            "Objects linked."
        );

        session.unlink_objects(&parent_id, &child_id).unwrap();
        assert_eq!(
            session.object_is_child_of(&parent_id, &child_id).unwrap(),
            false,
            "Objects unlinked."
        );
    }

    #[test]
    fn test_cyclic_strict_link_detection()
    {
        let mut session = temp_session_with_custom_sql(
            r#"
            CREATE TABLE obj_t1(id INTEGER PRIMARY KEY);
            CREATE TABLE obj_t2(id INTEGER PRIMARY KEY);
            CREATE TABLE obj_t3(id INTEGER PRIMARY KEY);
        "#,
        );

        let t1 = session.touch_object("t1").unwrap();
        let t2 = session.touch_object("t2").unwrap();
        let t3 = session.touch_object("t3").unwrap();

        session
            .link_objects(&t1, &t2, super::LinkType::Strict)
            .unwrap();
        session
            .link_objects(&t2, &t3, super::LinkType::Strict)
            .unwrap();

        assert_eq!(
            session.link_objects(&t3, &t1, super::LinkType::Strict),
            Err(Error::CyclicStrictLinkDetected {
                parent_id: t3.clone(),
                child_id:  t1.clone(),
            }),
            "Creating cyclic strict links denied."
        );

        assert_eq!(
            session.link_objects(
                &t3,
                &t3,
                crate::object_storage::LinkType::Strict
            ),
            Err(Error::CyclicStrictLinkDetected {
                parent_id: t3.clone(),
                child_id:  t3,
            }),
            "Creating selflinked strict relation."
        );
    }

    #[test]
    fn test_basic_dropping_objects()
    {
        let mut session = temp_session_with_custom_sql(
            r#"
            CREATE TABLE obj_parent(id INTEGER PRIMARY KEY);
        "#,
        );

        let parent_id = session.touch_object("parent").unwrap();
        session.drop_objects(vec![&parent_id]).unwrap();

        assert_eq!(session.object_exists(&parent_id).unwrap(), false);
    }

    #[test]
    fn test_dropping_objects_with_relations()
    {
        let mut session = temp_session_with_custom_sql(
            r#"
            CREATE TABLE obj_t1(id INTEGER PRIMARY KEY);
            CREATE TABLE obj_t2(id INTEGER PRIMARY KEY);
        "#,
        );

        let t1 = session.touch_object("t1").unwrap();
        let t2 = session.touch_object("t2").unwrap();
        session
            .link_objects(&t1, &t2, super::LinkType::Strict)
            .unwrap();

        let ids = session.drop_objects(vec![&t1]).unwrap();
        assert_eq!(session.object_exists(&t1).unwrap(), false);
        assert_eq!(session.object_exists(&t2).unwrap(), false);
        assert_eq!(ids, vec![t1, t2]);
    }

    #[test]
    fn test_setting_fields()
    {
        let mut session = temp_session_with_custom_sql(
            r#"
            CREATE TABLE obj_t(
                id INTEGER PRIMARY KEY,
                field TEXT
            );
        "#,
        );

        let t1 = session.touch_object("t").unwrap();

        let mut fields = Fields::new();
        fields.add("field", "test_value".into());
        session.object_set_fields(&t1, fields).unwrap();
    }

    #[test]
    fn test_getting_objects()
    {
        let mut session = temp_session_with_custom_sql(
            r#"
            CREATE TABLE obj_t(
                id INTEGER PRIMARY KEY,
                field TEXT
            );
        "#,
        );

        let t1 = session.touch_object("t").unwrap();

        let mut fields = Fields::new();
        fields.add("field", "test_value".into());
        session.object_set_fields(&t1, fields).unwrap();

        let objects = session
            .find_objects("t", Cond::Empty, &[], None, None)
            .unwrap();
        let field = objects.get(0).unwrap().fields.get(&"field".to_owned());

        assert_eq!(
            field,
            Some(&Some(FieldValue::Text("test_value".to_owned())))
        );

        let cond = CondBuilder::new_eq("field", "test_value".into()).cond();
        let objects = session.find_objects("t", cond, &[], None, None).unwrap();
        let field = objects.get(0).unwrap().fields.get(&"field".to_owned());

        assert_eq!(
            field,
            Some(&Some(FieldValue::Text("test_value".to_owned())))
        );
    }

    #[test]
    fn test_getting_related_objects()
    {
        let mut session = temp_session_with_custom_sql(
            r#"
            CREATE TABLE obj_parent(id INTEGER PRIMARY KEY);
            CREATE TABLE obj_child(id INTEGER PRIMARY KEY, field TEXT);
        "#,
        );

        let parent_id = session.touch_object("parent").unwrap();
        let child_id = session.touch_object("child").unwrap();
        session
            .link_objects(&parent_id, &child_id, super::LinkType::Free)
            .unwrap();

        let mut fields = Fields::new();
        fields.add("field", "test_value".into());
        session.object_set_fields(&child_id, fields).unwrap();

        let cond = CondBuilder::new_eq("field", "test_value".into()).cond();
        let objects = session
            .find_child_objects(&parent_id, "child", cond, &[], None, None)
            .unwrap();
        let (_object, link) = objects.get(0).unwrap();
        assert!(&link.parent_id == &parent_id && &link.child_id == &child_id);
    }

    #[bench]
    fn bench_linking_objects(b: &mut Bencher)
    {
        let mut session = temp_session_with_custom_sql(
            r#"
            CREATE TABLE obj_t(id INTEGER PRIMARY KEY);
            CREATE TABLE obj_t1(id INTEGER PRIMARY KEY);
        "#,
        );

        let tx = session.transaction().unwrap();

        let t = tx.touch_object("t").unwrap();

        b.iter(|| {
            let t1 = tx.touch_object("t1").unwrap();
            tx.link_objects(&t, &t1, super::LinkType::Free).unwrap();
        });

        tx.commit().unwrap();
    }

    #[bench]
    fn bench_finding_related_objects(b: &mut Bencher)
    {
        let mut session = temp_session_with_custom_sql(
            r#"
            CREATE TABLE obj_t(id INTEGER PRIMARY KEY);
            CREATE TABLE obj_t1(id INTEGER PRIMARY KEY, field TEXT);
        "#,
        );
        let tx = session.transaction().unwrap();

        let t = tx.touch_object("t").unwrap();
        for _ in 1..1000 {
            let t1 = tx.touch_object("t1").unwrap();
            let mut fields = Fields::new();
            fields.add("field", "text".into());
            tx.object_set_fields(&t1, fields).unwrap();
            tx.link_objects(&t, &t1, super::LinkType::Strict).unwrap();
        }

        b.iter(|| {
            tx.find_child_objects(&t, "t1", Cond::Empty, &[], None, None)
                .unwrap();
        });

        tx.commit().unwrap();
    }

    #[bench]
    fn bench_finding_basic_object_unique(b: &mut Bencher)
    {
        let mut session = temp_session_with_custom_sql(
            r#"
            CREATE TABLE obj_t1(id INTEGER PRIMARY KEY, field TEXT);
            CREATE UNIQUE INDEX indx0 ON obj_t1(field);
        "#,
        );
        let tx = session.transaction().unwrap();

        for i in 1..1000 {
            let t1 = tx.touch_object("t1").unwrap();
            let mut fields = Fields::new();
            fields.add("field", format!("text_{}", i).into());
            tx.object_set_fields(&t1, fields).unwrap();
        }

        tx.commit().unwrap();

        let mut i = 0;
        b.iter(|| {
            i += 1;
            session
                .find_unique_object(
                    "t1",
                    CondBuilder::new_eq("field", format!("text_{}", i).into())
                        .cond(),
                    &[],
                )
                .unwrap();
        });
    }

    #[test]
    fn test_getting_link_by_id()
    {
        let mut session = temp_session_with_custom_sql(
            r#"
            CREATE TABLE obj_t1(id INTEGER PRIMARY KEY);
            CREATE TABLE obj_t2(id INTEGER PRIMARY KEY);
        "#,
        );

        let t1 = session.touch_object("t1").unwrap();
        let t2 = session.touch_object("t2").unwrap();
        let link_id = session
            .link_objects(&t1, &t2, super::LinkType::Strict)
            .unwrap();

        assert_eq!(
            session.get_link_by_id(link_id).unwrap(),
            Some(Link {
                parent_id: t1,
                child_id:  t2,
                link_type: super::LinkType::Strict,
                meta:      None,
            })
        );
    }
}
