#![allow(dead_code)]

use std::{
    pin::Pin,
    sync::{
        Arc,
        Mutex,
    },
    time::Duration,
};

use crossbeam_channel::{
    unbounded,
    Receiver,
    Sender,
};
use tokio::{
    sync::mpsc,
    time::sleep,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    codegen::futures_core::Stream,
    Request,
    Response,
    Status,
};

use self::pb::storage_server::Storage as StorageTrait;
use crate::object_storage::{
    self,
    Cond,
    CondEntry,
    FieldValue,
    Fields,
    Link,
    LinkType,
    Object,
    ObjectId,
    Op,
    Session,
    Storage,
};

extern crate alloc;

pub mod pb
{
    #![allow(non_camel_case_types)]
    tonic::include_proto!("dndch.storage");
}

#[derive(Debug)]
pub enum Error
{
    MemberAlreadyExists,
}

#[derive(PartialEq, Clone, Debug)]
pub enum MemberRole
{
    GameMaster,
    Player(i64),
}

#[derive(Clone, Debug)]
pub struct Member
{
    role: MemberRole,
    name: String,
}

impl PartialEq for Member
{
    fn eq(&self, other: &Self) -> bool
    {
        self.role == other.role
    }
}

impl Into<pb::Member> for Member
{
    fn into(self) -> pb::Member
    {
        match self.role {
            MemberRole::GameMaster => pb::Member {
                role:         pb::MemberRole::GameMaster.into(),
                character_id: None,
                name:         self.name,
            },
            MemberRole::Player(character_id) => pb::Member {
                role:         pb::MemberRole::Player.into(),
                character_id: Some(character_id),
                name:         self.name,
            },
        }
    }
}

impl Into<pb::ObjectId> for ObjectId
{
    fn into(self) -> pb::ObjectId
    {
        pb::ObjectId {
            kind: self.kind.into(),
            id:   self.id,
        }
    }
}

impl Into<pb::FieldValue> for FieldValue
{
    fn into(self) -> pb::FieldValue
    {
        pb::FieldValue {
            field_value_union: Some(match self {
                FieldValue::Integer(i) => {
                    pb::field_value::FieldValueUnion::Integer(i)
                }
                FieldValue::Real(f) => {
                    pb::field_value::FieldValueUnion::Real(f as f32)
                }
                FieldValue::Text(t) => {
                    pb::field_value::FieldValueUnion::Text(t.into())
                }
                FieldValue::Blob(b) => {
                    pb::field_value::FieldValueUnion::Blob(b.into())
                }
            }),
        }
    }
}

impl Into<pb::Link> for Link
{
    fn into(self) -> pb::Link
    {
        pb::Link {
            parent: Some(self.parent_id.into()),
            child:  Some(self.child_id.into()),
            r#type: match self.link_type {
                LinkType::Free => 0,
                LinkType::Strict => 1,
            },
            meta:   self.meta.into(),
        }
    }
}

impl Into<pb::Object> for Object
{
    fn into(self) -> pb::Object
    {
        let mut entries: Vec<pb::object::FieldEntry> = Vec::new();
        for (name, value) in self.fields.into_iter() {
            entries.push(pb::object::FieldEntry {
                name,
                value: value.map(|value| value.into()),
            })
        }

        pb::Object {
            id:     Some(self.id.into()),
            fields: entries,
        }
    }
}

pub struct MemberCollection
{
    last_id: u32,
    members: Vec<(u32, Member)>,
}

impl MemberCollection
{
    pub fn new() -> Self
    {
        Self {
            last_id: 0,
            members: vec![],
        }
    }

    pub fn set(&mut self, member: Member) -> Result<u32, Error>
    {
        for (_, other_member) in self.members.iter() {
            if other_member == &member {
                return Err(Error::MemberAlreadyExists);
            }
        }
        self.last_id += 1;
        self.members.push((self.last_id, member));
        Ok(self.last_id)
    }

    pub fn get(&self, member_id: u32) -> Option<&Member>
    {
        for (id, member) in self.members.iter() {
            if *id == member_id {
                return Some(member);
            }
        }
        None
    }

    pub fn remove(&mut self, member_id: u32)
    {
        self.members.retain(|(id, _)| id == &member_id);
    }

    pub fn find_for_character_id(&self, id: i64) -> Option<&Member>
    {
        for (_, member) in self.members.iter() {
            match member.role {
                MemberRole::Player(character_id) if character_id == id => {
                    return Some(&member)
                }
                _ => {}
            }
        }
        None
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum EventKind
{
    Join
    {
        member: Member
    },
    Leave
    {
        member: Member
    },
    CreateObject
    {
        object_id: ObjectId
    },
    LinkObjects
    {
        parent_id: ObjectId,
        child_id:  ObjectId,
    },
    UnlinkObjects
    {
        id0: ObjectId, id1: ObjectId
    },
    DropObject
    {
        object_id: ObjectId
    },
    SetObjectField
    {
        object_id: ObjectId,
        name:      String,
        value:     FieldValue,
    },
}

impl Into<pb::event::EventKind> for EventKind
{
    fn into(self) -> pb::event::EventKind
    {
        pb::event::EventKind {
            event_kind: Some(match self {
                EventKind::Join { member } => {
                    pb::event::event_kind::EventKind::Join(pb::event::Join {
                        member: Some(member.into()),
                    })
                }
                EventKind::Leave { member } => {
                    pb::event::event_kind::EventKind::Leave(pb::event::Leave {
                        member: Some(member.into()),
                    })
                }
                EventKind::CreateObject { object_id } => {
                    pb::event::event_kind::EventKind::CreateObject(
                        pb::event::CreateObject {
                            id: Some(object_id.into()),
                        },
                    )
                }
                EventKind::LinkObjects {
                    parent_id,
                    child_id,
                } => pb::event::event_kind::EventKind::LinkObjects(
                    pb::event::LinkObjects {
                        parent: Some(parent_id.into()),
                        child:  Some(child_id.into()),
                    },
                ),
                EventKind::UnlinkObjects { id0, id1 } => {
                    pb::event::event_kind::EventKind::UnlinkObjects(
                        pb::event::UnlinkObjects {
                            id0: Some(id0.into()),
                            id1: Some(id1.into()),
                        },
                    )
                }
                EventKind::DropObject { object_id } => {
                    pb::event::event_kind::EventKind::DropObject(
                        pb::event::DropObject {
                            id: Some(object_id.into()),
                        },
                    )
                }
                EventKind::SetObjectField {
                    object_id,
                    name,
                    value,
                } => pb::event::event_kind::EventKind::SetObjectField(
                    pb::event::SetObjectField {
                        id:    Some(object_id.into()),
                        name:  name.into(),
                        value: Some(value.into()),
                    },
                ),
            }),
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct Event
{
    initiator: Member,
    kind:      EventKind,
}

impl Into<pb::Event> for Event
{
    fn into(self) -> pb::Event
    {
        pb::Event {
            initiator:  Some(self.initiator.into()),
            event_kind: Some(self.kind.into()),
        }
    }
}

pub struct StorageService
{
    storage:    Storage,
    members:    Arc<Mutex<MemberCollection>>,
    mpmc_event: (Sender<Event>, Receiver<Event>),
}

impl StorageService
{
    pub fn new(storage: Storage) -> Self
    {
        Self {
            storage,
            members: Arc::new(Mutex::new(MemberCollection::new())),
            mpmc_event: unbounded(),
        }
    }
}

type RpcResult<T> = Result<Response<T>, Status>;

impl From<object_storage::Error> for Status
{
    fn from(error: object_storage::Error) -> Self
    {
        Status::internal(error.to_string())
    }
}

#[tonic::async_trait]
impl StorageTrait for StorageService
{
    async fn characters_list(
        &self,
        _: Request<pb::Empty>,
    ) -> RpcResult<pb::CharactersListResponse>
    {
        characters_list_impl(
            &mut self.storage.start_session()?,
            self.members.clone(),
        )
    }

    async fn auth(
        &self,
        request: Request<pb::AuthRequest>,
    ) -> RpcResult<pb::AuthResponse>
    {
        auth_impl(
            &mut self.storage.start_session()?,
            request,
            self.members.clone(),
            self.mpmc_event.0.clone(),
        )
    }

    async fn leave(
        &self,
        request: Request<pb::LeaveRequest>,
    ) -> RpcResult<pb::Empty>
    {
        leave_impl(request, self.members.clone(), self.mpmc_event.0.clone())
    }

    async fn create_object(
        &self,
        request: Request<pb::CreateObjectRequest>,
    ) -> RpcResult<pb::CreateObjectResponse>
    {
        create_object_impl(
            &mut self.storage.start_session()?,
            request,
            self.members.clone(),
            self.mpmc_event.0.clone(),
        )
    }

    async fn set_object_fields(
        &self,
        request: Request<pb::SetObjectFieldsRequest>,
    ) -> RpcResult<pb::Empty>
    {
        set_object_fields_impl(
            &mut self.storage.start_session()?,
            request,
            self.members.clone(),
            self.mpmc_event.0.clone(),
        )
    }

    async fn link_objects(
        &self,
        request: Request<pb::LinkObjectsRequest>,
    ) -> RpcResult<pb::LinkObjectsResponse>
    {
        link_objects_impl(
            &mut self.storage.start_session()?,
            request,
            self.members.clone(),
            self.mpmc_event.0.clone(),
        )
    }

    async fn unlink_objects(
        &self,
        request: Request<pb::UnlinkObjectsRequest>,
    ) -> RpcResult<pb::Empty>
    {
        unlink_objects_impl(
            &mut self.storage.start_session()?,
            request,
            self.members.clone(),
            self.mpmc_event.0.clone(),
        )
    }

    async fn drop_object(
        &self,
        request: Request<pb::DropObjectRequest>,
    ) -> RpcResult<pb::Empty>
    {
        drop_object_impl(
            &mut self.storage.start_session()?,
            request,
            self.members.clone(),
            self.mpmc_event.0.clone(),
        )
    }

    async fn find_objects(
        &self,
        request: Request<pb::FindObjectsRequest>,
    ) -> RpcResult<pb::FindObjectsResponse>
    {
        find_objects_impl(
            &mut self.storage.start_session()?,
            request,
            self.members.clone(),
        )
    }

    type watchStream =
        Pin<Box<dyn Stream<Item = Result<pb::Event, Status>> + Send>>;

    async fn watch(&self, _: Request<pb::Empty>)
        -> RpcResult<Self::watchStream>
    {
        watch_impl(self.mpmc_event.1.clone())
    }
}

#[cfg(test)]
pub mod test_utils
{
    use std::sync::{
        Arc,
        Mutex,
    };

    use lazy_static::lazy_static;
    use random_string::generate;
    use tempdir::TempDir;

    use super::MemberCollection;
    use crate::object_storage::{
        CreateOptions,
        Session,
        Storage,
    };

    lazy_static! {
        static ref TEMPSTORAGE: TempDir = TempDir::new("test_storage").unwrap();
    }

    pub fn temp_session() -> Session
    {
        temp_session_with_options(CreateOptions {
            name: "Тестовое хранилище".to_owned(),
            ..Default::default()
        })
    }

    pub fn temp_session_with_options(options: CreateOptions) -> Session
    {
        let path = TEMPSTORAGE
            .path()
            .join(format!("{}.dbsqlite", generate(10, "0123456789abcdef")));
        Storage::create(path.as_os_str().to_str().unwrap(), options)
            .unwrap()
            .start_session()
            .unwrap()
    }

    pub fn temp_session_with_custom_sql(sql: &str) -> Session
    {
        temp_session_with_options(CreateOptions {
            custom_sql: Some(sql.to_owned()),
            ..Default::default()
        })
    }

    pub fn create_member_collection() -> Arc<Mutex<MemberCollection>>
    {
        Arc::new(Mutex::new(MemberCollection::new()))
    }
}

fn characters_list_impl(
    session: &mut Session,
    members: Arc<Mutex<MemberCollection>>,
) -> RpcResult<pb::CharactersListResponse>
{
    let mut characters: Vec<pb::characters_list_response::CharacterEntry> =
        Vec::new();
    for object in session.find_all_objects("characters")? {
        let name = match object.fields.get("name") {
            Some(Some(name)) => match name {
                FieldValue::Text(name) => name.clone(),
                _ => {
                    return Err(Status::internal(
                        "Invalid character name value type.",
                    ))
                }
            },
            Some(None) => "Unnamed".to_owned(),
            None => {
                return Err(Status::internal(
                    "Field \"name\" not found in character fields.",
                ))
            }
        };
        let member = match members.lock() {
            Ok(guard) => guard
                .find_for_character_id(object.id.id)
                .map(|member| member.clone().into()),
            Err(_) => {
                return Err(Status::internal("Cannot lock members collection."))
            }
        };
        let id = object.id.id as u64;
        characters.push(pb::characters_list_response::CharacterEntry {
            id,
            name,
            member,
        });
    }

    Ok(Response::new(pb::CharactersListResponse { characters }))
}

fn watch_impl(
    event_rx: Receiver<Event>,
) -> RpcResult<<StorageService as StorageTrait>::watchStream>
{
    let (tx, rx): (
        mpsc::Sender<Result<pb::Event, Status>>,
        mpsc::Receiver<Result<pb::Event, Status>>,
    ) = mpsc::channel(100);

    tokio::spawn(async move {
        loop {
            match event_rx.recv() {
                Ok(event) => {
                    if let Err(_) = tx.send(Ok(event.into())).await {
                        break;
                    }
                }
                Err(_) => {
                    sleep(Duration::from_millis(50)).await;
                }
            }
        }
    });

    Ok(Response::new(Box::pin(ReceiverStream::new(rx))
        as <StorageService as StorageTrait>::watchStream))
}

fn auth_impl(
    session: &mut Session,
    request: Request<pb::AuthRequest>,
    members: Arc<Mutex<MemberCollection>>,
    tx: Sender<Event>,
) -> RpcResult<pb::AuthResponse>
{
    let member = match request.into_inner().member {
        Some(pb_member) => Member {
            role: match pb_member.role {
                0 => MemberRole::GameMaster,
                1 => MemberRole::Player(match pb_member.character_id {
                    Some(id) => id,
                    None => {
                        return Err(Status::invalid_argument(
                            "Character id not setted.",
                        ))
                    }
                }),
                _ => {
                    return Err(Status::invalid_argument(
                        "Invalid range of member role enum.",
                    ))
                }
            },
            name: pb_member.name,
        },
        None => {
            return Err(Status::invalid_argument("Member field not setted."))
        }
    };

    if let Member {
        role: MemberRole::Player(character_id),
        name: _,
    } = member
    {
        if !session.object_exists(&ObjectId::new(
            "characters".to_owned(),
            character_id,
        ))? {
            return Err(Status::not_found(
                format!("Character #{} not found.", character_id).as_str(),
            ));
        }
    }

    let id = match members.lock() {
        Ok(mut guard) => match guard.set(member.clone()) {
            Ok(id) => id,
            Err(Error::MemberAlreadyExists) => {
                return Err(Status::already_exists("Member already exists."))
            }
        },
        Err(e) => {
            return Err(Status::internal(
                format!("Cannot lock mutex: {}", e.to_string()).as_str(),
            ))
        }
    };

    let _ = tx.send(Event {
        initiator: member.clone(),
        kind:      EventKind::Join { member },
    });

    Ok(Response::new(pb::AuthResponse {
        member_id: id as u64,
    }))
}

#[cfg(test)]
mod auth_tests
{
    use crossbeam_channel::unbounded;
    use tonic::{
        Code,
        Request,
    };

    use super::{
        auth_impl,
        pb,
        test_utils::temp_session,
    };
    use crate::{
        object_storage::Fields,
        rpc::{
            test_utils::create_member_collection,
            EventKind,
            Member,
            MemberRole,
        },
    };

    #[test]
    fn test_sucess_auth_player_member()
    {
        let mut session = temp_session();
        let id = session.touch_object("characters").unwrap();
        let mut fields = Fields::new();
        fields.add("name", "Test character".into());
        session.object_set_fields(&id, fields).unwrap();

        let members = create_member_collection();
        let (tx, rx) = unbounded();
        let member = Member {
            role: MemberRole::Player(id.id),
            name: "bob".to_owned(),
        };
        let request = Request::new(pb::AuthRequest {
            member: Some(member.clone().into()),
        });
        let result = auth_impl(&mut session, request, members, tx);
        assert!(result.is_ok());
        assert!(rx.recv().unwrap().kind == EventKind::Join { member });
    }

    #[test]
    fn test_success_auth_game_master_member()
    {
        let mut session = temp_session();

        let members = create_member_collection();
        let (tx, rx) = unbounded();
        let member = Member {
            role: MemberRole::GameMaster,
            name: "bob".to_owned(),
        };
        let request = Request::new(pb::AuthRequest {
            member: Some(member.clone().into()),
        });
        let result = auth_impl(&mut session, request, members, tx);
        assert!(result.is_ok());
        assert!(rx.recv().unwrap().kind == EventKind::Join { member });
    }

    #[test]
    fn test_try_auth_player_member_for_not_found_character()
    {
        let mut session = temp_session();

        let members = create_member_collection();
        let (tx, _rx) = unbounded();
        let member = Member {
            role: MemberRole::Player(1),
            name: "bob".to_owned(),
        };
        let request = Request::new(pb::AuthRequest {
            member: Some(member.clone().into()),
        });
        let result = auth_impl(&mut session, request, members, tx);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), Code::NotFound);
    }

    #[test]
    fn test_try_auth_one_member_kind_two_times()
    {
        let mut session = temp_session();

        let members = create_member_collection();
        let (tx, rx) = unbounded();
        let member = Member {
            role: MemberRole::GameMaster,
            name: "bob".to_owned(),
        };
        let request = Request::new(pb::AuthRequest {
            member: Some(member.clone().into()),
        });
        let result =
            auth_impl(&mut session, request, members.clone(), tx.clone());
        assert!(result.is_ok());
        assert!(rx.recv().unwrap().kind == EventKind::Join { member });

        let mut session = temp_session();
        let member = Member {
            role: MemberRole::GameMaster,
            name: "bob".to_owned(),
        };
        let request = Request::new(pb::AuthRequest {
            member: Some(member.clone().into()),
        });
        let result = auth_impl(&mut session, request, members, tx);

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), Code::AlreadyExists);
    }
}

fn leave_impl(
    request: Request<pb::LeaveRequest>,
    members: Arc<Mutex<MemberCollection>>,
    tx: Sender<Event>,
) -> RpcResult<pb::Empty>
{
    let request = request.into_inner();
    match members.lock() {
        Ok(mut guard) => match guard.get(request.member_id as u32) {
            Some(member) => {
                let _ = tx.send(Event {
                    initiator: member.clone(),
                    kind:      EventKind::Leave {
                        member: member.clone(),
                    },
                });
                guard.remove(request.member_id as u32);
            }
            None => {
                return Err(Status::not_found(format!(
                    "Member #{} not found.",
                    request.member_id
                )));
            }
        },
        Err(e) => {
            return Err(Status::internal(
                format!("Cannot lock mutex: {}", e.to_string()).as_str(),
            ))
        }
    }
    Ok(Response::new(pb::Empty {}))
}

#[cfg(test)]
mod leave_tests
{
    use crossbeam_channel::unbounded;
    use tonic::Request;

    use crate::rpc::{
        auth_impl,
        leave_impl,
        pb,
        test_utils::{
            create_member_collection,
            temp_session,
        },
        EventKind,
        Member,
        MemberRole,
    };

    #[test]
    fn test_leave_member()
    {
        let mut session = temp_session();

        let members = create_member_collection();
        let (tx, rx) = unbounded();
        let member = Member {
            role: MemberRole::GameMaster,
            name: "bob".to_owned(),
        };
        let request = Request::new(pb::AuthRequest {
            member: Some(member.clone().into()),
        });
        let result =
            auth_impl(&mut session, request, members.clone(), tx.clone());
        assert!(result.is_ok());
        assert!(
            rx.recv().unwrap().kind
                == EventKind::Join {
                    member: member.clone(),
                }
        );

        let request = Request::new(pb::LeaveRequest {
            member_id: result.unwrap().into_inner().member_id,
        });
        let result = leave_impl(request, members, tx);
        assert!(result.is_ok());
        assert!(
            rx.recv().unwrap().kind
                == EventKind::Leave {
                    member: member.clone(),
                }
        );
    }
}

fn create_object_impl(
    session: &mut Session,
    request: Request<pb::CreateObjectRequest>,
    members: Arc<Mutex<MemberCollection>>,
    tx: Sender<Event>,
) -> RpcResult<pb::CreateObjectResponse>
{
    let request = request.into_inner();
    let member = find_member(members, request.member_id)?;

    let transact = session.transaction()?;
    let object_id = transact.touch_object(request.kind.as_str())?;

    let mut fields = Fields::new();
    for value in request.values {
        fields.add(
            value.name.as_str(),
            match value.value {
                Some(entry_value) => match entry_value.field_value_union {
                    Some(union_value) => match union_value {
                        pb::field_value::FieldValueUnion::Text(t) => {
                            FieldValue::Text(t.into())
                        }
                        pb::field_value::FieldValueUnion::Integer(i) => {
                            FieldValue::Integer(i)
                        }
                        pb::field_value::FieldValueUnion::Real(r) => {
                            FieldValue::Real(r.into())
                        }
                        pb::field_value::FieldValueUnion::Blob(b) => {
                            FieldValue::Blob(b.into())
                        }
                    },
                    None => {
                        return Err(Status::invalid_argument(format!(
                            "FieldValueUnion for {} is None",
                            value.name
                        )))
                    }
                },
                None => {
                    return Err(Status::invalid_argument(format!(
                        "FieldEntry value for {} is None",
                        value.name
                    )))
                }
            },
        );
    }
    transact.object_set_fields(&object_id, fields)?;

    let mut link: Option<Link> = None;
    if let Some(relation) = request.relation {
        let parent_object_id = convert_object_id(relation.parent)?;
        let link_type = convert_link_type(relation.link_type)?;
        let link_id =
            transact.link_objects(&parent_object_id, &object_id, link_type)?;
        link = Some(match transact.get_link_by_id(link_id)? {
            Some(link) => link,
            None => return Err(Status::internal("internal error.")),
        });
    }

    let _ = tx.send(Event {
        initiator: member.clone(),
        kind:      EventKind::CreateObject {
            object_id: object_id.clone(),
        },
    });
    if let Some(link) = link.as_ref() {
        let _ = tx.send(Event {
            initiator: member,
            kind:      EventKind::LinkObjects {
                parent_id: link.parent_id.clone(),
                child_id:  link.child_id.clone(),
            },
        });
    }

    transact.commit()?;
    Ok(Response::new(pb::CreateObjectResponse {
        id:   Some(object_id.into()),
        link: link.map(|link| link.into()),
    }))
}

#[cfg(test)]
mod create_object_tests
{
    use std::sync::{
        Arc,
        Mutex,
    };

    use crossbeam_channel::unbounded;
    use tonic::Request;

    use super::{
        create_object_impl,
        pb,
        test_utils::temp_session_with_custom_sql,
        Event,
        EventKind,
        Member,
        MemberCollection,
        MemberRole,
    };
    use crate::object_storage::{
        FieldValue,
        ObjectId,
    };

    #[test]
    fn test_creating_basic_object()
    {
        let mut session = temp_session_with_custom_sql(
            r#"
            CREATE TABLE obj_t(id INTEGER PRIMARY KEY, field TEXT);
        "#,
        );
        let members = Arc::new(Mutex::new(MemberCollection::new()));
        let member = Member {
            role: MemberRole::GameMaster,
            name: "Bob".to_owned(),
        };
        let (tx, rx) = unbounded();
        let member_id = members.lock().unwrap().set(member.clone()).unwrap();
        let request = Request::new(pb::CreateObjectRequest {
            member_id: member_id as u64,
            relation:  None,
            kind:      "t".to_owned(),
            values:    vec![pb::FieldEntry {
                name:  "field".to_owned(),
                value: Some(pb::FieldValue {
                    field_value_union: Some(
                        pb::field_value::FieldValueUnion::Text(
                            "test_data".to_owned(),
                        ),
                    ),
                }),
            }],
        });

        let response = create_object_impl(&mut session, request, members, tx)
            .unwrap()
            .into_inner();
        let id = response.id.unwrap();
        let object = session
            .find_object_by_id(&ObjectId::new(id.kind.clone(), id.id))
            .unwrap();
        assert!(object.is_some());
        assert_eq!(
            object.unwrap().fields.get("field"),
            Some(&Some(FieldValue::Text("test_data".to_owned())))
        );
        assert_eq!(
            rx.recv().unwrap(),
            Event {
                initiator: member,
                kind:      EventKind::CreateObject {
                    object_id: ObjectId::new("t".to_owned(), 1),
                },
            }
        );
    }

    #[test]
    fn test_creating_object_with_relation()
    {
        let mut session = temp_session_with_custom_sql(
            r#"
            CREATE TABLE obj_t(id INTEGER PRIMARY KEY, field TEXT);
            CREATE TABLE obj_parent(id INTEGER PRIMARY KEY);
        "#,
        );
        let members = Arc::new(Mutex::new(MemberCollection::new()));
        let member = Member {
            role: MemberRole::GameMaster,
            name: "Bob".to_owned(),
        };

        let parent_id = session.touch_object("parent").unwrap();

        let (tx, rx) = unbounded();
        let member_id = members.lock().unwrap().set(member.clone()).unwrap();
        let request = Request::new(pb::CreateObjectRequest {
            member_id: member_id as u64,
            relation:  Some(pb::create_object_request::ParentRelation {
                parent:    Some(parent_id.clone().into()),
                link_type: 1,
            }),
            kind:      "t".to_owned(),
            values:    vec![pb::FieldEntry {
                name:  "field".to_owned(),
                value: Some(pb::FieldValue {
                    field_value_union: Some(
                        pb::field_value::FieldValueUnion::Text(
                            "test_data".to_owned(),
                        ),
                    ),
                }),
            }],
        });

        let _ = create_object_impl(&mut session, request, members, tx)
            .unwrap()
            .into_inner();
        assert_eq!(
            rx.recv().unwrap(),
            Event {
                initiator: member.clone(),
                kind:      EventKind::CreateObject {
                    object_id: ObjectId::new("t".to_owned(), 1),
                },
            }
        );
        assert_eq!(
            rx.recv().unwrap(),
            Event {
                initiator: member,
                kind:      EventKind::LinkObjects {
                    child_id:  ObjectId::new("t".to_owned(), 1),
                    parent_id: parent_id,
                },
            }
        )
    }
}

fn set_object_fields_impl(
    session: &mut Session,
    request: Request<pb::SetObjectFieldsRequest>,
    members: Arc<Mutex<MemberCollection>>,
    tx: Sender<Event>,
) -> RpcResult<pb::Empty>
{
    let request = request.into_inner();
    let member = find_member(members, request.member_id)?;

    let mut fields = Fields::new();
    for value in request.fields {
        fields.add(
            value.name.as_str(),
            match value.value {
                Some(entry_value) => match entry_value.field_value_union {
                    Some(union_value) => match union_value {
                        pb::field_value::FieldValueUnion::Text(t) => {
                            FieldValue::Text(t.into())
                        }
                        pb::field_value::FieldValueUnion::Integer(i) => {
                            FieldValue::Integer(i)
                        }
                        pb::field_value::FieldValueUnion::Real(r) => {
                            FieldValue::Real(r.into())
                        }
                        pb::field_value::FieldValueUnion::Blob(b) => {
                            FieldValue::Blob(b.into())
                        }
                    },
                    None => {
                        return Err(Status::invalid_argument(format!(
                            "FieldValueUnion for {} is None",
                            value.name
                        )))
                    }
                },
                None => {
                    return Err(Status::invalid_argument(format!(
                        "FieldEntry value for {} is None",
                        value.name
                    )))
                }
            },
        );
    }

    let object_id = match request.id {
        Some(id) => ObjectId::new(id.kind.clone(), id.id),
        None => {
            return Err(Status::invalid_argument("object Id is not defined."))
        }
    };

    session.object_set_fields(&object_id, fields.clone())?;

    for (name, value) in fields.into_entries() {
        let _ = tx.send(Event {
            initiator: member.clone(),
            kind:      EventKind::SetObjectField {
                object_id: object_id.clone(),
                name,
                value,
            },
        });
    }

    Ok(Response::new(pb::Empty {}))
}

#[cfg(test)]
mod set_fields_tests
{
    use std::sync::{
        Arc,
        Mutex,
    };

    use crossbeam_channel::unbounded;
    use tonic::Request;

    use super::{
        pb,
        set_object_fields_impl,
        test_utils::temp_session_with_custom_sql,
        Member,
        MemberCollection,
        MemberRole,
    };
    use crate::object_storage::FieldValue;

    #[test]
    fn test_setting_fields()
    {
        let mut session = temp_session_with_custom_sql(
            r#"
            CREATE TABLE obj_t(id INTEGER PRIMARY KEY, field TEXT);
        "#,
        );
        let object_id = session.touch_object("t").unwrap();

        let members = Arc::new(Mutex::new(MemberCollection::new()));
        let member = Member {
            role: MemberRole::GameMaster,
            name: "Bob".to_owned(),
        };
        let (tx, _rx) = unbounded();
        let member_id = members.lock().unwrap().set(member.clone()).unwrap();
        let request = Request::new(pb::SetObjectFieldsRequest {
            member_id: member_id as u64,
            id:        Some(object_id.clone().into()),
            fields:    vec![pb::FieldEntry {
                name:  "field".to_owned(),
                value: Some(pb::FieldValue {
                    field_value_union: Some(
                        pb::field_value::FieldValueUnion::Text(
                            "test_data".to_owned(),
                        ),
                    ),
                }),
            }],
        });

        let _ = set_object_fields_impl(&mut session, request, members, tx);
        let object = session.find_object_by_id(&object_id).unwrap().unwrap();
        assert_eq!(
            object.fields.get("field"),
            Some(&Some(FieldValue::Text("test_data".to_owned())))
        )
    }
}

fn link_objects_impl(
    session: &mut Session,
    request: Request<pb::LinkObjectsRequest>,
    members: Arc<Mutex<MemberCollection>>,
    tx: Sender<Event>,
) -> RpcResult<pb::LinkObjectsResponse>
{
    let request = request.into_inner();
    let member = find_member(members, request.member_id)?;

    let parent_id = convert_object_id(request.parent)?;
    let child_id = convert_object_id(request.child)?;
    let link_type = convert_link_type(request.r#type)?;

    let transact = session.transaction()?;
    let link_id = transact.link_objects(&parent_id, &child_id, link_type)?;
    if let Some(meta) = request.meta {
        transact.set_raw_link_meta(&parent_id, &child_id, meta)?;
    }
    transact.commit()?;

    let _ = tx.send(Event {
        initiator: member,
        kind:      EventKind::LinkObjects {
            parent_id,
            child_id,
        },
    });
    Ok(Response::new(pb::LinkObjectsResponse {
        link_id: link_id as u64,
    }))
}

fn unlink_objects_impl(
    session: &mut Session,
    request: Request<pb::UnlinkObjectsRequest>,
    members: Arc<Mutex<MemberCollection>>,
    tx: Sender<Event>,
) -> RpcResult<pb::Empty>
{
    let request = request.into_inner();
    let member = find_member(members, request.member_id)?;

    let parent_id = convert_object_id(request.parent)?;
    let child_id = convert_object_id(request.child)?;

    session.unlink_objects(&parent_id, &child_id)?;
    let _ = tx.send(Event {
        initiator: member,
        kind:      EventKind::UnlinkObjects {
            id0: parent_id,
            id1: child_id,
        },
    });

    Ok(Response::new(pb::Empty {}))
}

fn find_objects_impl(
    session: &mut Session,
    request: Request<pb::FindObjectsRequest>,
    members: Arc<Mutex<MemberCollection>>,
) -> RpcResult<pb::FindObjectsResponse>
{
    let request = request.into_inner();
    let _ = find_member(members, request.member_id)?;

    let mut limit: Option<i64> = None;
    let mut offset: Option<i64> = None;

    if let Some(slice) = request.slice {
        limit = Some(slice.limit as i64);
        if let Some(slice_offset) = slice.offset {
            offset = Some(slice_offset as i64);
        }
    };

    let mut cond: Cond = Cond::Empty;
    if let Some(request_cond) = request.cond {
        cond = convert_cond(Box::new(request_cond))?;
    }

    if let Some(parent_id) = request.child_of {
        let result = session.find_child_objects(
            &convert_object_id(Some(parent_id))?,
            request.kind.as_str(),
            cond,
            &[],
            limit,
            offset,
        )?;
        let mut entries: Vec<pb::find_objects_response::ResponseEntry> =
            Vec::new();
        for (object, link) in result {
            entries.push(pb::find_objects_response::ResponseEntry {
                object: Some(object.into()),
                link:   Some(link.into()),
            })
        }
        Ok(Response::new(pb::FindObjectsResponse { entries }))
    } else {
        let result = session.find_objects(
            request.kind.as_str(),
            cond,
            &[],
            limit,
            offset,
        )?;
        let mut entries: Vec<pb::find_objects_response::ResponseEntry> =
            Vec::new();
        for object in result {
            entries.push(pb::find_objects_response::ResponseEntry {
                object: Some(object.into()),
                link:   None,
            })
        }
        Ok(Response::new(pb::FindObjectsResponse { entries }))
    }
}

#[cfg(test)]
mod find_object_tests
{
    use std::sync::{
        Arc,
        Mutex,
    };

    use tonic::Request;

    use super::{
        find_objects_impl,
        pb,
        test_utils::temp_session_with_custom_sql,
        Member,
        MemberCollection,
        MemberRole,
    };

    #[test]
    fn test_finding_basic_objects()
    {
        let mut session = temp_session_with_custom_sql(
            r#"
            CREATE TABLE obj_t(id INTEGER PRIMARY KEY, field TEXT);
        "#,
        );
        let _ = session.touch_object("t").unwrap();

        let members = Arc::new(Mutex::new(MemberCollection::new()));
        let member = Member {
            role: MemberRole::GameMaster,
            name: "Bob".to_owned(),
        };
        let member_id = members.lock().unwrap().set(member.clone()).unwrap();

        let request = Request::new(pb::FindObjectsRequest {
            member_id: member_id as u64,
            kind:      "t".to_owned(),
            slice:     None,
            child_of:  None,
            cond:      Some(pb::Cond {
                cond_type: Some(pb::cond::CondType::Common(pb::cond::Common {
                    field: "id".to_owned(),
                    op:    0,
                    value: Some(pb::FieldValue {
                        field_value_union: Some(
                            pb::field_value::FieldValueUnion::Integer(1),
                        ),
                    }),
                })),
            }),
        });

        find_objects_impl(&mut session, request, members).unwrap();
    }
}

fn drop_object_impl(
    session: &mut Session,
    request: Request<pb::DropObjectRequest>,
    members: Arc<Mutex<MemberCollection>>,
    tx: Sender<Event>,
) -> RpcResult<pb::Empty>
{
    let request = request.into_inner();
    let member = find_member(members, request.member_id)?;
    let object_id = convert_object_id(request.id)?;

    let deleted_objects = session.drop_objects(vec![&object_id])?;

    for object_id in deleted_objects {
        let _ = tx.send(Event {
            initiator: member.clone(),
            kind:      EventKind::DropObject { object_id },
        });
    }

    Ok(Response::new(pb::Empty {}))
}

fn convert_object_id(
    object_id: Option<pb::ObjectId>,
) -> Result<ObjectId, Status>
{
    match object_id {
        Some(id) => Ok(ObjectId::new(id.kind.clone(), id.id)),
        None => Err(Status::invalid_argument("Id field not defined.")),
    }
}

fn convert_link_type(link_type: i32) -> Result<LinkType, Status>
{
    match link_type {
        0 => Ok(LinkType::Free),
        1 => Ok(LinkType::Strict),
        _ => Err(Status::invalid_argument("Invalid link type.")),
    }
}

fn find_member(
    members: Arc<Mutex<MemberCollection>>,
    member_id: u64,
) -> Result<Member, Status>
{
    match members.lock() {
        Ok(guard) => match guard.get(member_id as u32) {
            Some(member) => Ok(member.clone()),
            None => Err(Status::invalid_argument(format!(
                "Cannot find member with id: #{}",
                member_id
            ))),
        },
        Err(e) => Err(Status::internal(format!("Cannot lock mutex: {}", e))),
    }
}

fn convert_cond(cond: alloc::boxed::Box<pb::Cond>) -> Result<Cond, Status>
{
    let cond_type: pb::cond::CondType = match cond.cond_type {
        Some(cond_type) => cond_type,
        None => return Err(Status::invalid_argument("Cond type not defined.")),
    };

    let cond = match cond_type {
        pb::cond::CondType::And(and) => {
            let left: alloc::boxed::Box<pb::Cond> = match and.left {
                Some(l) => l,
                None => {
                    return Err(Status::invalid_argument(
                        "Condition not defined.",
                    ))
                }
            };
            let right: alloc::boxed::Box<pb::Cond> = match and.right {
                Some(r) => r,
                None => {
                    return Err(Status::invalid_argument(
                        "Condition not defined.",
                    ))
                }
            };
            Cond::And(
                Box::new(convert_cond(left)?),
                Box::new(convert_cond(right)?),
            )
        }
        pb::cond::CondType::Or(or) => {
            let left: alloc::boxed::Box<pb::Cond> = match or.left {
                Some(l) => l,
                None => {
                    return Err(Status::invalid_argument(
                        "Condition not defined.",
                    ))
                }
            };
            let right: alloc::boxed::Box<pb::Cond> = match or.right {
                Some(r) => r,
                None => {
                    return Err(Status::invalid_argument(
                        "Condition not defined.",
                    ))
                }
            };
            Cond::Or(
                Box::new(convert_cond(left)?),
                Box::new(convert_cond(right)?),
            )
        }
        pb::cond::CondType::Common(common) => {
            let op = match common.op {
                0 => Op::Eq,
                1 => Op::Neq,
                2 => Op::Gt,
                3 => Op::Lt,
                4 => Op::Gtq,
                5 => Op::Ltq,
                _ => {
                    return Err(Status::invalid_argument(
                        "Invalid common operation code.",
                    ))
                }
            };
            let value = match common.value {
                Some(pb::FieldValue {
                    field_value_union: Some(v),
                }) => convert_field_value(v),
                _ => {
                    return Err(Status::invalid_argument(
                        "Field value union not defined.",
                    ))
                }
            };
            Cond::Common(
                CondEntry::Field(common.field),
                op,
                CondEntry::Value(value),
            )
        }
        pb::cond::CondType::In(r#in) => {
            let mut values = Vec::new();
            for value in r#in.values {
                values.push(match value.field_value_union {
                    Some(v) => CondEntry::Value(convert_field_value(v)),
                    _ => {
                        return Err(Status::invalid_argument(
                            "Field value union not defined.",
                        ))
                    }
                });
            }
            Cond::In(CondEntry::Field(r#in.field), values, r#in.invert)
        }
        pb::cond::CondType::Like(like) => {
            Cond::Like(CondEntry::Field(like.field), like.pattern, like.invert)
        }
    };

    Ok(cond)
}

fn convert_field_value(
    value_union: pb::field_value::FieldValueUnion,
) -> FieldValue
{
    match value_union {
        pb::field_value::FieldValueUnion::Text(t) => FieldValue::Text(t.into()),
        pb::field_value::FieldValueUnion::Integer(i) => FieldValue::Integer(i),
        pb::field_value::FieldValueUnion::Real(r) => FieldValue::Real(r.into()),
        pb::field_value::FieldValueUnion::Blob(b) => FieldValue::Blob(b.into()),
    }
}
