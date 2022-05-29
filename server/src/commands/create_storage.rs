use crate::{
    error::Error,
    object_storage::{
        CreateOptions,
        Storage,
    },
};

pub fn handler(storage_path: String, name: String) -> Result<(), Error>
{
    let _ = Storage::create(
        storage_path.as_str(),
        CreateOptions {
            name,
            ..Default::default()
        },
    )?;

    Ok(())
}
