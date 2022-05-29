use crate::{
    error::Error,
    object_storage::Storage,
};

pub fn handler(storage_path: String, key: String) -> Result<(), Error>
{
    if let Some(value) =
        Storage::new(storage_path.as_str())?.get_property(key.as_str())?
    {
        println!("{:?}", value);
    }
    Ok(())
}
