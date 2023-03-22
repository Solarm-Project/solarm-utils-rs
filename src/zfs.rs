use derive_builder::{Builder, UninitializedFieldError};
use getset::Getters;
use miette::Diagnostic;
use std::{collections::HashMap, fmt::Display, str::FromStr};
use thiserror::Error;

#[cfg(not(test))]
use std::process::Command;

#[doc = "Error type for All zfs related builders"]
#[derive(Debug, Error, Diagnostic)]
#[non_exhaustive]
pub enum ZfsBuilderError {
    // where `LoremBuilder` is the name of the builder struct
    /// Uninitialized field
    UninitializedField(&'static str),
    /// Custom validation error
    ValidationError(String),
}

impl From<String> for ZfsBuilderError {
    fn from(s: String) -> Self {
        Self::ValidationError(s)
    }
}
impl From<UninitializedFieldError> for ZfsBuilderError {
    fn from(value: UninitializedFieldError) -> Self {
        Self::UninitializedField(value.field_name())
    }
}
impl Display for ZfsBuilderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ZfsBuilderError::UninitializedField(value) => {
                write!(f, "field {} must be initialized", value)
            }
            ZfsBuilderError::ValidationError(s) => write!(f, "validation error: {}", s),
        }
    }
}

#[derive(Debug, Clone)]
struct ZfsProperties(HashMap<String, String>);

impl Default for ZfsProperties {
    fn default() -> Self {
        Self(HashMap::new())
    }
}

impl Into<Vec<String>> for ZfsProperties {
    fn into(self) -> Vec<String> {
        self.0
            .iter()
            .map(|(key, value)| format!("{}={}", key, value))
            .collect()
    }
}

/// A Request to either create a dataset or volume making command building trivial
#[derive(Debug, Builder)]
#[builder(build_fn(validate = "Self::validate", error = "ZfsBuilderError"))]
pub struct CreateRequest {
    /// Defines the name of the dataset
    #[builder(setter(into))]
    name: String,

    /// All the properties setable with -o
    #[builder(setter(custom), default)]
    properties: ZfsProperties,

    /// Set to true to create parent datasets
    #[builder(default)]
    recursive: bool,

    /// Humans size of the volume
    #[builder(setter(into, strip_option), default)]
    volsize: Option<String>,

    /// Blocksize of the volume defaults to 128KB
    #[builder(setter(into, strip_option), default)]
    blocksize: Option<i32>,

    /// Choose if to create the volume as sparse
    #[builder(default)]
    sparse: bool,
}

impl CreateRequestBuilder {
    pub fn add_property<S: ToString>(&mut self, key: S, value: S) -> &mut Self {
        if let Some(mut properties) = self.properties.clone() {
            properties.0.insert(key.to_string(), value.to_string());
            self.properties = Some(properties);
        } else {
            self.properties = Some(ZfsProperties(HashMap::from([(
                key.to_string(),
                value.to_string(),
            )])));
        }

        self
    }

    fn validate(&self) -> std::result::Result<(), String> {
        if let Some(name) = &self.name {
            if name.contains("@") {
                return Err("Invalid dataset name".to_string());
            }
        }

        Ok(())
    }
}

pub fn create(req: &CreateRequest) -> crate::Result<Dataset> {
    let props: Vec<String> = req.properties.clone().into();

    let mut args = vec![];

    if req.recursive {
        args.push(String::from("-p"));
    }

    if req.volsize.is_some() {
        if req.sparse {
            args.push(String::from("-s"));
        }

        if let Some(blocksize) = &req.blocksize {
            args.push(String::from("-b"));
            args.push(blocksize.to_string());
        }
    }

    for p in props {
        args.push(String::from("-o"));
        args.push(p);
    }

    if let Some(volsize) = &req.volsize {
        args.push(String::from("-V"));
        args.push(volsize.clone());
    }

    args.push(req.name.clone());

    zfs(ZfsCommand::Create, args).map(|_v| Dataset {
        name: req.name.clone(),
    })
}

#[derive(Debug, Clone, Builder)]
#[builder(build_fn(validate = "Self::validate", error = "ZfsBuilderError"))]
pub struct CloneRequest {
    /// Clone from this snapshot
    #[builder(setter(into))]
    snapshot: String,

    /// Target name
    #[builder(setter(into))]
    target: String,

    /// Set to true to create non existing parent datasets of the target
    #[builder(default)]
    create_parents: bool,

    #[builder(setter(custom), default)]
    properties: ZfsProperties,
}

impl CloneRequestBuilder {
    /// define a zfs property that the target dataset|volume should have
    // this property won't apply to the source
    pub fn add_property<S: ToString>(&mut self, key: S, value: S) -> &mut Self {
        if let Some(mut properties) = self.properties.clone() {
            properties.0.insert(key.to_string(), value.to_string());
            self.properties = Some(properties);
        } else {
            self.properties = Some(ZfsProperties(HashMap::from([(
                key.to_string(),
                value.to_string(),
            )])));
        }

        self
    }

    fn validate(&self) -> std::result::Result<(), String> {
        if let Some(name) = &self.target {
            if name.contains("@") {
                return Err("Invalid target name".to_string());
            }
        }

        if let Some(name) = &self.snapshot {
            if !name.contains("@") {
                return Err("Invalid snapshot name".to_string());
            }
        }

        Ok(())
    }
}

pub fn clone(req: &CloneRequest) -> crate::Result<Dataset> {
    let props: Vec<String> = req.properties.clone().into();

    let mut args = vec![];

    if req.create_parents {
        args.push(String::from("-p"));
    }

    for p in props {
        args.push(String::from("-o"));
        args.push(p);
    }

    args.push(req.snapshot.clone());
    args.push(req.target.clone());

    zfs(ZfsCommand::Clone, args).map(|_v| Dataset {
        name: req.target.clone(),
    })
}

pub fn open(name: &str) -> crate::Result<Dataset> {
    let ds_name = zfs(ZfsCommand::Open, &["-Ho", "name", name])?;
    Ok(Dataset { name: ds_name })
}

#[derive(Debug, Clone, Builder)]
#[builder(build_fn(validate = "Self::validate", error = "ZfsBuilderError"))]
pub struct SnapshotRequest {
    /// Name to give the snapshot
    #[builder(setter(into))]
    snapshot: String,

    /// Set to true to create snapshots on all child datasets
    #[builder(default)]
    recursive: bool,

    #[builder(setter(custom), default)]
    properties: ZfsProperties,
}

impl SnapshotRequestBuilder {
    /// define a zfs property that the target dataset|volume should have
    // this property won't apply to the source
    pub fn add_property<S: ToString>(&mut self, key: S, value: S) -> &mut Self {
        if let Some(mut properties) = self.properties.clone() {
            properties.0.insert(key.to_string(), value.to_string());
            self.properties = Some(properties);
        } else {
            self.properties = Some(ZfsProperties(HashMap::from([(
                key.to_string(),
                value.to_string(),
            )])));
        }

        self
    }

    fn validate(&self) -> std::result::Result<(), String> {
        if let Some(name) = &self.snapshot {
            if !name.contains("@") {
                return Err("Invalid snapshot name".to_string());
            }
        }

        Ok(())
    }
}

pub fn snapshot(req: &SnapshotRequest) -> crate::Result<Snapshot> {
    let props: Vec<String> = req.properties.clone().into();

    let mut args = vec![];

    if req.recursive {
        args.push(String::from("-r"));
    }

    for p in props {
        args.push(String::from("-o"));
        args.push(p);
    }

    args.push(req.snapshot.clone());

    zfs(ZfsCommand::Snapshot, args).map(|_v| Snapshot {
        name: req.snapshot.clone(),
    })
}

#[derive(Debug, Clone, Builder)]
#[builder(build_fn(error = "ZfsBuilderError"))]
pub struct ListRequest {
    /// Name to give the snapshot
    #[builder(setter(into, strip_option), default)]
    root: Option<String>,

    /// A List of types of children to display
    #[builder(setter(custom), default)]
    list_types: Vec<ListType>,

    #[builder(setter(into, strip_option), default)]
    recursion_depth: Option<String>,

    /// Set to true to create snapshots on all child datasets
    #[builder(default)]
    recursive: bool,

    #[builder(setter(custom), default)]
    properties: ZfsProperties,
}

impl ListRequestBuilder {
    /// define a zfs property that the target dataset|volume should have
    // this property won't apply to the source
    pub fn add_property<S: ToString>(&mut self, key: S, value: S) -> &mut Self {
        if let Some(mut properties) = self.properties.clone() {
            properties.0.insert(key.to_string(), value.to_string());
            self.properties = Some(properties);
        } else {
            self.properties = Some(ZfsProperties(HashMap::from([(
                key.to_string(),
                value.to_string(),
            )])));
        }

        self
    }

    pub fn add_list_option<L: Into<ListType>>(&mut self, opt: L) -> &mut Self {
        if let Some(mut list) = self.list_types.clone() {
            list.push(opt.into());
            self.list_types = Some(list);
        } else {
            self.list_types = Some(vec![opt.into()]);
        }

        self
    }
}

#[derive(Debug, Clone)]
pub enum ListType {
    FileSystem,
    Snapshot,
    Volume,
    Bookmark,
    All,
}

impl FromStr for ListType {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "filesystem" => Ok(Self::FileSystem),
            "snapshot" => Ok(Self::Snapshot),
            "volume" => Ok(Self::Volume),
            "bookmark" => Ok(Self::Bookmark),
            "all" => Ok(Self::All),
            x => Err(crate::Error::InvalidZfsListType(x.to_string())),
        }
    }
}

impl Into<String> for ListType {
    fn into(self) -> String {
        String::from(match self {
            ListType::FileSystem => "filesystem",
            ListType::Snapshot => "snapshot",
            ListType::Volume => "volume",
            ListType::Bookmark => "bookmark",
            ListType::All => "all",
        })
    }
}

pub fn list(req: &ListRequest) -> crate::Result<Vec<Vec<String>>> {
    let props: Vec<String> = req.properties.clone().into();

    let mut args = vec![];

    if req.recursive {
        args.push(String::from("-r"));
        if let Some(depth) = &req.recursion_depth {
            args.push(String::from("-d"));
            args.push(depth.clone());
        }
    }

    args.push(String::from("-Hp"));

    for p in props {
        args.push(String::from("-o"));
        args.push(p);
    }

    if req.list_types.len() > 0 {
        args.push(String::from("-t"));
        args.push(
            req.list_types
                .iter()
                .map(|l| l.clone().into())
                .collect::<Vec<String>>()
                .join(","),
        );
    }

    if let Some(root) = &req.root {
        args.push(root.clone());
    }

    zfs(ZfsCommand::List, args).map(|v| {
        v.lines()
            .into_iter()
            .map(|l| {
                l.split_whitespace()
                    .map(|str| str.to_string())
                    .collect::<Vec<String>>()
            })
            .collect()
    })
}

enum ZfsCommand {
    Create,
    Clone,
    Promote,
    List,
    Open,
    Set,
    Get,
    Snapshot,
    Destroy,
}

#[cfg(not(test))]
fn zfs<I>(zfs_cmd: ZfsCommand, args: I) -> crate::Result<String>
where
    I: IntoIterator,
    I::Item: ToString,
{
    let mut cmd = Command::new("zfs");
    match zfs_cmd {
        ZfsCommand::Create => cmd.arg("create"),
        ZfsCommand::Clone => cmd.arg("clone"),
        ZfsCommand::Destroy => cmd.arg("destroy"),
        ZfsCommand::Promote => cmd.arg("promote"),
        ZfsCommand::List => cmd.arg("list"),
        ZfsCommand::Open => cmd.arg("list"),
        ZfsCommand::Set => cmd.arg("set"),
        ZfsCommand::Get => cmd.arg("get"),
        ZfsCommand::Snapshot => cmd.arg("snapshot"),
    };

    for arg in args {
        cmd.arg(arg.to_string().as_str());
    }

    cmd.env_clear();
    let output = cmd.output()?;

    if !output.status.success() {
        Err(crate::Error::ZFSError(String::from_utf8(output.stderr)?))
    } else {
        Ok(String::from_utf8(output.stdout)?)
    }
}

#[cfg(test)]
fn zfs<I>(zfs_cmd: ZfsCommand, args: I) -> crate::Result<String>
where
    I: IntoIterator,
    I::Item: ToString,
{
    match zfs_cmd {
        ZfsCommand::Create => Ok(String::new()),
        ZfsCommand::Clone => Ok(String::new()),
        ZfsCommand::Promote => Ok(String::new()),
        ZfsCommand::List => Ok(String::new()),
        ZfsCommand::Destroy => Ok(String::new()),
        ZfsCommand::Open => Ok(args.into_iter().last().unwrap().to_string()),
        ZfsCommand::Set => Ok(String::new()),
        ZfsCommand::Get => Ok(String::from("test_value")),
        ZfsCommand::Snapshot => Ok(String::new()),
    }
}

#[derive(Getters, Debug, Clone)]
pub struct Dataset {
    #[getset(get = "pub")]
    name: String,
}

impl Dataset {
    pub fn get(&self, name: &str) -> crate::Result<String> {
        zfs(ZfsCommand::Get, &["-H", "-o", "value", name, &self.name])
    }

    pub fn set(&self, name: &str, value: &str) -> crate::Result<()> {
        let val_arg = format!("{}={}", name, value);
        zfs(ZfsCommand::Set, &[val_arg.as_str(), &self.name]).map(|_v| ())
    }

    pub fn promote(&self) -> crate::Result<Dataset> {
        zfs(ZfsCommand::Promote, &[self.name.as_str()]).map(|_v| Dataset {
            name: self.name.clone(),
        })
    }

    pub fn destroy(&self) -> crate::Result<()> {
        zfs(ZfsCommand::Destroy, &[self.name.as_str()]).map(|_v| ())
    }

    pub fn snapshot(&self, name: &str) -> crate::Result<Snapshot> {
        snapshot(
            &SnapshotRequestBuilder::default()
                .snapshot(&format!("{}@{}", &self.name, name))
                .build()?,
        )
    }
}

#[derive(Getters, Debug, Clone)]
pub struct Snapshot {
    #[getset(get = "pub")]
    name: String,
}

impl Snapshot {
    pub fn get(&self, name: &str) -> crate::Result<String> {
        zfs(ZfsCommand::Get, &["-H", "-o", "value", name, &self.name])
    }

    pub fn set(&self, name: &str, value: &str) -> crate::Result<()> {
        let val_arg = format!("{}={}", name, value);
        zfs(ZfsCommand::Set, &[val_arg.as_str(), &self.name]).map(|_v| ())
    }

    pub fn destroy(&self) -> crate::Result<()> {
        zfs(ZfsCommand::Destroy, &[self.name.as_str()]).map(|_v| ())
    }
}
