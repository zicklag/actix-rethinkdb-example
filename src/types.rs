use serde::*;

#[derive(Serialize, Deserialize, Debug)]
struct Teapot {
    name: String,
    capacity: i32,
    short_and_stout: bool,
}

#[derive(Serialize, Deserialize, Debug)]
struct TeapotPatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    capacity: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    short_and_stout: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug)]
struct TeapotCreateRes {
    id: String,
}
