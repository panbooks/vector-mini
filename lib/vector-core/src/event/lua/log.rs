use mlua::prelude::*;

use super::super::{EventMetadata, LogEvent, Value};

impl IntoLua for LogEvent {
    #![allow(clippy::wrong_self_convention)] // this trait is defined by mlua
    fn into_lua(self, lua: &Lua) -> LuaResult<LuaValue> {
        let (value, _metadata) = self.into_parts();
        value.into_lua(lua)
    }
}

impl FromLua for LogEvent {
    fn from_lua(lua_value: LuaValue, lua: &Lua) -> LuaResult<Self> {
        let value = Value::from_lua(lua_value, lua)?;
        Ok(LogEvent::from_parts(value, EventMetadata::default()))
    }
}

