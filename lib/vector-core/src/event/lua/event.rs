use mlua::prelude::*;

use super::super::{Event, LogEvent, Metric};
use super::metric::LuaMetric;

pub struct LuaEvent {
    pub event: Event,
    pub metric_multi_value_tags: bool,
}

impl IntoLua for LuaEvent {
    #![allow(clippy::wrong_self_convention)] // this trait is defined by mlua
    fn into_lua(self, lua: &Lua) -> LuaResult<LuaValue> {
        let table = lua.create_table()?;
        match self.event {
            Event::Log(log) => table.raw_set("log", log.into_lua(lua)?)?,
            Event::Metric(metric) => table.raw_set(
                "metric",
                LuaMetric {
                    metric,
                    multi_value_tags: self.metric_multi_value_tags,
                }
                .into_lua(lua)?,
            )?,
            Event::Trace(_) => {
                return Err(LuaError::ToLuaConversionError {
                    from: String::from("Event"),
                    to: "table",
                    message: Some("Trace are not supported".to_string()),
                })
            }
        }
        Ok(LuaValue::Table(table))
    }
}

impl FromLua for Event {
    fn from_lua(value: LuaValue, lua: &Lua) -> LuaResult<Self> {
        let LuaValue::Table(table) = &value else {
            return Err(LuaError::FromLuaConversionError {
                from: value.type_name(),
                to: String::from("Event"),
                message: Some("Event should be a Lua table".to_string()),
            });
        };
        match (table.raw_get("log")?, table.raw_get("metric")?) {
            (LuaValue::Table(log), LuaValue::Nil) => {
                Ok(Event::Log(LogEvent::from_lua(LuaValue::Table(log), lua)?))
            }
            (LuaValue::Nil, LuaValue::Table(metric)) => Ok(Event::Metric(Metric::from_lua(
                LuaValue::Table(metric),
                lua,
            )?)),
            _ => Err(LuaError::FromLuaConversionError {
                from: value.type_name(),
                to: String::from("Event"),
                message: Some(
                    "Event should contain either \"log\" or \"metric\" key at the top level"
                        .to_string(),
                ),
            }),
        }
    }
}

