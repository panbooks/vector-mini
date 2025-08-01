use std::{future::ready, pin::Pin};

use futures::{stream, Stream, StreamExt};
use mlua::ExternalError;
use mlua::FromLua;
use ordered_float::NotNan;
use snafu::{ResultExt, Snafu};
use vector_lib::configurable::configurable_component;
use vrl::path::parse_target_path;

use crate::config::OutputId;
use crate::schema::Definition;
use crate::{
    config::{DataType, Input, TransformOutput},
    event::{Event, Value},
    internal_events::{LuaGcTriggered, LuaScriptError},
    schema,
    transforms::{TaskTransform, Transform},
};

#[derive(Debug, Snafu)]
enum BuildError {
    #[snafu(display("Lua error: {}", source))]
    InvalidLua { source: mlua::Error },
}

/// Configuration for version one of the `lua` transform.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct LuaConfig {
    /// The Lua program to execute for each event.
    source: String,

    /// A list of directories to search when loading a Lua file via the `require` function.
    ///
    /// If not specified, the modules are looked up in the configuration directories.
    #[serde(default)]
    search_dirs: Vec<String>,
}

impl LuaConfig {
    pub fn build(&self) -> crate::Result<Transform> {
        warn!(
            "DEPRECATED The `lua` transform API version 1 is deprecated. Please convert your script to version 2."
        );
        Lua::new(self.source.clone(), self.search_dirs.clone()).map(Transform::event_task)
    }

    pub fn input(&self) -> Input {
        Input::log()
    }

    pub fn outputs(
        &self,
        input_definitions: &[(OutputId, schema::Definition)],
    ) -> Vec<TransformOutput> {
        // Lua causes the type definition to be reset
        let namespaces = input_definitions
            .iter()
            .flat_map(|(_output, definition)| definition.log_namespaces().clone())
            .collect();

        let definition = input_definitions
            .iter()
            .map(|(output, _definition)| {
                (
                    output.clone(),
                    Definition::default_for_namespace(&namespaces),
                )
            })
            .collect();

        vec![TransformOutput::new(DataType::Log, definition)]
    }
}

// Lua's garbage collector sometimes seems to be not executed automatically on high event rates,
// which leads to leak-like RAM consumption pattern. This constant sets the number of invocations of
// the Lua transform after which GC would be called, thus ensuring that the RAM usage is not too high.
//
// This constant is larger than 1 because calling GC is an expensive operation, so doing it
// after each transform would have significant footprint on the performance.
const GC_INTERVAL: usize = 16;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct Lua {
    #[derivative(Debug = "ignore")]
    source: String,
    #[derivative(Debug = "ignore")]
    search_dirs: Vec<String>,
    #[derivative(Debug = "ignore")]
    lua: mlua::Lua,
    vector_func: mlua::RegistryKey,
    invocations_after_gc: usize,
}

impl Clone for Lua {
    fn clone(&self) -> Self {
        Lua::new(self.source.clone(), self.search_dirs.clone())
            .expect("Tried to clone existing valid lua transform. This is an invariant.")
    }
}

// This wrapping structure is added in order to make it possible to have independent implementations
// of `mlua::UserData` trait for event in version 1 and version 2 of the transform.
#[derive(Clone, FromLua)]
struct LuaEvent {
    inner: Event,
}

impl Lua {
    pub fn new(source: String, search_dirs: Vec<String>) -> crate::Result<Self> {
        // In order to support loading C modules in Lua, we need to create unsafe instance
        // without debug library.
        let lua = unsafe {
            mlua::Lua::unsafe_new_with(mlua::StdLib::ALL_SAFE, mlua::LuaOptions::default())
        };

        let additional_paths = search_dirs
            .iter()
            .map(|d| format!("{d}/?.lua"))
            .collect::<Vec<_>>()
            .join(";");

        if !additional_paths.is_empty() {
            let package = lua
                .globals()
                .get::<mlua::Table>("package")
                .context(InvalidLuaSnafu)?;
            let current_paths = package
                .get::<String>("path")
                .unwrap_or_else(|_| ";".to_string());
            let paths = format!("{additional_paths};{current_paths}");
            package.set("path", paths).context(InvalidLuaSnafu)?;
        }

        let func = lua.load(&source).into_function().context(InvalidLuaSnafu)?;
        let vector_func = lua.create_registry_value(func).context(InvalidLuaSnafu)?;

        Ok(Self {
            source,
            search_dirs,
            lua,
            vector_func,
            invocations_after_gc: 0,
        })
    }

    fn process(&mut self, event: Event) -> Result<Option<Event>, mlua::Error> {
        let source_id = event.source_id().cloned();
        let lua = &self.lua;
        let globals = lua.globals();

        globals.raw_set("event", LuaEvent { inner: event })?;

        let func = lua.registry_value::<mlua::Function>(&self.vector_func)?;
        func.call::<()>(())?;

        let result = globals.raw_get::<Option<LuaEvent>>("event").map(|option| {
            option.map(|lua_event| {
                let mut event = lua_event.inner;
                if let Some(source_id) = source_id {
                    event.set_source_id(source_id);
                }
                event
            })
        });

        self.invocations_after_gc += 1;
        if self.invocations_after_gc % GC_INTERVAL == 0 {
            emit!(LuaGcTriggered {
                used_memory: self.lua.used_memory()
            });
            self.lua.gc_collect()?;
            self.invocations_after_gc = 0;
        }

        result
    }

    pub fn transform_one(&mut self, event: Event) -> Option<Event> {
        match self.process(event) {
            Ok(event) => event,
            Err(error) => {
                emit!(LuaScriptError { error });
                None
            }
        }
    }
}

impl TaskTransform<Event> for Lua {
    fn transform(
        self: Box<Self>,
        task: Pin<Box<dyn Stream<Item = Event> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = Event> + Send>>
    where
        Self: 'static,
    {
        let mut inner = self;
        Box::pin(
            task.filter_map(move |event| {
                let mut output = Vec::with_capacity(1);
                ready(match inner.process(event) {
                    Ok(event) => {
                        output.extend(event);
                        Some(stream::iter(output))
                    }
                    Err(error) => {
                        emit!(LuaScriptError { error });
                        None
                    }
                })
            })
            .flatten(),
        )
    }
}

impl mlua::UserData for LuaEvent {
    fn add_methods<M: mlua::UserDataMethods<Self>>(methods: &mut M) {
        methods.add_meta_method_mut(
            mlua::MetaMethod::NewIndex,
            |_lua, this, (key, value): (String, Option<mlua::Value>)| {
                let key_path = parse_target_path(key.as_str()).map_err(|e| e.into_lua_err())?;
                match value {
                    Some(mlua::Value::String(string)) => {
                        this.inner.as_mut_log().insert(
                            &key_path,
                            Value::from(string.to_str().expect("Expected UTF-8.").to_owned()),
                        );
                    }
                    Some(mlua::Value::Integer(integer)) => {
                        this.inner
                            .as_mut_log()
                            .insert(&key_path, Value::Integer(integer));
                    }
                    Some(mlua::Value::Number(number)) if !number.is_nan() => {
                        this.inner
                            .as_mut_log()
                            .insert(&key_path, Value::Float(NotNan::new(number).unwrap()));
                    }
                    Some(mlua::Value::Boolean(boolean)) => {
                        this.inner
                            .as_mut_log()
                            .insert(&key_path, Value::Boolean(boolean));
                    }
                    Some(mlua::Value::Nil) | None => {
                        this.inner.as_mut_log().remove(&key_path);
                    }
                    _ => {
                        info!(
                            message =
                                "Could not set field to Lua value of invalid type, dropping field.",
                            field = key.as_str(),
                        );
                        this.inner.as_mut_log().remove(&key_path);
                    }
                }

                Ok(())
            },
        );

        methods.add_meta_method(mlua::MetaMethod::Index, |lua, this, key: String| {
            if let Some(value) = this
                .inner
                .as_log()
                .parse_path_and_get_value(key.as_str())
                .ok()
                .flatten()
            {
                let string = lua.create_string(value.coerce_to_bytes())?;
                Ok(Some(string))
            } else {
                Ok(None)
            }
        });

        methods.add_meta_function(mlua::MetaMethod::Pairs, |lua, event: LuaEvent| {
            let state = lua.create_table()?;
            {
                if let Some(keys) = event.inner.as_log().keys() {
                    let keys = lua.create_table_from(keys.map(|k| (k, true)))?;
                    state.raw_set("keys", keys)?;
                }
                state.raw_set("event", event)?;
            }
            let function =
                lua.create_function(|lua, (state, prev): (mlua::Table, Option<String>)| {
                    let event: LuaEvent = state.raw_get("event")?;
                    let keys: mlua::Table = state.raw_get("keys")?;
                    let next: mlua::Function = lua.globals().raw_get("next")?;
                    let key: Option<String> = next.call((keys, prev))?;
                    let value = key.clone().and_then(|k| {
                        event
                            .inner
                            .as_log()
                            .parse_path_and_get_value(k.as_str())
                            .ok()
                            .flatten()
                    });
                    match value {
                        Some(value) => Ok((key, Some(lua.create_string(value.coerce_to_bytes())?))),
                        None => Ok((None, None)),
                    }
                })?;
            Ok((function, state))
        });
    }
}

pub fn format_error(error: &mlua::Error) -> String {
    match error {
        mlua::Error::CallbackError { traceback, cause } => format_error(cause) + "\n" + traceback,
        err => err.to_string(),
    }
}
