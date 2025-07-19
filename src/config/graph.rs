use super::{
    schema, ComponentKey, DataType, OutputId, SinkOuter, SourceOuter, SourceOutput, TransformOuter,
    TransformOutput, WildcardMatching,
};
use indexmap::{set::IndexSet, IndexMap};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;

#[derive(Debug, Clone)]
pub enum Node {
    Source {
        outputs: Vec<SourceOutput>,
    },
    Transform {
        in_ty: DataType,
        outputs: Vec<TransformOutput>,
    },
    Sink {
        ty: DataType,
    },
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Node::Source { outputs } => {
                write!(f, "component_kind: source\n  outputs:")?;
                for output in outputs {
                    write!(f, "\n    {output}")?;
                }
                Ok(())
            }
            Node::Transform { in_ty, outputs } => {
                write!(
                    f,
                    "component_kind: source\n  input_types: {in_ty}\n  outputs:"
                )?;
                for output in outputs {
                    write!(f, "\n    {output}")?;
                }
                Ok(())
            }
            Node::Sink { ty } => {
                write!(f, "component_kind: sink\n  types: {ty}")
            }
        }
    }
}

#[derive(Debug, Clone)]
struct Edge {
    from: OutputId,
    to: ComponentKey,
}

#[derive(Default)]
pub struct Graph {
    nodes: HashMap<ComponentKey, Node>,
    edges: Vec<Edge>,
}

impl Graph {
    pub fn new(
        sources: &IndexMap<ComponentKey, SourceOuter>,
        transforms: &IndexMap<ComponentKey, TransformOuter<String>>,
        sinks: &IndexMap<ComponentKey, SinkOuter<String>>,
        schema: schema::Options,
        wildcard_matching: WildcardMatching,
    ) -> Result<Self, Vec<String>> {
        Self::new_inner(sources, transforms, sinks, false, schema, wildcard_matching)
    }

    pub fn new_unchecked(
        sources: &IndexMap<ComponentKey, SourceOuter>,
        transforms: &IndexMap<ComponentKey, TransformOuter<String>>,
        sinks: &IndexMap<ComponentKey, SinkOuter<String>>,
        schema: schema::Options,
        wildcard_matching: WildcardMatching,
    ) -> Self {
        Self::new_inner(sources, transforms, sinks, true, schema, wildcard_matching)
            .expect("errors ignored")
    }

    fn new_inner(
        sources: &IndexMap<ComponentKey, SourceOuter>,
        transforms: &IndexMap<ComponentKey, TransformOuter<String>>,
        sinks: &IndexMap<ComponentKey, SinkOuter<String>>,
        ignore_errors: bool,
        schema: schema::Options,
        wildcard_matching: WildcardMatching,
    ) -> Result<Self, Vec<String>> {
        let mut graph = Graph::default();
        let mut errors = Vec::new();

        // First, insert all of the different node types
        for (id, config) in sources.iter() {
            graph.nodes.insert(
                id.clone(),
                Node::Source {
                    outputs: config.inner.outputs(schema.log_namespace()),
                },
            );
        }

        for (id, transform) in transforms.iter() {
            graph.nodes.insert(
                id.clone(),
                Node::Transform {
                    in_ty: transform.inner.input().data_type(),
                    outputs: transform.inner.outputs(
                        vector_lib::enrichment::TableRegistry::default(),
                        &[(id.into(), schema::Definition::any())],
                        schema.log_namespace(),
                    ),
                },
            );
        }

        for (id, config) in sinks {
            graph.nodes.insert(
                id.clone(),
                Node::Sink {
                    ty: config.inner.input().data_type(),
                },
            );
        }

        // With all of the nodes added, go through inputs and add edges, resolving strings into
        // actual `OutputId`s along the way.
        let available_inputs = graph.input_map()?;

        for (id, config) in transforms.iter() {
            for input in config.inputs.iter() {
                if let Err(e) = graph.add_input(input, id, &available_inputs, wildcard_matching) {
                    errors.push(e);
                }
            }
        }

        for (id, config) in sinks {
            for input in config.inputs.iter() {
                if let Err(e) = graph.add_input(input, id, &available_inputs, wildcard_matching) {
                    errors.push(e);
                }
            }
        }

        if ignore_errors || errors.is_empty() {
            Ok(graph)
        } else {
            Err(errors)
        }
    }

    fn add_input(
        &mut self,
        from: &str,
        to: &ComponentKey,
        available_inputs: &HashMap<String, OutputId>,
        wildcard_matching: WildcardMatching,
    ) -> Result<(), String> {
        if let Some(output_id) = available_inputs.get(from) {
            self.edges.push(Edge {
                from: output_id.clone(),
                to: to.clone(),
            });
            Ok(())
        } else {
            let output_type = match self.nodes.get(to) {
                Some(Node::Transform { .. }) => "transform",
                Some(Node::Sink { .. }) => "sink",
                _ => panic!("only transforms and sinks have inputs"),
            };
            // allow empty result if relaxed wildcard matching is enabled
            match wildcard_matching {
                WildcardMatching::Relaxed => {
                    // using value != glob::Pattern::escape(value) to check if value is a glob
                    // TODO: replace with proper check when https://github.com/rust-lang/glob/issues/72 is resolved
                    if from != glob::Pattern::escape(from) {
                        info!("Input \"{from}\" for {output_type} \"{to}\" didn’t match any components, but this was ignored because `relaxed_wildcard_matching` is enabled.");
                        return Ok(());
                    }
                }
                WildcardMatching::Strict => {}
            }
            info!(
                "Available components:\n{}",
                self.nodes
                    .iter()
                    .map(|(key, node)| format!("\"{key}\":\n  {node}"))
                    .collect::<Vec<_>>()
                    .join("\n")
            );
            Err(format!(
                "Input \"{from}\" for {output_type} \"{to}\" doesn't match any components.",
            ))
        }
    }

    /// Return the input type of a given component.
    ///
    /// # Panics
    ///
    /// Will panic if the given key is not present in the graph or identifies a source, which can't
    /// have inputs.
    fn get_input_type(&self, key: &ComponentKey) -> DataType {
        match self.nodes[key] {
            Node::Source { .. } => panic!("no inputs on sources"),
            Node::Transform { in_ty, .. } => in_ty,
            Node::Sink { ty } => ty,
        }
    }

    /// Return the output type associated with a given `OutputId`.
    ///
    /// # Panics
    ///
    /// Will panic if the given id is not present in the graph or identifies a sink, which can't
    /// have inputs.
    fn get_output_type(&self, id: &OutputId) -> DataType {
        match &self.nodes[&id.component] {
            Node::Source { outputs } => outputs
                .iter()
                .find(|output| output.port == id.port)
                .map(|output| output.ty)
                .expect("output didn't exist"),
            Node::Transform { outputs, .. } => outputs
                .iter()
                .find(|output| output.port == id.port)
                .map(|output| output.ty)
                .expect("output didn't exist"),
            Node::Sink { .. } => panic!("no outputs on sinks"),
        }
    }

    pub fn typecheck(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        // check that all edges connect components with compatible data types
        for edge in &self.edges {
            let from_ty = self.get_output_type(&edge.from);
            let to_ty = self.get_input_type(&edge.to);

            if !from_ty.intersects(to_ty) {
                errors.push(format!(
                    "Data type mismatch between {} ({}) and {} ({})",
                    edge.from, from_ty, edge.to, to_ty
                ));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            errors.sort();
            errors.dedup();
            Err(errors)
        }
    }

    pub fn check_for_cycles(&self) -> Result<(), String> {
        // find all sinks
        let sinks = self.nodes.iter().filter_map(|(name, node)| match node {
            Node::Sink { .. } => Some(name),
            _ => None,
        });

        // run DFS from each sink while keep tracking the current stack to detect cycles
        for s in sinks {
            let mut traversal: VecDeque<ComponentKey> = VecDeque::new();
            let mut visited: HashSet<ComponentKey> = HashSet::new();
            let mut stack: IndexSet<ComponentKey> = IndexSet::new();

            traversal.push_back(s.to_owned());
            while !traversal.is_empty() {
                let n = traversal.back().expect("can't be empty").clone();
                if !visited.contains(&n) {
                    visited.insert(n.clone());
                    stack.insert(n.clone());
                } else {
                    // we came back to the node after exploring all its children - remove it from the stack and traversal
                    stack.shift_remove(&n);
                    traversal.pop_back();
                }
                let inputs = self
                    .edges
                    .iter()
                    .filter(|e| e.to == n)
                    .map(|e| e.from.clone());
                for input in inputs {
                    if !visited.contains(&input.component) {
                        traversal.push_back(input.component);
                    } else if stack.contains(&input.component) {
                        // we reached the node while it is on the current stack - it's a cycle
                        let path = stack
                            .iter()
                            .skip(1) // skip the sink
                            .rev()
                            .map(|item| item.to_string())
                            .collect::<Vec<_>>();
                        return Err(format!(
                            "Cyclic dependency detected in the chain [ {} -> {} ]",
                            input.component.id(),
                            path.join(" -> ")
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn valid_inputs(&self) -> HashSet<OutputId> {
        self.nodes
            .iter()
            .flat_map(|(key, node)| match node {
                Node::Sink { .. } => vec![],
                Node::Source { outputs } => outputs
                    .iter()
                    .map(|output| OutputId {
                        component: key.clone(),
                        port: output.port.clone(),
                    })
                    .collect(),
                Node::Transform { outputs, .. } => outputs
                    .iter()
                    .map(|output| OutputId {
                        component: key.clone(),
                        port: output.port.clone(),
                    })
                    .collect(),
            })
            .collect()
    }

    /// Produce a map of output IDs for the current set of nodes in the graph, keyed by their string
    /// representation. Returns errors for any nodes that have the same string representation,
    /// making input specifications ambiguous.
    ///
    /// When we get a dotted path in the `inputs` section of a user's config, we need to determine
    /// which of a few things that represents:
    ///
    ///   1. A component that's part of an expanded macro (e.g. `route.branch`)
    ///   2. A named output of a branching transform (e.g. `name.errors`)
    ///
    /// A naive way to do that is to compare the string representation of all valid inputs to the
    /// provided string and pick the one that matches. This works better if you can assume that there
    /// are no conflicting string representations, so this function reports any ambiguity as an
    /// error when creating the lookup map.
    pub fn input_map(&self) -> Result<HashMap<String, OutputId>, Vec<String>> {
        let mut mapped: HashMap<String, OutputId> = HashMap::new();
        let mut errors = HashSet::new();

        for id in self.valid_inputs() {
            if let Some(_other) = mapped.insert(id.to_string(), id.clone()) {
                errors.insert(format!("Input specifier {id} is ambiguous"));
            }
        }

        if errors.is_empty() {
            Ok(mapped)
        } else {
            Err(errors.into_iter().collect())
        }
    }

    pub fn inputs_for(&self, node: &ComponentKey) -> Vec<OutputId> {
        self.edges
            .iter()
            .filter(|edge| &edge.to == node)
            .map(|edge| edge.from.clone())
            .collect()
    }

    /// From a given root node, get all paths from the root node to leaf nodes
    /// where the leaf node must be a sink. This is useful for determining which
    /// components are relevant in a Vector unit test.
    ///
    /// Caller must check for cycles before calling this function.
    pub fn paths_to_sink_from(&self, root: &ComponentKey) -> Vec<Vec<ComponentKey>> {
        let mut traversal: VecDeque<(ComponentKey, Vec<_>)> = VecDeque::new();
        let mut paths = Vec::new();

        traversal.push_back((root.to_owned(), Vec::new()));
        while !traversal.is_empty() {
            let (n, mut path) = traversal.pop_back().expect("can't be empty");
            path.push(n.clone());
            let neighbors = self
                .edges
                .iter()
                .filter(|e| e.from.component == n)
                .map(|e| e.to.clone())
                .collect::<Vec<_>>();

            if neighbors.is_empty() {
                paths.push(path.clone());
            } else {
                for neighbor in neighbors {
                    traversal.push_back((neighbor, path.clone()));
                }
            }
        }

        // Keep only components from paths that end at a sink
        paths
            .into_iter()
            .filter(|path| {
                if let Some(key) = path.last() {
                    matches!(self.nodes.get(key), Some(Node::Sink { ty: _ }))
                } else {
                    false
                }
            })
            .collect()
    }
}

