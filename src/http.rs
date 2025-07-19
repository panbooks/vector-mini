#![allow(missing_docs)]
use futures::future::BoxFuture;
use headers::{Authorization, HeaderMapExt};
use http::{
    header::HeaderValue, request::Builder, uri::InvalidUri, HeaderMap, Request, Response, Uri,
    Version,
};
use hyper::{
    body::{Body, HttpBody},
    client,
    client::{Client, HttpConnector},
};
use hyper_openssl::HttpsConnector;
use hyper_proxy::ProxyConnector;
use rand::Rng;
use serde_with::serde_as;
use snafu::{ResultExt, Snafu};
use std::{
    collections::HashMap,
    fmt,
    net::SocketAddr,
    task::{Context, Poll},
    time::Duration,
};
use tokio::time::Instant;
use tower::{Layer, Service};
use tower_http::{
    classify::{ServerErrorsAsFailures, SharedClassifier},
    trace::TraceLayer,
};
use tracing::{Instrument, Span};
use vector_lib::configurable::configurable_component;
use vector_lib::sensitive_string::SensitiveString;

#[cfg(feature = "aws-core")]
use crate::aws::AwsAuthentication;

use crate::{
    config::ProxyConfig,
    internal_events::{http_client, HttpServerRequestReceived, HttpServerResponseSent},
    tls::{tls_connector_builder, MaybeTlsSettings, TlsError},
};

pub mod status {
    pub const FORBIDDEN: u16 = 403;
    pub const NOT_FOUND: u16 = 404;
    pub const TOO_MANY_REQUESTS: u16 = 429;
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum HttpError {
    #[snafu(display("Failed to build TLS connector: {}", source))]
    BuildTlsConnector { source: TlsError },
    #[snafu(display("Failed to build HTTPS connector: {}", source))]
    MakeHttpsConnector { source: openssl::error::ErrorStack },
    #[snafu(display("Failed to build Proxy connector: {}", source))]
    MakeProxyConnector { source: InvalidUri },
    #[snafu(display("Failed to make HTTP(S) request: {}", source))]
    CallRequest { source: hyper::Error },
    #[snafu(display("Failed to build HTTP request: {}", source))]
    BuildRequest { source: http::Error },
}

impl HttpError {
    pub const fn is_retriable(&self) -> bool {
        match self {
            HttpError::BuildRequest { .. } | HttpError::MakeProxyConnector { .. } => false,
            HttpError::CallRequest { .. }
            | HttpError::BuildTlsConnector { .. }
            | HttpError::MakeHttpsConnector { .. } => true,
        }
    }
}

pub type HttpClientFuture = <HttpClient as Service<http::Request<Body>>>::Future;
type HttpProxyConnector = ProxyConnector<HttpsConnector<HttpConnector>>;

pub struct HttpClient<B = Body> {
    client: Client<HttpProxyConnector, B>,
    user_agent: HeaderValue,
    proxy_connector: HttpProxyConnector,
}

impl<B> HttpClient<B>
where
    B: fmt::Debug + HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<crate::Error>,
{
    pub fn new(
        tls_settings: impl Into<MaybeTlsSettings>,
        proxy_config: &ProxyConfig,
    ) -> Result<HttpClient<B>, HttpError> {
        HttpClient::new_with_custom_client(tls_settings, proxy_config, &mut Client::builder())
    }

    pub fn new_with_custom_client(
        tls_settings: impl Into<MaybeTlsSettings>,
        proxy_config: &ProxyConfig,
        client_builder: &mut client::Builder,
    ) -> Result<HttpClient<B>, HttpError> {
        let proxy_connector = build_proxy_connector(tls_settings.into(), proxy_config)?;
        let client = client_builder.build(proxy_connector.clone());

        let app_name = crate::get_app_name();
        let version = crate::get_version();
        let user_agent = HeaderValue::from_str(&format!("{app_name}/{version}"))
            .expect("Invalid header value for user-agent!");

        Ok(HttpClient {
            client,
            user_agent,
            proxy_connector,
        })
    }

    pub fn send(
        &self,
        mut request: Request<B>,
    ) -> BoxFuture<'static, Result<http::Response<Body>, HttpError>> {
        let span = tracing::info_span!("http");
        let _enter = span.enter();

        default_request_headers(&mut request, &self.user_agent);
        self.maybe_add_proxy_headers(&mut request);

        emit!(http_client::AboutToSendHttpRequest { request: &request });

        let response = self.client.request(request);

        let fut = async move {
            // Capture the time right before we issue the request.
            // Request doesn't start the processing until we start polling it.
            let before = std::time::Instant::now();

            // Send request and wait for the result.
            let response_result = response.await;

            // Compute the roundtrip time it took to send the request and get
            // the response or error.
            let roundtrip = before.elapsed();

            // Handle the errors and extract the response.
            let response = response_result
                .inspect_err(|error| {
                    // Emit the error into the internal events system.
                    emit!(http_client::GotHttpWarning { error, roundtrip });
                })
                .context(CallRequestSnafu)?;

            // Emit the response into the internal events system.
            emit!(http_client::GotHttpResponse {
                response: &response,
                roundtrip
            });
            Ok(response)
        }
        .instrument(span.clone().or_current());

        Box::pin(fut)
    }

    fn maybe_add_proxy_headers(&self, request: &mut Request<B>) {
        if let Some(proxy_headers) = self.proxy_connector.http_headers(request.uri()) {
            for (k, v) in proxy_headers {
                let request_headers = request.headers_mut();
                if !request_headers.contains_key(k) {
                    request_headers.insert(k, v.into());
                }
            }
        }
    }
}

pub fn build_proxy_connector(
    tls_settings: MaybeTlsSettings,
    proxy_config: &ProxyConfig,
) -> Result<ProxyConnector<HttpsConnector<HttpConnector>>, HttpError> {
    // Create dedicated TLS connector for the proxied connection with user TLS settings.
    let tls = tls_connector_builder(&tls_settings)
        .context(BuildTlsConnectorSnafu)?
        .build();
    let https = build_tls_connector(tls_settings)?;
    let mut proxy = ProxyConnector::new(https).unwrap();
    // Make proxy connector aware of user TLS settings by setting the TLS connector:
    // https://github.com/vectordotdev/vector/issues/13683
    proxy.set_tls(Some(tls));
    proxy_config
        .configure(&mut proxy)
        .context(MakeProxyConnectorSnafu)?;
    Ok(proxy)
}

pub fn build_tls_connector(
    tls_settings: MaybeTlsSettings,
) -> Result<HttpsConnector<HttpConnector>, HttpError> {
    let mut http = HttpConnector::new();
    http.enforce_http(false);

    let tls = tls_connector_builder(&tls_settings).context(BuildTlsConnectorSnafu)?;
    let mut https = HttpsConnector::with_connector(http, tls).context(MakeHttpsConnectorSnafu)?;

    let settings = tls_settings.tls().cloned();
    https.set_callback(move |c, _uri| {
        if let Some(settings) = &settings {
            settings.apply_connect_configuration(c)
        } else {
            Ok(())
        }
    });
    Ok(https)
}

fn default_request_headers<B>(request: &mut Request<B>, user_agent: &HeaderValue) {
    if !request.headers().contains_key("User-Agent") {
        request
            .headers_mut()
            .insert("User-Agent", user_agent.clone());
    }

    if !request.headers().contains_key("Accept-Encoding") {
        // hardcoding until we support compressed responses:
        // https://github.com/vectordotdev/vector/issues/5440
        request
            .headers_mut()
            .insert("Accept-Encoding", HeaderValue::from_static("identity"));
    }
}

impl<B> Service<Request<B>> for HttpClient<B>
where
    B: fmt::Debug + HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<crate::Error> + Send,
{
    type Response = http::Response<Body>;
    type Error = HttpError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        self.send(request)
    }
}

impl<B> Clone for HttpClient<B> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            user_agent: self.user_agent.clone(),
            proxy_connector: self.proxy_connector.clone(),
        }
    }
}

impl<B> fmt::Debug for HttpClient<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpClient")
            .field("client", &self.client)
            .field("user_agent", &self.user_agent)
            .finish()
    }
}

/// Configuration of the authentication strategy for HTTP requests.
///
/// HTTP authentication should be used with HTTPS only, as the authentication credentials are passed as an
/// HTTP header without any additional encryption beyond what is provided by the transport itself.
#[configurable_component]
#[derive(Clone, Debug, Eq, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "strategy")]
#[configurable(metadata(docs::enum_tag_description = "The authentication strategy to use."))]
pub enum Auth {
    /// Basic authentication.
    ///
    /// The username and password are concatenated and encoded via [base64][base64].
    ///
    /// [base64]: https://en.wikipedia.org/wiki/Base64
    Basic {
        /// The basic authentication username.
        #[configurable(metadata(docs::examples = "${USERNAME}"))]
        #[configurable(metadata(docs::examples = "username"))]
        user: String,

        /// The basic authentication password.
        #[configurable(metadata(docs::examples = "${PASSWORD}"))]
        #[configurable(metadata(docs::examples = "password"))]
        password: SensitiveString,
    },

    /// Bearer authentication.
    ///
    /// The bearer token value (OAuth2, JWT, etc.) is passed as-is.
    Bearer {
        /// The bearer authentication token.
        token: SensitiveString,
    },

    #[cfg(feature = "aws-core")]
    /// AWS authentication.
    Aws {
        /// The AWS authentication configuration.
        auth: AwsAuthentication,

        /// The AWS service name to use for signing.
        service: String,
    },
}

pub trait MaybeAuth: Sized {
    fn choose_one(&self, other: &Self) -> crate::Result<Self>;
}

impl MaybeAuth for Option<Auth> {
    fn choose_one(&self, other: &Self) -> crate::Result<Self> {
        if self.is_some() && other.is_some() {
            Err("Two authorization credentials was provided.".into())
        } else {
            Ok(self.clone().or_else(|| other.clone()))
        }
    }
}

impl Auth {
    pub fn apply<B>(&self, req: &mut Request<B>) {
        self.apply_headers_map(req.headers_mut())
    }

    pub fn apply_builder(&self, mut builder: Builder) -> Builder {
        if let Some(map) = builder.headers_mut() {
            self.apply_headers_map(map)
        }
        builder
    }

    pub fn apply_headers_map(&self, map: &mut HeaderMap) {
        match &self {
            Auth::Basic { user, password } => {
                let auth = Authorization::basic(user.as_str(), password.inner());
                map.typed_insert(auth);
            }
            Auth::Bearer { token } => match Authorization::bearer(token.inner()) {
                Ok(auth) => map.typed_insert(auth),
                Err(error) => error!(message = "Invalid bearer token.", token = %token, %error),
            },
            #[cfg(feature = "aws-core")]
            _ => {}
        }
    }
}

pub fn get_http_scheme_from_uri(uri: &Uri) -> &'static str {
    // If there's no scheme, we just use "http" since it provides the most semantic relevance without inadvertently
    // implying things it can't know i.e. returning "https" when we're not actually sure HTTPS was used.
    uri.scheme_str().map_or("http", |scheme| match scheme {
        "http" => "http",
        "https" => "https",
        // `http::Uri` ensures that we always get "http" or "https" if the URI is created with a well-formed scheme, but
        // it also supports arbitrary schemes, which is where we bomb out down here, since we can't generate a static
        // string for an arbitrary input string... and anything other than "http" and "https" makes no sense for an HTTP
        // client anyways.
        s => panic!("invalid URI scheme for HTTP client: {s}"),
    })
}

/// Builds a [TraceLayer] configured for a HTTP server.
///
/// This layer emits HTTP specific telemetry for requests received, responses sent, and handler duration.
pub fn build_http_trace_layer<T, U>(
    span: Span,
) -> TraceLayer<
    SharedClassifier<ServerErrorsAsFailures>,
    impl Fn(&Request<T>) -> Span + Clone,
    impl Fn(&Request<T>, &Span) + Clone,
    impl Fn(&Response<U>, Duration, &Span) + Clone,
    (),
    (),
    (),
> {
    TraceLayer::new_for_http()
        .make_span_with(move |request: &Request<T>| {
            // This is an error span so that the labels are always present for metrics.
            error_span!(
               parent: &span,
               "http-request",
               method = %request.method(),
               path = %request.uri().path(),
            )
        })
        .on_request(Box::new(|_request: &Request<T>, _span: &Span| {
            emit!(HttpServerRequestReceived);
        }))
        .on_response(|response: &Response<U>, latency: Duration, _span: &Span| {
            emit!(HttpServerResponseSent { response, latency });
        })
        .on_failure(())
        .on_body_chunk(())
        .on_eos(())
}

/// Configuration of HTTP server keepalive parameters.
#[serde_as]
#[configurable_component]
#[derive(Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct KeepaliveConfig {
    /// The maximum amount of time a connection may exist before it is closed by sending
    /// a `Connection: close` header on the HTTP response. Set this to a large value like
    /// `100000000` to "disable" this feature
    ///
    ///
    /// Only applies to HTTP/0.9, HTTP/1.0, and HTTP/1.1 requests.
    ///
    /// A random jitter configured by `max_connection_age_jitter_factor` is added
    /// to the specified duration to spread out connection storms.
    #[serde(default = "default_max_connection_age")]
    #[configurable(metadata(docs::examples = 600))]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    #[configurable(metadata(docs::human_name = "Maximum Connection Age"))]
    pub max_connection_age_secs: Option<u64>,

    /// The factor by which to jitter the `max_connection_age_secs` value.
    ///
    /// A value of 0.1 means that the actual duration will be between 90% and 110% of the
    /// specified maximum duration.
    #[serde(default = "default_max_connection_age_jitter_factor")]
    #[configurable(validation(range(min = 0.0, max = 1.0)))]
    pub max_connection_age_jitter_factor: f64,
}

const fn default_max_connection_age() -> Option<u64> {
    Some(300) // 5 minutes
}

const fn default_max_connection_age_jitter_factor() -> f64 {
    0.1
}

impl Default for KeepaliveConfig {
    fn default() -> Self {
        Self {
            max_connection_age_secs: default_max_connection_age(),
            max_connection_age_jitter_factor: default_max_connection_age_jitter_factor(),
        }
    }
}

/// A layer that limits the maximum duration of a client connection. It does so by adding a
/// `Connection: close` header to the response if `max_connection_duration` time has elapsed
/// since `start_reference`.
///
/// **Notes:**
/// - This is intended to be used in a Hyper server (or similar) that will automatically close
///   the connection after a response with a `Connection: close` header is sent.
/// - This layer assumes that it is instantiated once per connection, which is true within the
///   Hyper framework.
pub struct MaxConnectionAgeLayer {
    start_reference: Instant,
    max_connection_age: Duration,
    peer_addr: SocketAddr,
}

impl MaxConnectionAgeLayer {
    pub fn new(max_connection_age: Duration, jitter_factor: f64, peer_addr: SocketAddr) -> Self {
        Self {
            start_reference: Instant::now(),
            max_connection_age: Self::jittered_duration(max_connection_age, jitter_factor),
            peer_addr,
        }
    }

    fn jittered_duration(duration: Duration, jitter_factor: f64) -> Duration {
        // Ensure the jitter_factor is between 0.0 and 1.0
        let jitter_factor = jitter_factor.clamp(0.0, 1.0);
        // Generate a random jitter factor between `1 - jitter_factor`` and `1 + jitter_factor`.
        let mut rng = rand::rng();
        let random_jitter_factor = rng.random_range(-jitter_factor..=jitter_factor) + 1.;
        duration.mul_f64(random_jitter_factor)
    }
}

impl<S> Layer<S> for MaxConnectionAgeLayer
where
    S: Service<Request<Body>, Response = Response<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Service = MaxConnectionAgeService<S>;

    fn layer(&self, service: S) -> Self::Service {
        MaxConnectionAgeService {
            service,
            start_reference: self.start_reference,
            max_connection_age: self.max_connection_age,
            peer_addr: self.peer_addr,
        }
    }
}

/// A service that limits the maximum age of a client connection. It does so by adding a
/// `Connection: close` header to the response if `max_connection_age` time has elapsed
/// since `start_reference`.
///
/// **Notes:**
/// - This is intended to be used in a Hyper server (or similar) that will automatically close
///   the connection after a response with a `Connection: close` header is sent.
/// - This service assumes that it is instantiated once per connection, which is true within the
///   Hyper framework.
#[derive(Clone)]
pub struct MaxConnectionAgeService<S> {
    service: S,
    start_reference: Instant,
    max_connection_age: Duration,
    peer_addr: SocketAddr,
}

impl<S, E> Service<Request<Body>> for MaxConnectionAgeService<S>
where
    S: Service<Request<Body>, Response = Response<Body>, Error = E> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = E;
    type Future = BoxFuture<'static, Result<Self::Response, E>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let start_reference = self.start_reference;
        let max_connection_age = self.max_connection_age;
        let peer_addr = self.peer_addr;
        let version = req.version();
        let future = self.service.call(req);
        Box::pin(async move {
            let mut response = future.await?;
            match version {
                Version::HTTP_09 | Version::HTTP_10 | Version::HTTP_11 => {
                    if start_reference.elapsed() >= max_connection_age {
                        debug!(
                            message = "Closing connection due to max connection age.",
                            ?max_connection_age,
                            connection_age = ?start_reference.elapsed(),
                            ?peer_addr,
                        );
                        // Tell the client to close this connection.
                        // Hyper will automatically close the connection after the response is sent.
                        response.headers_mut().insert(
                            hyper::header::CONNECTION,
                            hyper::header::HeaderValue::from_static("close"),
                        );
                    }
                }
                // TODO need to send GOAWAY frame
                Version::HTTP_2 => (),
                // TODO need to send GOAWAY frame
                Version::HTTP_3 => (),
                _ => (),
            }
            Ok(response)
        })
    }
}

/// The type of a query parameter's value, determines if it's treated as a plain string or a VRL expression.
#[configurable_component]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ParamType {
    /// The parameter value is a plain string.
    #[default]
    String,
    /// The parameter value is a VRL expression that will be evaluated before each request.
    Vrl,
}

impl ParamType {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

/// Represents a query parameter value, which can be a simple string or a typed object
/// indicating whether the value is a string or a VRL expression.
#[configurable_component]
#[derive(Clone, Debug, Eq, PartialEq)]
#[serde(untagged)]
pub enum ParameterValue {
    /// A simple string value. For backwards compatibility.
    String(String),
    /// A value with an explicit type.
    Typed {
        /// The raw value of the parameter.
        value: String,
        /// The type of the parameter, indicating how the `value` should be treated.
        #[serde(
            default,
            skip_serializing_if = "ParamType::is_default",
            rename = "type"
        )]
        r#type: ParamType,
    },
}

impl ParameterValue {
    /// Returns true if the parameter is a VRL expression.
    pub const fn is_vrl(&self) -> bool {
        match self {
            ParameterValue::String(_) => false,
            ParameterValue::Typed { r#type, .. } => matches!(r#type, ParamType::Vrl),
        }
    }

    /// Returns the raw string value of the parameter.
    #[allow(clippy::missing_const_for_fn)]
    pub fn value(&self) -> &str {
        match self {
            ParameterValue::String(s) => s,
            ParameterValue::Typed { value, .. } => value,
        }
    }

    /// Consumes the `ParameterValue` and returns the owned raw string value.
    pub fn into_value(self) -> String {
        match self {
            ParameterValue::String(s) => s,
            ParameterValue::Typed { value, .. } => value,
        }
    }
}

/// Configuration of the query parameter value for HTTP requests.
#[configurable_component]
#[derive(Clone, Debug, Eq, PartialEq)]
#[serde(untagged)]
#[configurable(metadata(docs::enum_tag_description = "Query parameter value"))]
pub enum QueryParameterValue {
    /// Query parameter with single value
    SingleParam(ParameterValue),
    /// Query parameter with multiple values
    MultiParams(Vec<ParameterValue>),
}

impl QueryParameterValue {
    /// Returns an iterator over the contained `ParameterValue`s.
    pub fn iter(&self) -> impl Iterator<Item = &ParameterValue> {
        match self {
            QueryParameterValue::SingleParam(param) => std::slice::from_ref(param).iter(),
            QueryParameterValue::MultiParams(params) => params.iter(),
        }
    }

    /// Convert to `Vec<ParameterValue>` for owned iteration.
    fn into_vec(self) -> Vec<ParameterValue> {
        match self {
            QueryParameterValue::SingleParam(param) => vec![param],
            QueryParameterValue::MultiParams(params) => params,
        }
    }
}

// Implement IntoIterator for owned QueryParameterValue
impl IntoIterator for QueryParameterValue {
    type Item = ParameterValue;
    type IntoIter = std::vec::IntoIter<ParameterValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.into_vec().into_iter()
    }
}

pub type QueryParameters = HashMap<String, QueryParameterValue>;

