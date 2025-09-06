//! Warp implementation of the WebServer trait

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use warp::{Filter, Rejection, Reply, filters::BoxedFilter};
use obzenflow_core::web::{
    HttpEndpoint, HttpMethod, Request, ServerConfig, WebError, WebServer,
    server::ServerShutdownHandle,
};

/// Warp-based web server implementation
pub struct WarpServer {
    endpoints: Vec<Arc<dyn HttpEndpoint>>,
}

impl WarpServer {
    /// Create a new Warp server
    pub fn new() -> Self {
        Self {
            endpoints: Vec::new(),
        }
    }
    
    /// Build path filter from string path
    fn build_path_filter(&self, path: &str) -> impl Filter<Extract = (), Error = Rejection> + Clone {
        if path == "/" {
            warp::path::end().boxed()
        } else {
            let segments: Vec<&str> = path.trim_start_matches('/')
                .split('/')
                .filter(|s| !s.is_empty())
                .collect();
            
            let mut filter = warp::path(segments[0].to_string()).boxed();
            for segment in &segments[1..] {
                filter = filter.and(warp::path(segment.to_string())).boxed();
            }
            filter.and(warp::path::end()).boxed()
        }
    }
    
    /// Convert our generic Request to a handler function
    fn create_handler(
        endpoint: Arc<dyn HttpEndpoint>,
    ) -> impl Fn(warp::path::FullPath, warp::http::Method, warp::http::HeaderMap, warp::hyper::body::Bytes) 
        -> Box<dyn std::future::Future<Output = Result<Box<dyn Reply>, Rejection>> + Send> + Clone
    {
        move |path: warp::path::FullPath, 
              method: warp::http::Method, 
              headers: warp::http::HeaderMap,
              body: warp::hyper::body::Bytes| {
            let endpoint = endpoint.clone();
            Box::new(async move {
                // Convert Warp types to our generic types
                let http_method = match method.as_str() {
                    "GET" => HttpMethod::Get,
                    "POST" => HttpMethod::Post,
                    "PUT" => HttpMethod::Put,
                    "DELETE" => HttpMethod::Delete,
                    "PATCH" => HttpMethod::Patch,
                    "HEAD" => HttpMethod::Head,
                    "OPTIONS" => HttpMethod::Options,
                    _ => return Err(warp::reject::reject()),
                };
                
                // Convert headers
                let mut req_headers = HashMap::new();
                for (name, value) in headers.iter() {
                    if let Ok(value_str) = value.to_str() {
                        req_headers.insert(name.to_string(), value_str.to_string());
                    }
                }
                
                // Build our generic request
                let request = Request {
                    method: http_method,
                    path: path.as_str().to_string(),
                    headers: req_headers,
                    query_params: HashMap::new(), // TODO: Parse query params
                    body: body.to_vec(),
                };
                
                // Call the endpoint handler
                match endpoint.handle(request).await {
                    Ok(response) => {
                        // Convert our Response to Warp response
                        let mut warp_response = warp::http::Response::builder()
                            .status(response.status);
                        
                        for (key, value) in response.headers {
                            warp_response = warp_response.header(key, value);
                        }
                        
                        let reply = warp_response
                            .body(response.body)
                            .map_err(|_| warp::reject::reject())?;
                        
                        Ok(Box::new(reply) as Box<dyn Reply>)
                    }
                    Err(_) => Err(warp::reject::reject()),
                }
            })
        }
    }
    
    /// Build Warp filter from endpoints  
    fn build_filter(&self) -> BoxedFilter<(Box<dyn Reply>,)> {
        let mut combined_route: Option<BoxedFilter<(Box<dyn Reply>,)>> = None;
        
        for endpoint in &self.endpoints {
            let endpoint = endpoint.clone();
            let path_str = endpoint.path().to_string();
            
            // Create simple route for this endpoint
            let route = self.build_simple_route(path_str, endpoint);
            
            combined_route = match combined_route {
                Some(existing) => Some(existing.or(route).unify().boxed()),
                None => Some(route.boxed()),
            };
        }
        
        // Return combined routes or 404 if none
        combined_route.unwrap_or_else(|| {
            warp::any()
                .map(|| -> Box<dyn Reply> { 
                    Box::new(warp::http::Response::builder()
                        .status(404)
                        .body(Vec::new())
                        .unwrap())
                })
                .boxed()
        })
    }
    
    /// Build a simple route for an endpoint
    fn build_simple_route(
        &self,
        path_str: String,
        endpoint: Arc<dyn HttpEndpoint>,
    ) -> impl Filter<Extract = (Box<dyn Reply>,), Error = Rejection> + Clone {
        let path_filter = self.build_path_filter(&path_str);
        
        // Create GET/HEAD/DELETE routes (no body)
        let get_route = warp::get()
            .and(path_filter.clone())
            .and(warp::header::headers_cloned())
            .and_then({
                let endpoint = endpoint.clone();
                let path_str = path_str.clone();
                move |headers: warp::http::HeaderMap| {
                    handle_request_no_body(endpoint.clone(), HttpMethod::Get, headers, path_str.clone())
                }
            });
            
        let head_route = warp::head()
            .and(path_filter.clone())
            .and(warp::header::headers_cloned())
            .and_then({
                let endpoint = endpoint.clone();
                let path_str = path_str.clone();
                move |headers: warp::http::HeaderMap| {
                    handle_request_no_body(endpoint.clone(), HttpMethod::Head, headers, path_str.clone())
                }
            });
            
        let delete_route = warp::delete()
            .and(path_filter.clone())
            .and(warp::header::headers_cloned())
            .and_then({
                let endpoint = endpoint.clone();
                let path_str = path_str.clone();
                move |headers: warp::http::HeaderMap| {
                    handle_request_no_body(endpoint.clone(), HttpMethod::Delete, headers, path_str.clone())
                }
            });
            
        // Create POST/PUT/PATCH routes (with body)
        let post_route = warp::post()
            .and(path_filter.clone())
            .and(warp::header::headers_cloned())
            .and(warp::body::bytes())
            .and_then({
                let endpoint = endpoint.clone();
                let path_str = path_str.clone();
                move |headers: warp::http::HeaderMap, body: bytes::Bytes| {
                    handle_request_with_body(endpoint.clone(), HttpMethod::Post, headers, body, path_str.clone())
                }
            });
            
        let put_route = warp::put()
            .and(path_filter.clone())
            .and(warp::header::headers_cloned())
            .and(warp::body::bytes())
            .and_then({
                let endpoint = endpoint.clone();
                let path_str = path_str.clone();
                move |headers: warp::http::HeaderMap, body: bytes::Bytes| {
                    handle_request_with_body(endpoint.clone(), HttpMethod::Put, headers, body, path_str.clone())
                }
            });
            
        let patch_route = warp::patch()
            .and(path_filter.clone())
            .and(warp::header::headers_cloned())
            .and(warp::body::bytes())
            .and_then({
                let endpoint = endpoint.clone();
                let path_str = path_str.clone();
                move |headers: warp::http::HeaderMap, body: bytes::Bytes| {
                    handle_request_with_body(endpoint.clone(), HttpMethod::Patch, headers, body, path_str.clone())
                }
            });
        
        // Combine all routes
        get_route
            .or(post_route)
            .unify()
            .or(put_route)
            .unify()
            .or(patch_route)
            .unify()
            .or(delete_route)
            .unify()
            .or(head_route)
            .unify()
    }
}

impl Default for WarpServer {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper function to handle requests without body (GET, HEAD, DELETE, OPTIONS)
async fn handle_request_no_body(
    endpoint: Arc<dyn HttpEndpoint>,
    method: HttpMethod,
    headers: warp::http::HeaderMap,
    path: String,
) -> Result<Box<dyn Reply>, Rejection> {
    // Check if endpoint supports this method
    let supported_methods = endpoint.methods();
    if !supported_methods.is_empty() && !supported_methods.contains(&method) {
        return Err(warp::reject::not_found());
    }
    
    // Convert headers
    let mut req_headers = HashMap::new();
    for (name, value) in headers.iter() {
        if let Ok(value_str) = value.to_str() {
            req_headers.insert(name.to_string(), value_str.to_string());
        }
    }
    
    let request = Request {
        method,
        path,
        headers: req_headers,
        query_params: HashMap::new(),
        body: Vec::new(),
    };
    
    // Handle request
    match endpoint.handle(request).await {
        Ok(response) => {
            let mut builder = warp::http::Response::builder()
                .status(response.status);
            
            for (key, value) in response.headers {
                builder = builder.header(key, value);
            }
            
            let reply = builder
                .body(response.body)
                .map_err(|_| warp::reject::reject())?;
            
            Ok(Box::new(reply) as Box<dyn Reply>)
        }
        Err(_) => Err(warp::reject::reject()),
    }
}

/// Helper function to handle requests with body (POST, PUT, PATCH)
async fn handle_request_with_body(
    endpoint: Arc<dyn HttpEndpoint>,
    method: HttpMethod,
    headers: warp::http::HeaderMap,
    body: bytes::Bytes,
    path: String,
) -> Result<Box<dyn Reply>, Rejection> {
    // Check if endpoint supports this method
    let supported_methods = endpoint.methods();
    if !supported_methods.is_empty() && !supported_methods.contains(&method) {
        return Err(warp::reject::not_found());
    }
    
    // Convert headers
    let mut req_headers = HashMap::new();
    for (name, value) in headers.iter() {
        if let Ok(value_str) = value.to_str() {
            req_headers.insert(name.to_string(), value_str.to_string());
        }
    }
    
    let request = Request {
        method,
        path,
        headers: req_headers,
        query_params: HashMap::new(),
        body: body.to_vec(),
    };
    
    // Handle request
    match endpoint.handle(request).await {
        Ok(response) => {
            let mut builder = warp::http::Response::builder()
                .status(response.status);
            
            for (key, value) in response.headers {
                builder = builder.header(key, value);
            }
            
            let reply = builder
                .body(response.body)
                .map_err(|_| warp::reject::reject())?;
            
            Ok(Box::new(reply) as Box<dyn Reply>)
        }
        Err(_) => Err(warp::reject::reject()),
    }
}

#[async_trait]
impl WebServer for WarpServer {
    fn register_endpoint(&mut self, endpoint: Box<dyn HttpEndpoint>) -> Result<(), WebError> {
        self.endpoints.push(Arc::from(endpoint));
        Ok(())
    }
    
    async fn start(self, config: ServerConfig) -> Result<(), WebError> {
        let addr: SocketAddr = config.address()
            .parse()
            .map_err(|e| WebError::BindFailed {
                address: config.address(),
                source: Some(Box::new(e)),
            })?;
        
        let routes = self.build_filter();
        
        // Start the server
        warp::serve(routes)
            .run(addr)
            .await;
        
        Ok(())
    }
    
    fn shutdown_handle(&self) -> Option<Box<dyn ServerShutdownHandle>> {
        // Warp doesn't provide easy shutdown handles in this simple implementation
        // For production, we'd need to use warp::Server::bind_with_graceful_shutdown
        None
    }
}