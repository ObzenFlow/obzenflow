//! Warp implementation of the WebServer trait

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use warp::{Filter, Rejection, Reply};
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
    fn build_filter(&self) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
        let mut routes = Vec::new();
        
        for endpoint in &self.endpoints {
            let endpoint = endpoint.clone();
            let path = endpoint.path().to_string();
            
            // Create a filter for this endpoint
            let route = warp::path::full()
                .and(warp::method())
                .and(warp::header::headers_cloned())
                .and(warp::body::bytes())
                .and_then({
                    let endpoint = endpoint.clone();
                    let path = path.clone();
                    move |full_path: warp::path::FullPath, 
                          method: warp::http::Method,
                          headers: warp::http::HeaderMap,
                          body: warp::hyper::body::Bytes| {
                        let endpoint = endpoint.clone();
                        let path = path.clone();
                        async move {
                            // Check if this endpoint handles this path
                            if full_path.as_str() != path {
                                return Err::<Box<dyn Reply>, Rejection>(warp::reject::not_found());
                            }
                            
                            // Convert method
                            let http_method = match method.as_str() {
                                "GET" => HttpMethod::Get,
                                "POST" => HttpMethod::Post,
                                "PUT" => HttpMethod::Put,
                                "DELETE" => HttpMethod::Delete,
                                "PATCH" => HttpMethod::Patch,
                                "HEAD" => HttpMethod::Head,
                                "OPTIONS" => HttpMethod::Options,
                                _ => return Err(warp::reject::not_found()),
                            };
                            
                            // Check if endpoint handles this method
                            let supported_methods = endpoint.methods();
                            if !supported_methods.is_empty() && !supported_methods.contains(&http_method) {
                                return Err(warp::reject::not_found());
                            }
                            
                            // Convert headers
                            let mut req_headers = HashMap::new();
                            for (name, value) in headers.iter() {
                                if let Ok(value_str) = value.to_str() {
                                    req_headers.insert(name.to_string(), value_str.to_string());
                                }
                            }
                            
                            // Build request
                            let request = Request {
                                method: http_method,
                                path: full_path.as_str().to_string(),
                                headers: req_headers,
                                query_params: HashMap::new(),
                                body: body.to_vec(),
                            };
                            
                            // Handle request
                            match endpoint.handle(request).await {
                                Ok(response) => {
                                    // Build Warp response
                                    let mut builder = warp::http::Response::builder()
                                        .status(response.status);
                                    
                                    for (key, value) in response.headers {
                                        builder = builder.header(key, value);
                                    }
                                    
                                    let reply = builder
                                        .body(response.body)
                                        .map_err(|_| warp::reject::reject())?;
                                    
                                    Ok::<Box<dyn Reply>, Rejection>(Box::new(reply))
                                }
                                Err(_) => Err(warp::reject::reject()),
                            }
                        }
                    }
                })
                .boxed();
            
            routes.push(route);
        }
        
        // Combine all routes with "or"
        routes
            .into_iter()
            .reduce(|a, b| a.or(b).unify().boxed())
            .unwrap_or_else(|| {
                // If no routes, return 404 for everything
                warp::any()
                    .and_then(|| async { Err::<Box<dyn Reply>, Rejection>(warp::reject::not_found()) })
                    .boxed()
            })
    }
}

impl Default for WarpServer {
    fn default() -> Self {
        Self::new()
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