pub mod admin_service;
pub mod auth_interceptor;
pub mod file_service;

// Include the generated protobuf code at compile time.
// The build.rs writes to src/grpc/generated/
pub mod pb {
    tonic::include_proto!("jihuan.v1");
}
