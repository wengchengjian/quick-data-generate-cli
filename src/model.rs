use clickhouse::Row;
use serde::{Deserialize, Serialize};

#[derive(Row, Serialize)]
struct MyRow {
    num: u32,
    row_name: String,
    time: String,
}

#[derive(Row, Serialize, Debug, Deserialize)]
pub struct IpSessionRow {
    pub src_ip: String,
    pub dst_ip: String,
    pub device_id: u64,
    pub customer_id: u64,
    pub source_id: String,
    pub protocol: u32,
    pub protocol_ip: u32,
    pub src_port: Vec<u32>,
    pub dst_port: Vec<u32>,
    pub send_traffic: u64,
    pub recv_traffic: u64,
    pub end_time: u32,
    pub request_time: u32,
    pub src_ip_type: String,
    pub dst_ip_type: String,
    pub src_inner_network_ip: u8,
    pub src_country_name: String,
    pub src_province_name: String,
    pub src_city_name: String,
    pub src_ip_longitude_latitude: Vec<f32>,

    pub dst_inner_network_ip: u8,
    pub dst_country_name: String,
    pub dst_province_name: String,
    pub dst_city_name: String,
    pub dst_ip_longitude_latitude: Vec<f32>,
    pub create_time: u32,
}
