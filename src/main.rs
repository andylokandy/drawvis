use std::path::PathBuf;

use failure::ResultExt;
use fehler::throws;
use image::{DynamicImage, GenericImage, GenericImageView, ImageBuffer};
use mysql::prelude::*;
use mysql::*;
use serde_derive::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub image: PathBuf,
    pub connect_url: String,
}

impl Config {
    #[throws(failure::Error)]
    fn from_env() -> Self {
        envy::from_env().context("while reading from environment")?
    }
}

const INTERVAL_SECS: usize = 60;
const INTERS_PER_INTERVAL: usize = 6;
const ROWS_PER_TABLE: usize = 100;

#[throws(failure::Error)]
fn main() -> () {
    dotenv::dotenv()?;
    let config = Config::from_env()?;

    let img = image::open(config.image)?.into_luma();
    let (width, height) = img.dimensions();

    dbg!(width, height);

    println!("Connecting");

    let pool = Pool::new(config.connect_url)?;
    let mut conn = pool.get_conn()?;

    println!("Preparing");

    prepare_db(&mut conn, height as usize)?;

    println!("Start painting");

    for x in 0..width {
        println!("Drawing x = {}", x);
        for _ in 0..INTERS_PER_INTERVAL {
            for y in 0..height {
                println!("Drawing y = {}", y);
                write_batch(&mut conn, img.get_pixel(x, y).0[0] as usize)?;
            }
            std::thread::sleep(std::time::Duration::from_secs(
                (INTERVAL_SECS / INTERS_PER_INTERVAL) as u64,
            ));
        }
    }

    println!("WOW!");
}

#[throws(failure::Error)]
fn prepare_db(conn: &mut PooledConn, table_count: usize) -> () {
    conn.query_drop(r"DROP DATABASE test")?;
    conn.query_drop(r"CREATE DATABASE test")?;
    conn.query_drop(r"USE test")?;

    for table in 0..table_count {
        println!("Preparing talbe {}", table);

        conn.query_drop(format!(
            r"CREATE TABLE draw{} (
                id int not null,
                val int not null
            )",
            table
        ))?;
        conn.exec_batch(
            format!(r"INSERT INTO draw{} (id, val) VALUES (:id, :val)", table),
            (0..ROWS_PER_TABLE).map(|r| {
                params! {
                    "id" => r,
                    "val" => r,
                }
            }),
        )?;
    }
}

#[throws(failure::Error)]
fn write_batch(conn: &mut PooledConn, table_idx: usize) -> () {
    conn.exec_batch(
        format!(
            r"UPDATE INTO draw{} SET val = val + 1 WHERE id = :id",
            table_idx
        ),
        (0..ROWS_PER_TABLE).map(|r| {
            params! {
                "id" => r,
            }
        }),
    )?;
}
