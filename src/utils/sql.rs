use crate::backend;
use log::trace;
use mysql::prelude::*;
use mysql::OptsBuilder;
use mysql::Pool;
use mysql::PooledConn;
use std::error::Error;

pub struct Client {
    pool: Pool,
}

impl<'a> Client {
    pub fn new(opts: &backend::Backend) -> Result<Client, Box<dyn Error>> {
        let _cfg = opts.get_mysql();
        let builder = OptsBuilder::new()
            .ip_or_hostname(_cfg.host)
            .tcp_port(_cfg.port)
            .user(_cfg.user)
            .pass(_cfg.password)
            .db_name(_cfg.db);
        trace!("Connecting MySQL...: {:?}", builder);
        let _pool = Pool::new(builder)?;
        trace!("Connected MySQL...");
        trace!("Checking first time setup");
        let q0 = "CREATE TABLE IF NOT EXISTS \
                 tbl_hash(name varchar(2048) PRIMARY KEY, hash BIGINT UNSIGNED, \
                 version VARCHAR(2048));";
        let mut conn = _pool.get_conn()?;
        trace!("SQL: {:?}", q0);
        conn.query_drop(q0)?;
        Ok(Client { pool: _pool })
    }

    pub fn get_conn(&self) -> Result<PooledConn, Box<dyn Error>> {
        let conn = self.pool.get_conn()?;
        Ok(conn)
    }
}
