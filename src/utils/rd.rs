use crate::backend;
use log::trace;
use redis;
use redis::Connection;
use redis::ConnectionAddr::Tcp;
use std::error::Error;

pub struct Client {
    pub cli: redis::Client,
}

impl Client {
    pub fn new(opts: &backend::Backend) -> Result<Client, Box<dyn Error>> {
        let _cfg = opts.get_redis();
        let _coninfo = redis::ConnectionInfo {
            addr: Box::new(Tcp(_cfg.host.to_string(), _cfg.port)),
            db: 0,
            passwd: _cfg.password,
            username: _cfg.user,
        };
        trace!("Connecting Redis...: {:?}", _coninfo);
        let _cli = redis::Client::open(_coninfo)?;
        trace!("Connected Redis...");
        Ok(Client { cli: _cli })
    }
    pub fn con(&self) -> Result<Connection, Box<dyn Error>> {
        Ok(self.cli.get_connection()?)
    }
}
