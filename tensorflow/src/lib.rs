
#[macro_use]
extern crate log;
extern crate wascc_actor as actor;
#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate serde_json;

use actor::prelude::*;
use codec::messaging;
use codec::messaging::{BrokerMessage, RequestMessage};

use tea_codec::{task};
use wascc_codec::{serialize};

const CAPABILITY_ID_TPM: &str = "tea:tpm";


actor_handlers!{ 
    // codec::messaging::OP_DELIVER_MESSAGE => handle_message, 
    codec::core::OP_HEALTH_REQUEST => health,
    task::START => handle_task
}


fn handle_message(msg: BrokerMessage) -> HandlerResult<()> {
    //println(&format!("Received message broker message: {:?}", msg));
    let channel_parts: Vec<&str> = msg.subject.split('.').collect();
    match &channel_parts[..]{
        ["actor", "execute", "task", "with_param", .. ] => run_test_tensorflow(&msg),

        _=>Ok(())
    }
}

fn handle_task(payload: task::TensorflowParam) -> HandlerResult<task::TensorflowResult> {
    let image = payload.image;
    let res = untyped::default().call(
        "tea:tensorflow",
        "recognize",
        image,
    )?;

    let rs = String::from_utf8(res.clone()).unwrap();

    Ok(task::TensorflowResult {
        result: rs
    })
}


fn health(_req: codec::core::HealthRequest) -> HandlerResult<()> {
    Ok(())
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TensorFlowMessage {
    
}


fn run_test_tensorflow(msg: &BrokerMessage) -> HandlerResult<()> {
    let cid = String::from_utf8(msg.body.clone()).unwrap();

    let buf = get_block_from_ipfs(&cid)?;

    // info!("{:#?}", buf);

    let res = untyped::default().call(
        "tea:tensorflow",
        "recognize",
        buf,
    )?;

    let rs = String::from_utf8(res).unwrap();

    untyped::default().call(
        "wascc:messaging",
        messaging::OP_PUBLISH_MESSAGE,
        serialize(BrokerMessage {
            subject: String::from(&msg.reply_to),
            reply_to : String::new(), 
            body: rs.as_bytes().to_vec(),
        })?
    )?;

    Ok(())

}

fn get_block_from_ipfs(cid: &str) -> HandlerResult<Vec<u8>> {
    // let req = ipfs_proto::BlockGetRequest{hash: cid.to_string()};
    // let mut buf = Vec::with_capacity(req.encoded_len());
    // req.encode(&mut buf);
    let res = untyped::default().call(
        "tea:ipfs",
        "block_get",
        cid.as_bytes().to_vec()
    )?;


    Ok(res)
}

