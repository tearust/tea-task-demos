
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

const CAPABILITY_ID_TPM: &str = "tea:tpm";


actor_handlers!{ 
    codec::messaging::OP_DELIVER_MESSAGE => handle_message, 
    codec::core::OP_HEALTH_REQUEST => health
}


fn handle_message(msg: BrokerMessage) -> HandlerResult<()> {
    println(&format!("Received message broker message: {:?}", msg));
    let channel_parts: Vec<&str> = msg.subject.split('.').collect();
    match &channel_parts[..]{
        ["actor", "execute", "task", "with_param", .. ] => run_test_tensorflow(&msg),

        _=>Ok(())
    }
}



fn handle_tensorflow(query: &str) -> HandlerResult<codec::http::Response> {
    let res = untyped::default().call(
        "tea:tensorflow",
        "recognize",
        serialize(TensorFlowMessage { })?,
    )?;

    let rs = String::from_utf8(res).unwrap();
    info!("{:#?}", rs);

    let result = json!({ "calling tensorflow result": rs });
    Ok(codec::http::Response::json(result, 200, "OK"))  
}


fn health(_req: codec::core::HealthRequest) -> HandlerResult<()> {
    Ok(())
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TensorFlowMessage {
    
}


fn run_test_tensorflow(msg: &BrokerMessage) -> HandlerResult<()> {
    let cid = String::from_utf8(msg.body.clone()).unwrap();
    info!("====== {:?}", cid);

    let buf = get_block_from_ipfs(&cid)?;

    // info!("{:#?}", buf);

    let res = untyped::default().call(
        "tea:tensorflow",
        "recognize",
        buf,
    )?;

    let rs = String::from_utf8(res).unwrap();
    info!("{:#?}", rs);

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

    info!("==== >>> {:?}", res);

    Ok(res)
}

