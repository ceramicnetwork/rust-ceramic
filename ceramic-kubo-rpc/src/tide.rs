use tide::prelude::*;
use tide::Request;

#[derive(Debug, Deserialize)]
struct Animal {
    name: String,
    legs: u16,
}

async fn serve() -> tide::Result<()> {
    let mut app = tide::new();
    app.at("/dag/get").post(dag_get);
    app.listen("127.0.0.1:8080").await?;
    Ok(())
}

async fn order_shoes(mut req: Request<()>) -> tide::Result {
    let Animal { name, legs } = req.body_json().await?;
    Ok(format!("Hello, {}! I've put in an order for {} shoes", name, legs).into())
}

async fn dag_get(mut req: Request<()>) -> tide::Result {
    let Animal { name, legs } = req.body_json().await?;
    Ok(format!("Hello, {}! I've put in an order for {} shoes", name, legs).into())
}
