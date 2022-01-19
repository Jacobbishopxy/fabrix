//! impl FnMut for sync fn & async fn

use futures::future::{BoxFuture, LocalBoxFuture};

fn main() {
    unimplemented!();
}

struct Foo
where
    Self: Send;

impl Foo {
    fn convert1(&mut self, data: &[u8]) -> Result<usize, ()> {
        Ok(data.len())
    }

    async fn consume1<'a>(&self, data: &[u8]) -> Result<usize, ()> {
        Ok(data.len())
    }

    fn convert2(&mut self, data: &[u8]) -> Result<usize, ()> {
        Ok(data.len() * 10)
    }

    async fn consume2<'a>(&mut self, data: &[u8]) -> Result<usize, ()> {
        Ok(data.len() * 10)
    }
}

struct Bar;

impl Bar {
    async fn process(
        &mut self,
        d1: &[u8],
        d2: &[u8],
        mut convert_fn: impl FnMut(&[u8]) -> Result<usize, ()>,
        mut consume_fn: impl FnMut(&[u8]) -> LocalBoxFuture<Result<usize, ()>>,
    ) -> Result<usize, ()> {
        let a = convert_fn(d1).unwrap();

        let b = consume_fn(d2).await.unwrap();

        Ok(a + b)
    }
}

// notice that async fn `consume_fn` cannot borrow value from the outer scope
// error: lifetime may not live long enough closure implements `FnMut`,
// so references to captured variables can't escape the closure
#[tokio::test]
async fn use_fn_mut_test() {
    let mut bar = Bar;

    let d1 = [1, 2, 3];
    let d2 = [4, 5, 6];

    let mut x = Foo;

    let res1 = bar
        .process(
            &d1,
            &d2,
            |d| x.convert1(d),
            |d| Box::pin(async { Foo.consume1(d).await }),
        )
        .await;

    println!("{:?}", res1);

    let res2 = bar
        .process(
            &d1,
            &d2,
            |d| x.convert2(d),
            |d| Box::pin(async { Foo.consume2(d).await }),
        )
        .await;

    println!("{:?}", res2);
}

struct Qux {
    consume_fn: Box<dyn Fn(&[u8]) -> BoxFuture<usize> + Send + Sync>,
}

struct Zot;

impl Zot {
    async fn process<'a>(
        &mut self,
        d1: &[u8],
        d2: &'a [u8],
        mut convert_fn: impl FnMut(&[u8]) -> usize,
        mut consume_fn: impl FnMut(&'a [u8]) -> BoxFuture<'a, usize>,
    ) -> usize {
        let a = convert_fn(d1);
        let b = consume_fn(d2).await;

        a + b
    }
}

// notice that async fn `consume_fn` cannot borrow value from the outer scope
// error: lifetime may not live long enough closure implements `FnMut`,
// so references to captured variables can't escape the closure
#[tokio::test]
async fn use_fn_mut_test2() {
    use std::sync::Arc;
    use tokio::sync::Mutex;

    let mut zot = Zot;

    let d1 = [1, 2, 3];
    let d2 = [4, 5, 6, 7];

    let qux = Qux {
        consume_fn: Box::new(|data: &[u8]| Box::pin(async { data.len() * 10 })),
    };

    let qux = Arc::new(Mutex::new(qux));

    let res = zot
        .process(
            &d1,
            &d2,
            |data| data.len(),
            |data| {
                Box::pin(async {
                    let am = Arc::clone(&qux);
                    let lk = am.lock().await;

                    let f = &lk.consume_fn;

                    f(data).await
                })
            },
        )
        .await;

    println!("{:?}", res);
}
