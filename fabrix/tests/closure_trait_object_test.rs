use async_trait::async_trait;

#[tokio::test]
async fn closure_trait_object_test() {
    let foo_baby = FooBaby(32);

    let a = A {
        foo: Box::new(foo_baby),
    };

    let foo_fn = |x| x * 10;

    let foo_ans = a.foo.foo(Box::new(foo_fn)).await;

    println!("{:?}", foo_ans);

    let foo_ans2 = a.foo.foo(Box::new(foo_fn)).await;

    println!("{:?}", foo_ans2);
}

#[async_trait]
trait Foo {
    async fn foo(&self, f: Box<dyn Fn(i32) -> i32 + Send>) -> i32;
}

struct A {
    foo: Box<dyn Foo>,
}

struct FooBaby(i32);

#[async_trait]
impl Foo for FooBaby {
    async fn foo(&self, f: Box<dyn Fn(i32) -> i32 + Send>) -> i32 {
        f(self.0)
    }
}
