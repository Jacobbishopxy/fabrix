trait Foo {
    type Output;

    fn f1(i: usize) -> Self::Output;
}

struct FooXiaoChai<TARGET>
where
    TARGET: Foo,
{
    target: TARGET,
}

impl<TARGET> FooXiaoChai<TARGET>
where
    TARGET: Foo,
{
    fn c1_fn0<FN>(&self, i: usize, f: FN) -> TARGET::Output
    where
        FN: Fn(usize) -> TARGET::Output,
    {
        let foo = f;
        foo(i)
    }

    fn c1_fn1(&self, i: usize) -> TARGET::Output {
        let foo = TARGET::f1;
        foo(i)
    }
}

#[test]
fn name() {
    struct FooGeiGei;

    impl Foo for FooGeiGei {
        type Output = usize;

        fn f1(i: usize) -> Self::Output {
            i * 10
        }
    }

    let xiao_chai = FooXiaoChai { target: FooGeiGei };

    let x = xiao_chai.c1_fn0(10, |i| i * 10);

    let y = xiao_chai.c1_fn1(10);
}

fn main() {
    unimplemented!()
}
