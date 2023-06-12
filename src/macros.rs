#[macro_export]
macro_rules! impl_func_is_primitive_by_parse {
    ($(($func: ident, $tp: ty)), +) => {
        $(
            pub fn $func(val: &str) -> bool {
                val.parse::<$tp>().is_ok()
            }
            )*
    };
}



