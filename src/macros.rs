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

#[macro_export]
macro_rules! impl_block_parse_type_val_for_number {
    ($tp: ty, $rng: ident, $func: ident, $val: ident, $data: ident, $name: ident) => {
        $data[$name] = match $val {
            FixedValue::Single(val) => json!(val.parse::<$tp>().unwrap_or_else(|_e| $func())),
            FixedValue::Array(vals) => {
                json!(vals[$rng.gen_range(0..vals.len())]
                    .parse::<$tp>()
                    .unwrap_or_else(|_e| $func()))
            }
            FixedValue::Range(val1, val2) => match val1.parse::<$tp>() {
                Ok(val1) => match val2.parse::<$tp>() {
                    Ok(val2) => {
                        json!($rng.gen_range(val1..val2))
                    }
                    Err(_) => {
                        json!($func())
                    }
                },
                Err(_) => {
                    json!($func())
                }
            },
            FixedValue::None => json!($func()),
        }
    };
}

#[macro_export]
macro_rules! impl_block_parse_type_val_for_str {
    ($rng: ident, $func: ident, $val: ident, $data: ident, $name: ident, $random: ident) => {
        $data[$name] = match $val {
            FixedValue::Single(val) => json!(val),
            FixedValue::Array(vals) => json!(vals[$rng.gen_range(0..vals.len())]),
            FixedValue::Range(val1, val2) => json!($random(val1, val2)),
            FixedValue::None => json!($func()),
        }
    };

    ($rng: ident, $func: expr, $val: ident, $data: ident, $name: ident, $random: ident) => {
        $data[$name] = match $val {
            FixedValue::Single(val) => json!(val),
            FixedValue::Array(vals) => json!(vals[$rng.gen_range(0..vals.len())]),
            FixedValue::Range(val1, val2) => json!($random(val1, val2)),
            FixedValue::None => json!($func),
        }
    };
}
