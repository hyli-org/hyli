pub trait Pick<T> {
    fn get(&self) -> &T;
    fn get_mut(&mut self) -> &mut T;
    /// # Safety
    /// This function is unsafe because it returns a raw pointer to the inner value.
    /// This is intended for splitting borrows.
    unsafe fn splitting_get_mut(&mut self) -> *mut T;
}

pub use paste;

/// This creates a struct that provides Pick for each type in the initial list,
/// allowing generic code to access the inner values.
/// Only one value of each type can be stored in the struct.
/// (Somewhat similar to frunk HList or a fancier named tuple).
#[macro_export]
macro_rules! static_type_map {
    // Basic syntax - similar to a named tuple.
    // This just reformats to the recursive form
    (
        $(#[$meta:meta])*
        $pub:vis struct $name:ident $(< $( $lt:tt $( : $clt:tt $(+ $dlt:tt )* )? ),+ >)? ($($t1:ty,)+);
    ) => {
        $crate::utils::static_type_map::paste::paste! {
            // Wrap it in a module to make the member names private
            mod [<$name:snake>] {
                #[allow(unused)]
                use super::*;

                $crate::utils::static_type_map::static_type_map! {
                    $(#[$meta])*
                    ha: $pub struct $name $(< $( $lt $( : $clt $(+ $dlt )* )? ),+ >)? ($($t1,)+) {}
                }
            }
            #[doc(inline)]
            $pub use [<$name:snake>]::$name;
        }
    };
    // Recursive case - we append to $index every time, processing one type.
    (
        $(#[$meta:meta])*
        $index:ident: $pub:vis struct $name:ident $(< $( $lt:tt $( : $clt:tt $(+ $dlt:tt )* )? ),+ >)? ($t1:ty, $($t2:ty,)*) { $($idx:ident: $t3:ty,)* }
    ) => {
        $crate::utils::static_type_map::paste::paste! {
            $crate::utils::static_type_map::static_type_map! {
                $(#[$meta])*
                [<ha $index>]: $pub struct $name $(< $( $lt $( : $clt $(+ $dlt )* )? ),+ >)? ( $($t2,)* ) {
                    $($idx: $t3,)*
                    $index: $t1,
                }
            }
        }
        impl $(< $( $lt $( : $clt $(+ $dlt )* )? ),+ >)? $crate::utils::static_type_map::Pick<$t1> for $name $(< $( $lt ),+ >)? {
            fn get(&self) -> &$t1 {
                &self.$index
            }
            fn get_mut(&mut self) -> &mut $t1 {
                &mut self.$index
            }
            unsafe fn splitting_get_mut(&mut self) -> *mut $t1 {
                &mut self.$index as *mut $t1
            }
        }
    };
    // End state - just output the struct and the new function (which works like the named tuple constructor)
    (
        $(#[$meta:meta])*
        $index:ident: $pub:vis struct $name:ident $(< $( $lt:tt $( : $clt:tt $(+ $dlt:tt )* )? ),+ >)? ( ) { $($idx:ident: $t3:ty,)+ }
    ) => {
        $(#[$meta])*
        pub struct $name $(< $( $lt $( : $clt $(+ $dlt )* )? ),+ >)? {
            $($idx: $t3,)+
        }
        impl $(< $( $lt $( : $clt $(+ $dlt )* )? ),+ >)? $name $(< $( $lt ),+ >)? {
            #[allow(clippy::too_many_arguments)]
            pub fn new($($idx: $t3,)*) -> Self {
                Self {
                    $($idx,)*
                }
            }
        }
    };
}
pub use static_type_map;
