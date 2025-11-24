use godot::prelude::*;

mod utils;

struct Utils;

#[gdextension]
unsafe impl ExtensionLibrary for Utils {}
