use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::cmp;
use std::mem;
use redb::{Database, Error, TableDefinition, TypeName, Value};

use godot::prelude::*;
use godot::classes::Node;
use godot::classes::INode;
use godot::builtin::PackedInt64Array;
use godot::builtin::PackedByteArray;
use godot::global::roundi;
use godot::global::{var_to_bytes_with_objects, bytes_to_var_with_objects};

// These constants can be adjusted:
const TILE_DEFAULT: u8 = 0;                  // Default value of tile bytes, idk why another value mb needed.
const TILE_SIZE: usize = 1;                  // Size of each tile in bytes.
const MAX_POW: i8 = u64::BITS as i8;         // 2^MAX_POW is count of possible chunks, must be even.
const CHUNK_SIZE_Y_POW: i64 = 4;             // 2^CHUNK_SIZE_Y_POW is chunk height in tiles.
const CHUNK_SIZE_X_POW: i64 = 4;             // 2^CHUNK_SIZE_X_POW is chunk width in tiles.
const VIEW_DEFAULT_POS_X: f32 = (            // If there is no saved info about player position,
    (1 as i64) << (CHUNK_SIZE_X_POW as i64)  // this will be taken, so, after world creation, player
) as f32;                                    // will spawn at VIEW_DEFAULT_POS_X;VIEW_DEFAULT_POS_Y.
const VIEW_DEFAULT_POS_Y: f32 = (            // Current values are not meaning anything, adjust as
    (1 as i64) << (CHUNK_SIZE_X_POW as i64)  // you want.
) as f32;
// Other constants are not meant to be changed!

const CHUNKS: TableDefinition<u64, ChunkContent> = TableDefinition::new("chunks");
const OBJECTS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("objects_table");
const GLOBAL_FLOAT: TableDefinition<&str, f32> = TableDefinition::new("global_float");
const AXIS_MIN: i64 = 0;
const AXIS_MAX: i64 = (1 << (MAX_POW >> 1)) - 1;
const AXIS_WIDTH: i64 = AXIS_MAX - AXIS_MIN + 1;

fn id_to_xy(mut uid: u64, bbox_x: i64, bbox_y: i64,) -> (i64, i64) {
    let mut x: i64 = 0;
    let mut y: i64 = 0;
    for _i in 0..(MAX_POW >> 1) {
        x <<= 1;
        y <<= 1;
        y |= (uid & 1) as i64;
        x |= ((uid & 2) >> 1) as i64; 
        uid >>= 2;
    }
    let mut x_new = x - AXIS_MIN + bbox_x - ((bbox_x - AXIS_MIN) & (AXIS_WIDTH - 1));
    if x_new < bbox_x {
        x_new += AXIS_WIDTH;
    }
    let mut y_new = y - AXIS_MIN + bbox_y - ((bbox_y - AXIS_MIN) & (AXIS_WIDTH - 1));
    if y_new < bbox_y {
        y_new += AXIS_WIDTH;
    }
    (x_new, y_new)
}

fn xy_to_id(mut x: i64, mut y: i64) -> u64 {
    x = ((((x - AXIS_MIN) % AXIS_WIDTH) + AXIS_WIDTH) % AXIS_WIDTH) + AXIS_MIN;
    y = ((((y - AXIS_MIN) % AXIS_WIDTH) + AXIS_WIDTH) % AXIS_WIDTH) + AXIS_MIN;
    let mut uid: u64 = 0;
    for _i in 0..(MAX_POW >> 1) {
        uid <<= 1;
        uid |= (x & 1) as u64;
        uid <<= 1;
        uid |= (y & 1) as u64;
        x >>= 1;
        y >>= 1;
    }
    return uid;
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct Tile {
    data: [u8; TILE_SIZE],
}

#[derive(Debug)]
#[repr(C)]
struct ChunkContent {
    tiles: [Tile; WorldSaver::CHUNK_AREA]
}

impl ChunkContent {
    fn new(tiles: Option<&[Tile; WorldSaver::CHUNK_AREA]>) -> ChunkContent {
        ChunkContent { tiles: match tiles {
            Some(t) => *t,
            None => [Tile { data: [TILE_DEFAULT; TILE_SIZE] }; WorldSaver::CHUNK_AREA]
        } }
    }
}

impl Value for ChunkContent {
    type SelfType<'a> = ChunkContent;
    type AsBytes<'a> = [u8; mem::size_of::<ChunkContent>()];

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        // SAFETY: This is safe because ChunkContent is a POD type with the same layout as [u8; mem::size_of::<ChunkContent>()]
        unsafe { std::mem::transmute::<[u8; mem::size_of::<ChunkContent>()], ChunkContent>(data.try_into().unwrap()) }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        // SAFETY: This is safe because ChunkContent is a POD type with the same layout as [u8; mem::size_of::<ChunkContent>()]
        unsafe { std::mem::transmute_copy(value) }
    }

    fn fixed_width() -> Option<usize> {
        Some(mem::size_of::<ChunkContent>())
    }

    fn type_name() -> redb::TypeName {
        TypeName::new("ChunkContent")
    }
}
#[derive(GodotClass)]
#[class(base=Node)]
struct WorldSaver {
    tile_size_x: i64,
    tile_size_y: i64,
    center_x: i64,
    center_y: i64,
    bbox_x: i64,
    bbox_y: i64,
    bbox_x_prev: i64,
    bbox_y_prev: i64,
    db: Option<Database>,
    world_name: String,
    active_chunks: BTreeSet<u64>,
    deactivating_chunks: Vec<u64>,
    load_bus: BTreeMap<u64, ChunkContent>,
    base: Base<Node>
}

#[godot_api]
impl INode for WorldSaver {
    fn init(base: Base<Node>) -> Self {
        Self {
            tile_size_x: 0,
            tile_size_y: 0,
            center_x: 0,
            center_y: 0,
            bbox_x: 0,
            bbox_y: 0,
            bbox_x_prev: 0,
            bbox_y_prev: 0,
            db: None,
            world_name: "".to_string(),
            active_chunks: BTreeSet::new(),
            deactivating_chunks: Vec::new(),
            load_bus: BTreeMap::new(),
            base
        }
    }
}

#[godot_api]
impl WorldSaver {
    #[constant]
    const CHUNK_SIZE_X_POW: i64 = CHUNK_SIZE_X_POW;
    #[constant]
    const CHUNK_SIZE_Y_POW: i64 = CHUNK_SIZE_Y_POW;
    #[constant]
    const CHUNK_SIZE_X: usize = (1 << WorldSaver::CHUNK_SIZE_X_POW);
    #[constant]
    const CHUNK_SIZE_Y: usize = (1 << WorldSaver::CHUNK_SIZE_Y_POW);
    #[constant]
    const CHUNK_AREA: usize = (WorldSaver::CHUNK_SIZE_X * WorldSaver::CHUNK_SIZE_Y);

    #[func]
    fn start(&mut self, world_name: String, tile_size_x: i64, tile_size_y: i64) -> Vector2 {
        match fs::create_dir_all("saves") {
            Ok(_) => godot_print!("Saving \"{}\" into saves...", world_name),
            Err(e) => godot_print!("Error (from rust): \"{}\".", e)
        };
        self.world_name = format!("saves\\{}", world_name);
        self.tile_size_x = tile_size_x;
        self.tile_size_y = tile_size_y;
        self.db = Some(Database::create(&self.world_name).unwrap());
        create_tables(self.db.as_ref().unwrap()).unwrap();
        let pos = get_position(&self.db.as_ref().unwrap());
        self.set_view_center(pos.x as f64, pos.y as f64, true);
        pos
    }

    #[func]
    fn save_as(&mut self, key: GString, obj: Variant) {
        save_obj_as(self.db.as_ref().unwrap(), key.to_string().as_str(), obj);
    }

    #[func]
    fn load_by(&mut self, key: GString) -> Variant {
        load_obj_by(self.db.as_ref().unwrap(), key.to_string().as_str())
    }

    #[func]
    fn set_view_center(&mut self, x: f64, y: f64, initial: bool) {
        let x_i64: i64 = (roundi(x / self.tile_size_x as f64 - 0.5) + 1) >> WorldSaver::CHUNK_SIZE_X_POW;
        let y_i64: i64 = (roundi(y / self.tile_size_y as f64 - 0.5) + 1) >> WorldSaver::CHUNK_SIZE_Y_POW;
        self.update_center(x_i64, y_i64, initial);
    }

    #[func]
    fn load_another_one(&mut self) -> PackedByteArray {
        match self.load_bus.pop_first() {
            Some(kv) => {
                let xy = id_to_xy(kv.0, self.bbox_x, self.bbox_y);
                const LEN: usize = mem::size_of::<i64>() * 2 + mem::size_of::<ChunkContent>();
                let mut tmp: [u8; LEN] = [TILE_DEFAULT; LEN];
                tmp[0 .. mem::size_of::<i64>()].copy_from_slice(&xy.0.to_le_bytes());
                tmp[mem::size_of::<i64>() .. mem::size_of::<i64>() * 2].copy_from_slice(&xy.1.to_le_bytes());
                tmp[mem::size_of::<i64>() * 2 .. mem::size_of::<i64>() * 2 + ChunkContent::fixed_width().unwrap()].copy_from_slice(&ChunkContent::as_bytes(&kv.1));
                PackedByteArray::from(&tmp)
            },
            None => PackedByteArray::new()
        }
    }

    #[func]
    fn which_to_unload(&mut self) -> PackedInt64Array {
        match self.deactivating_chunks.pop() {
            Some(id) => {
                let xy = id_to_xy(id, self.bbox_x_prev, self.bbox_y_prev);
                PackedInt64Array::from(&[xy.0, xy.1])
            },
            None => PackedInt64Array::new()
        }
    }

    #[func]
    fn unload(&mut self, payload: PackedByteArray) {
        let x = i64::from_le_bytes(payload.as_slice()[0 .. mem::size_of::<i64>()].try_into().unwrap());
        let y = i64::from_le_bytes(payload.as_slice()[mem::size_of::<i64>() .. mem::size_of::<i64>() * 2].try_into().unwrap());
        const SIZE_OF_I64: usize = 8; // Well, just for readability.
        let content: ChunkContent = ChunkContent::from_bytes(payload.as_slice()[SIZE_OF_I64 * 2 .. SIZE_OF_I64 * 2 + ChunkContent::fixed_width().unwrap()].try_into().unwrap());
        let _updated = write_chunk(&self.db.as_ref().unwrap(), xy_to_id(x, y), content).unwrap();
    }

    #[func]
    fn exit(&mut self, pos: Vector2) {
        self.bbox_x_prev = self.bbox_x;
        self.bbox_y_prev = self.bbox_y;
        self.full_unload();
        set_position(self.db.as_ref().unwrap(), pos);
    }
}

impl WorldSaver {
    const BBOX_W: i64 = 4; // Magic number, if not 4, world would collapse!
    const BBOX_AREA: usize = (WorldSaver::BBOX_W * WorldSaver::BBOX_W) as usize;

    fn full_unload(&mut self) {
        while !self.active_chunks.is_empty() {
            self.deactivating_chunks.push(self.active_chunks.pop_first().unwrap());
        }
    }

    fn full_load(&mut self, bbox_x: i64, bbox_y: i64) {
        for x_now in bbox_x..bbox_x+WorldSaver::BBOX_W {
            for y_now in bbox_y..bbox_y+WorldSaver::BBOX_W {
                self.load_chunk(xy_to_id(x_now, y_now));
            }
        }
    }

    fn load_unload(&mut self, bbox_x_new: i64, bbox_y_new: i64) {
        let overlap_left = cmp::max(self.bbox_x, bbox_x_new);
        let overlap_down = cmp::max(self.bbox_y, bbox_y_new);
        let overlap_right = cmp::min(self.bbox_x + WorldSaver::BBOX_W - 1, bbox_x_new + WorldSaver::BBOX_W - 1);
        let overlap_up = cmp::min(self.bbox_y + WorldSaver::BBOX_W - 1, bbox_y_new + WorldSaver::BBOX_W - 1);
        if overlap_left <= overlap_right && overlap_down <= overlap_up {
            let mut to_unload_load: [(u64, u64); WorldSaver::BBOX_AREA] = [(0, 0); WorldSaver::BBOX_AREA];
            let mut cnt = 0;
            for id in &self.active_chunks {
                let (x, y) = id_to_xy(*id, self.bbox_x, self.bbox_y);
                if !(overlap_left <= x && x <= overlap_right && overlap_down <= y && y <= overlap_up) {
                    let to_load = xy_to_id((overlap_right - x) + overlap_left, (overlap_down - y) + overlap_up);
                    to_unload_load[cnt] = (*id, to_load);
                    cnt += 1;
                }
            }
            while cnt > 0 {
                cnt -= 1;
                self.active_chunks.remove(&to_unload_load[cnt].0);
                self.deactivating_chunks.push(to_unload_load[cnt].0);
                self.load_chunk(to_unload_load[cnt].1);
            }
        } else {
            self.full_unload();
            self.full_load(bbox_x_new, bbox_y_new);
        }
    }

    fn update_center(&mut self, x: i64, y: i64, initial: bool) {
        let mut bbox_x_new: i64 = self.bbox_x;
        let mut bbox_y_new: i64 = self.bbox_y;
        if !(self.bbox_x < x && x < self.bbox_x + WorldSaver::BBOX_W - 1) {
            bbox_x_new = ((self.center_x - x) >> (MAX_POW - 1)) + x - 1;
        }
        if !(self.bbox_y < y && y < self.bbox_y + WorldSaver::BBOX_W - 1) {
            bbox_y_new = ((self.center_y - y) >> (MAX_POW - 1)) + y - 1;
        }
        if self.bbox_x != bbox_x_new || self.bbox_y != bbox_y_new || initial {
            if !initial {
                self.load_unload(bbox_x_new, bbox_y_new);
                self.bbox_x_prev = self.bbox_x;
                self.bbox_y_prev = self.bbox_y;
            } else {
                self.full_load(bbox_x_new, bbox_y_new);
            }
            self.bbox_x = bbox_x_new;
            self.bbox_y = bbox_y_new;
        }
        self.center_x = x;
        self.center_y = y;
    }

    fn load_chunk(&mut self, id: u64) {
        let got = get_chunk(&self.db.as_ref().unwrap(), id);
        let rec = match got {
            Ok(d) => d,
            Err(_) => {
                let content = ChunkContent::new(Some(&[Tile { data: [TILE_DEFAULT; TILE_SIZE] }; WorldSaver::CHUNK_AREA]));
                let created = write_chunk(&self.db.as_ref().unwrap(), id, content);
                match created {
                    Ok(_) => get_chunk(&self.db.as_ref().unwrap(), id).unwrap(),
                    Err(e_final) => panic!("{}", e_final)
                }
            }
        };
        self.active_chunks.insert(id);
        self.load_bus.insert(id, rec);
    }
}

fn write_chunk(db: &Database, id: u64, content: ChunkContent) -> Result<ChunkContent, Error> {
    let write_txn = db.begin_write()?;
    let prev_val = {
        let mut table = write_txn.open_table(CHUNKS)?;
        let val = match table.insert(id, &content) {
            Ok(a) => match a {
                Some(v) => v.value(),
                None => ChunkContent::new(None)
            },
            Err(e) => panic!("{}", e)
        };
        val
    };
    write_txn.commit()?;
    Ok(prev_val)
}

fn get_chunk(db: &Database, id: u64) -> Result<ChunkContent, Error> {
    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(CHUNKS)?;
    match table.get(id).unwrap() {
        Some(v) => Ok(v.value()),
        None => Err(Error::Corrupted("No such chunk!".to_string()))
    }
}

fn get_position(db: &Database) -> Vector2 {
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(GLOBAL_FLOAT).unwrap();
    let x = match table.get("playerPosX").unwrap() {
        Some(v) => v.value(),
        None => VIEW_DEFAULT_POS_X
    };
    let y = match table.get("playerPosY").unwrap() {
        Some(v) => v.value(),
        None => VIEW_DEFAULT_POS_Y
    };
    Vector2::new(x, y)
}

fn set_position(db: &Database, pos: Vector2) {
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(GLOBAL_FLOAT).unwrap();
        match table.insert("playerPosX", pos.x) {
            Ok(_) => {},
            Err(e) => panic!("{}", e)
        };
        match table.insert("playerPosY", pos.y) {
            Ok(_) => {},
            Err(e) => panic!("{}", e)
        };
    };
    write_txn.commit().unwrap();
}

fn save_obj_as(db: &Database, key: &str, obj: Variant) {
    let bytes = var_to_bytes_with_objects(obj);
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(OBJECTS_TABLE).unwrap();
        match table.insert(key, bytes.as_slice()) {
            Ok(_) => {},
            Err(e) => panic!("{}", e)
        };
    };
    write_txn.commit().unwrap();
}

fn load_obj_by(db: &Database, key: &str) -> Variant {
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(OBJECTS_TABLE).unwrap();
    match table.get(key).unwrap() {
        Some(v) => bytes_to_var_with_objects(PackedByteArray::from(v.value())),
        None => Variant::nil()
    }
}

fn create_tables(db: &Database) -> Result<(), Error> {
    let write_txn = db.begin_write()?;
    write_txn.open_table(GLOBAL_FLOAT)?;
    write_txn.open_table(CHUNKS)?;
    write_txn.open_table(OBJECTS_TABLE)?;
    write_txn.commit()?;
    Ok(())
}
