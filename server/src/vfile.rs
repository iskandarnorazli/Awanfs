//! The file seam (#213, decision 67): everything a volume does to one of its files. Production
//! uses `AlignedFile` (and `DiskFs`), unchanged in what it does; the deterministic simulation
//! uses `crate::sim::SimFile`, which can crash, tear and fail on a seed.
//!
//! The volume code becomes generic over `VolFs` in stage 1b, with `DiskFs` as the default, so
//! production is monomorphised and pays nothing for the seam.

use crate::direct_io::AlignedFile;
use memmap2::Mmap;
use std::io;
use std::path::Path;
use std::sync::Arc;

/// One file of a volume: the data file or its journal. Offsets and lengths follow the same
/// rules as `AlignedFile` (sector aligned for the data file).
pub trait VolFile: Send + Sync + 'static {
    fn write_aligned_at(&self, offset: u64, buf: &[u8]) -> io::Result<()>;
    fn read_aligned_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()>;
    fn len(&self) -> io::Result<u64>;
    fn set_len(&self, len: u64) -> io::Result<()>;
    /// The data and the length are durable when this returns.
    fn sync_data(&self) -> io::Result<()>;
    /// Marks the file sparse where the platform needs it (Windows); a no-op elsewhere.
    fn make_sparse(&self) -> io::Result<()>;
    /// The read mapping over the whole file, where there is one. `None` makes reads use
    /// `read_aligned_at` (the simulation has no mapping).
    fn map(&self) -> io::Result<Option<Mmap>>;
}

/// Where a volume's files live: the disk, or the simulation's in-memory files.
pub trait VolFs: Send + Sync + 'static {
    type File: VolFile;
    /// Opens `path` for reading and writing, creating it when `create` is set.
    fn open(&self, path: &Path, create: bool) -> io::Result<Arc<Self::File>>;
    fn exists(&self, path: &Path) -> bool;
    fn remove(&self, path: &Path) -> io::Result<()>;
}

impl VolFile for AlignedFile {
    fn write_aligned_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        AlignedFile::write_aligned_at(self, offset, buf)
    }
    fn read_aligned_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        AlignedFile::read_aligned_at(self, offset, buf)
    }
    fn len(&self) -> io::Result<u64> {
        Ok(self.metadata()?.len())
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        AlignedFile::set_len(self, len)
    }
    fn sync_data(&self) -> io::Result<()> {
        AlignedFile::sync_data(self)
    }
    fn make_sparse(&self) -> io::Result<()> {
        AlignedFile::make_sparse(self)
    }
    fn map(&self) -> io::Result<Option<Mmap>> {
        // SAFETY: as everywhere volumes map their file: AwanFS only appends to a volume and
        // never truncates below a mapped length while a mapping is in use.
        Ok(Some(unsafe { Mmap::map(self.file()) }?))
    }
}

/// The real file system.
#[derive(Clone, Copy, Debug, Default)]
pub struct DiskFs;

impl VolFs for DiskFs {
    type File = AlignedFile;
    fn open(&self, path: &Path, create: bool) -> io::Result<Arc<AlignedFile>> {
        if !create && !path.exists() {
            return Err(io::Error::new(io::ErrorKind::NotFound, format!("{} does not exist", path.display())));
        }
        Ok(Arc::new(AlignedFile::open_direct(path, false)?))
    }
    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }
    fn remove(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }
}
